#include "thread_pool.h"

#include <pthread.h>
#include <vector>
#include <queue>
#include <cerrno>

struct thread_task {
	thread_task_f function;
	enum State { CREATED, QUEUED, RUNNING, FINISHED } state;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    thread_pool* pool;
    bool detached;
};

struct thread_pool {
	std::vector<pthread_t> threads;
	std::queue<thread_task*> tasks;
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	int max_threads;
	int tasks_cnt;
	bool stop;
};

static void*
worker(void* arg) {
	thread_pool* pool = static_cast<thread_pool*>(arg);

	while (true) {
		pthread_mutex_lock(&pool->mutex);

		while (!pool->stop && pool->tasks.empty()) {
            pthread_cond_wait(&pool->cond, &pool->mutex);
        }
		if (pool->stop && pool->tasks.empty()) {
            pthread_mutex_unlock(&pool->mutex);
            break;
        }

		thread_task* task = pool->tasks.front();
        pool->tasks.pop();
        pthread_mutex_unlock(&pool->mutex);

        pthread_mutex_lock(&task->mutex);
        task->state = thread_task::RUNNING;
        pthread_mutex_unlock(&task->mutex);
        task->function();

        pthread_mutex_lock(&pool->mutex);
        pool->tasks_cnt--;
        pthread_mutex_unlock(&pool->mutex);

        pthread_mutex_lock(&task->mutex);
        task->state = thread_task::FINISHED;
        if (task->detached) {
            pthread_mutex_unlock(&task->mutex);
            pthread_mutex_destroy(&task->mutex);
            pthread_cond_destroy(&task->cond);
            delete task;
        } else {
            pthread_cond_signal(&task->cond);
            pthread_mutex_unlock(&task->mutex);
        }
	}
	return nullptr;
}

int
thread_pool_new(int thread_count, struct thread_pool **pool)
{
	if (thread_count <= 0 || thread_count > TPOOL_MAX_THREADS) {
		return TPOOL_ERR_INVALID_ARGUMENT;
	}

	thread_pool* thread_pool_new = new thread_pool;
	thread_pool_new->max_threads = thread_count;;
	thread_pool_new->tasks_cnt = 0;
	thread_pool_new->stop = false;
	pthread_mutex_init(&thread_pool_new->mutex, nullptr);
	pthread_cond_init(&thread_pool_new->cond, nullptr);
	*pool = thread_pool_new;
	return 0;
}

int
thread_pool_delete(struct thread_pool *pool)
{
	pthread_mutex_lock(&pool->mutex);
	if (pool->tasks_cnt > 0) {
		pthread_mutex_unlock(&pool->mutex);
		return TPOOL_ERR_HAS_TASKS;
	}

	pool->stop = true;
	pthread_cond_broadcast(&pool->cond);
	pthread_mutex_unlock(&pool->mutex);
	for (pthread_t t: pool->threads) {
		pthread_join(t, nullptr);
	}
	pthread_mutex_destroy(&pool->mutex);
    pthread_cond_destroy(&pool->cond);
	delete pool;
	return 0;
}

int
thread_pool_push_task(struct thread_pool *pool, struct thread_task *task)
{
	pthread_mutex_lock(&pool->mutex);
	if (pool->tasks_cnt >= TPOOL_MAX_TASKS) {
		pthread_mutex_unlock(&pool->mutex);
		return TPOOL_ERR_TOO_MANY_TASKS;
	}

	pthread_mutex_lock(&task->mutex);
	task->state = thread_task::QUEUED;
	task->pool = pool;
	pthread_mutex_unlock(&task->mutex);
	pool->tasks.push(task);
	++(pool->tasks_cnt);
	if (pool->threads.size() < static_cast<size_t>(pool->max_threads)) {
        pthread_t thread;
        pthread_create(&thread, nullptr, worker, pool);
        pool->threads.push_back(thread);
    }
	pthread_cond_signal(&pool->cond);
	pthread_mutex_unlock(&pool->mutex);
	return 0;
}

int
thread_task_new(struct thread_task **task, const thread_task_f &function)
{
	thread_task* new_task = new thread_task;
	new_task->function = function;
	new_task->state = thread_task::CREATED;
	pthread_mutex_init(&new_task->mutex, nullptr);
	pthread_cond_init(&new_task->cond, nullptr);
	new_task->pool = nullptr;
    new_task->detached = false;
	*task = new_task;
	return 0;
}

bool
thread_task_is_finished(const struct thread_task *task)
{
	pthread_mutex_lock(const_cast<pthread_mutex_t*>(&task->mutex));
	bool ret = task->state == thread_task::FINISHED;
	pthread_mutex_unlock(const_cast<pthread_mutex_t*>(&task->mutex));
	return ret;
}

bool
thread_task_is_running(const struct thread_task *task)
{
	pthread_mutex_lock(const_cast<pthread_mutex_t*>(&task->mutex));
	bool ret = task->state == thread_task::RUNNING;
	pthread_mutex_unlock(const_cast<pthread_mutex_t*>(&task->mutex));
	return ret;
}

int
thread_task_join(struct thread_task *task)
{
	pthread_mutex_lock(&task->mutex);
	if (task->pool == nullptr) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TASK_NOT_PUSHED;
	}

	while (task->state != thread_task::FINISHED) {
		pthread_cond_wait(&task->cond, &task->mutex);
	}
	pthread_mutex_unlock(&task->mutex);
	return 0;
}

#if NEED_TIMED_JOIN

int
thread_task_timed_join(struct thread_task *task, double timeout)
{
	pthread_mutex_lock(&task->mutex);
	if (task->pool == nullptr) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TASK_NOT_PUSHED;
	}
	if (timeout < 0) {
        pthread_mutex_unlock(&task->mutex);
        return TPOOL_ERR_TIMEOUT;
    }

	struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
	time_t sec = static_cast<time_t>(timeout);
    long nsec = static_cast<long>((timeout - sec) * 1e9);
    ts.tv_sec += sec;
    ts.tv_nsec += nsec;
    if (ts.tv_nsec >= 1000000000) {
        ts.tv_sec++;
        ts.tv_nsec -= 1000000000;
    }
	while (task->state != thread_task::FINISHED) {
		int errn = pthread_cond_timedwait(&task->cond, &task->mutex, &ts);
		if (errn == ETIMEDOUT) {
			pthread_mutex_unlock(&task->mutex);
			return TPOOL_ERR_TIMEOUT;
		}
	}
	pthread_mutex_unlock(&task->mutex);
	return 0;
}

#endif

int
thread_task_delete(struct thread_task *task)
{
	pthread_mutex_lock(&task->mutex);
	if (task->state == thread_task::QUEUED || task->state == thread_task::RUNNING) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TASK_IN_POOL;
	}

	pthread_mutex_unlock(&task->mutex);
	pthread_mutex_destroy(&task->mutex);
    pthread_cond_destroy(&task->cond);
	delete task;
	return 0;
}

#if NEED_DETACH

int
thread_task_detach(struct thread_task *task)
{
    pthread_mutex_lock(&task->mutex);
    if (task->pool == nullptr) {
        pthread_mutex_unlock(&task->mutex);
        return TPOOL_ERR_TASK_NOT_PUSHED;
    }
    if (task->state == thread_task::FINISHED) {
        pthread_mutex_unlock(&task->mutex);
        pthread_mutex_destroy(&task->mutex);
        pthread_cond_destroy(&task->cond);
        delete task;
        return 0;
    }

    task->detached = true;
    pthread_mutex_unlock(&task->mutex);
    return 0;
}

#endif
