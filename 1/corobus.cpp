#include "corobus.h"

#include "libcoro.h"
#include "rlist.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <deque>
#include <vector>

/**
 * One coroutine waiting to be woken up in a list of other
 * suspended coros.
 */
struct wakeup_entry {
	struct rlist base;
	struct coro *coro;
	bool is_in_queue;
};

/** A queue of suspended coros waiting to be woken up. */
struct wakeup_queue {
	struct rlist coros;
};

static void
wakeup_queue_create(struct wakeup_queue *queue)
{
	rlist_create(&queue->coros);
}

/** Suspend the current coroutine until it is woken up. */
static void
wakeup_queue_suspend_this(struct wakeup_queue *queue)
{
	struct wakeup_entry entry;
	entry.coro = coro_this();
	entry.is_in_queue = true;
	rlist_add_tail_entry(&queue->coros, &entry, base);
	coro_suspend();
	if (entry.is_in_queue) {
		rlist_del_entry(&entry, base);
	}
}

/** Wakeup the first coroutine in the queue. */
static void
wakeup_queue_wakeup_first(struct wakeup_queue *queue)
{
	if (rlist_empty(&queue->coros))
		return;
	struct wakeup_entry *entry = rlist_first_entry(&queue->coros,
		struct wakeup_entry, base);
    entry->is_in_queue = false;
    rlist_del_entry(entry, base);
	coro_wakeup(entry->coro);
}

static void
wakeup_queue_wakeup_all(struct wakeup_queue *queue)
{
	std::vector<struct wakeup_entry*> entries_to_wake;
	struct wakeup_entry *entry;
	rlist_foreach_entry(entry, &queue->coros, base) {
		entries_to_wake.push_back(entry);
	}
	for (struct wakeup_entry* wake_entry : entries_to_wake) {
		wake_entry->is_in_queue = false;
		rlist_del_entry(wake_entry, base);
		coro_wakeup(wake_entry->coro);
	}
}

struct coro_bus_channel {
	/** Channel max capacity. */
	size_t size_limit;
	/** Coroutines waiting until the channel is not full. */
	struct wakeup_queue send_queue;
	/** Coroutines waiting until the channel is not empty. */
	struct wakeup_queue recv_queue;
	/** Message queue. */
	std::deque<unsigned> data;
	bool is_closed;

	coro_bus_channel(size_t limit) : size_limit(limit), is_closed(false) {
		wakeup_queue_create(&send_queue);
		wakeup_queue_create(&recv_queue);
	}

	~coro_bus_channel() {
		data.clear();
	}
};

struct coro_bus {
	std::vector<coro_bus_channel*> channels;
};

static enum coro_bus_error_code global_error = CORO_BUS_ERR_NONE;

enum coro_bus_error_code
coro_bus_errno(void)
{
	return global_error;
}

void
coro_bus_errno_set(enum coro_bus_error_code err)
{
	global_error = err;
}

struct coro_bus *
coro_bus_new(void)
{
	struct coro_bus *bus = new coro_bus();
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	return bus;
}

void
coro_bus_delete(struct coro_bus *bus)
{
	if (bus == nullptr) {
		return;
	}
	for (size_t i = 0; i < bus->channels.size(); ++i) {
		if (bus->channels[i]) {
			coro_bus_channel_close(bus, i);
			coro_yield();
		}
	}
	delete bus;
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
}

int
coro_bus_channel_open(struct coro_bus *bus, size_t size_limit)
{
	for (size_t i = 0; i < bus->channels.size(); ++i) {
		if (bus->channels[i] == nullptr) {
			bus->channels[i] = new coro_bus_channel(size_limit);
			coro_bus_errno_set(CORO_BUS_ERR_NONE);
			return i;
		}
	}

	int new_id = bus->channels.size();
	bus->channels.push_back(new coro_bus_channel(size_limit));
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	return new_id;
}

void
coro_bus_channel_close(struct coro_bus *bus, int channel)
{
	if (bus == nullptr || channel < 0 || channel >= static_cast<int>(bus->channels.size()) ||
	    bus->channels[channel] == nullptr) {
		return;
	}

	struct coro_bus_channel *ch = bus->channels[channel];
	ch->is_closed = true;

	wakeup_queue_wakeup_all(&ch->send_queue);
	wakeup_queue_wakeup_all(&ch->recv_queue);

	bus->channels[channel] = nullptr;
	delete ch;
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
}

int
coro_bus_send(struct coro_bus *bus, int channel, unsigned data)
{
	while (true) {
		if (!bus || channel < 0 || channel >= static_cast<int>(bus->channels.size()) ||
			bus->channels[channel] == nullptr || bus->channels[channel]->is_closed) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}

		coro_bus_channel *ch = bus->channels[channel];
		int result = coro_bus_try_send(bus, channel, data);
		if (result == 0) {
			return 0;
		}

		if (coro_bus_errno() != CORO_BUS_ERR_WOULD_BLOCK) {
			return -1;
		}

		wakeup_queue_suspend_this(&ch->send_queue);
	}
}

int
coro_bus_try_send(struct coro_bus *bus, int channel, unsigned data)
{
	if (bus == nullptr || channel < 0 || channel >= static_cast<int>(bus->channels.size()) ||
	    bus->channels[channel] == nullptr || bus->channels[channel]->is_closed) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}

	coro_bus_channel *ch = bus->channels[channel];
	if (ch->data.size() >= ch->size_limit) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}

	ch->data.push_back(data);
	wakeup_queue_wakeup_first(&ch->recv_queue);

	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	return 0;
}

int
coro_bus_recv(struct coro_bus *bus, int channel, unsigned *data)
{
	while (true) {
		if (bus == nullptr || channel < 0 || channel >= static_cast<int>(bus->channels.size()) ||
			bus->channels[channel] == nullptr || bus->channels[channel]->is_closed) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}

		coro_bus_channel *ch = bus->channels[channel];
		int result = coro_bus_try_recv(bus, channel, data);
		if (result == 0) {
			return 0;
		}

		if (coro_bus_errno() != CORO_BUS_ERR_WOULD_BLOCK) {
			return -1;
		}

		wakeup_queue_suspend_this(&ch->recv_queue);
	}
}

int
coro_bus_try_recv(struct coro_bus *bus, int channel, unsigned *data)
{
	if (bus == nullptr || channel < 0 || channel >= static_cast<int>(bus->channels.size()) ||
	    bus->channels[channel] == nullptr || bus->channels[channel]->is_closed) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}

	coro_bus_channel *ch = bus->channels[channel];
	if (ch->data.empty()) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}

	*data = ch->data.front();
	ch->data.pop_front();
	wakeup_queue_wakeup_first(&ch->send_queue);

	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	return 0;
}


#if NEED_BROADCAST

int
coro_bus_broadcast(struct coro_bus *bus, unsigned data)
{
	while (true) {
		int result = coro_bus_try_broadcast(bus, data);
		if (result == 0) {
			return 0;
		}

		if (coro_bus_errno() != CORO_BUS_ERR_WOULD_BLOCK) {
			return -1;
		}

		for (size_t i = 0; i < bus->channels.size(); ++i) {
			struct coro_bus_channel *ch = bus->channels[i];
			if (ch != nullptr && ch->data.size() >= ch->size_limit) {
				wakeup_queue_suspend_this(&ch->send_queue);
				break;
			}
		}
	}
}

int
coro_bus_try_broadcast(struct coro_bus *bus, unsigned data)
{
	if (bus == nullptr) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}

	bool has_channels = false;
	for (size_t i = 0; i < bus->channels.size(); ++i) {
		if (bus->channels[i] != nullptr && !(bus->channels[i]->is_closed)) {
			has_channels = true;
			break;
		}
	}

	if (!has_channels) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}

	for (size_t i = 0; i < bus->channels.size(); ++i) {
		coro_bus_channel *ch = bus->channels[i];
		if (ch != nullptr && !(ch->is_closed) && ch->data.size() >= ch->size_limit) {
			coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
			return -1;
		}
	}

	for (size_t i = 0; i < bus->channels.size(); ++i) {
		coro_bus_channel *ch = bus->channels[i];
		if (ch != nullptr && !(ch->is_closed)) {
			ch->data.push_back(data);
			wakeup_queue_wakeup_first(&ch->recv_queue);
		}
	}

	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	return 0;
}

#endif

#if NEED_BATCH

int
coro_bus_send_v(struct coro_bus *bus, int channel, const unsigned *data, unsigned count)
{
    unsigned sent_total = 0;
    const unsigned *current_data = data;

    while (true) {
        if (bus == nullptr || channel < 0 || channel >= static_cast<int>(bus->channels.size()) ||
			bus->channels[channel] == nullptr || bus->channels[channel]->is_closed) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}

        coro_bus_channel *ch = bus->channels[channel];
        int result = coro_bus_try_send_v(bus, channel, current_data, count - sent_total);
        if (result > 0) {
            sent_total += result;
            current_data += result;

            if (sent_total == count) {
                coro_bus_errno_set(CORO_BUS_ERR_NONE);
                return sent_total;
            }
            continue;
        }

		if (coro_bus_errno() != CORO_BUS_ERR_WOULD_BLOCK) {
			return -1;
		}

		if (sent_total > 0) {
			coro_bus_errno_set(CORO_BUS_ERR_NONE);
			return sent_total;
		}

		wakeup_queue_suspend_this(&ch->send_queue);
    }
}

int
coro_bus_try_send_v(struct coro_bus *bus, int channel, const unsigned *data, unsigned count)
{
	if (bus == nullptr || channel < 0 || channel >= static_cast<int>(bus->channels.size()) ||
        bus->channels[channel] == nullptr || bus->channels[channel]->is_closed) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }

    coro_bus_channel *ch = bus->channels[channel];
    if (ch->data.size() >= ch->size_limit) {
        coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
        return -1;
    }

    size_t msg_av = ch->size_limit - ch->data.size();
    unsigned to_send = count < msg_av ? count : msg_av;

    for (unsigned i = 0; i < to_send; ++i) {
        ch->data.push_back(data[i]);
    }

    for (unsigned i = 0; i < to_send && !rlist_empty(&ch->recv_queue.coros); ++i) {
        wakeup_queue_wakeup_first(&ch->recv_queue);
    }

    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    return to_send;
}

int
coro_bus_recv_v(struct coro_bus *bus, int channel, unsigned *data, unsigned capacity)
{
    unsigned received_total = 0;
    unsigned *current_data = data;

	while (true) {
        if (bus == nullptr || channel < 0 || channel >= static_cast<int>(bus->channels.size()) ||
			bus->channels[channel] == nullptr || bus->channels[channel]->is_closed) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}

        coro_bus_channel *ch = bus->channels[channel];
        int result = coro_bus_try_recv_v(bus, channel, current_data, capacity - received_total);

        if (result > 0) {
            received_total += result;
            current_data += result;

            if (received_total == capacity) {
                coro_bus_errno_set(CORO_BUS_ERR_NONE);
                return received_total;
            }

            if (!rlist_empty(&ch->recv_queue.coros)) {
                coro_yield();
            }
            continue;
        }

		if (coro_bus_errno() != CORO_BUS_ERR_WOULD_BLOCK) {
			return -1;
		}

		if (received_total > 0) {
			coro_bus_errno_set(CORO_BUS_ERR_NONE);
			return received_total;
		}

		wakeup_queue_suspend_this(&ch->recv_queue);
    }
}

int
coro_bus_try_recv_v(struct coro_bus *bus, int channel, unsigned *data, unsigned capacity)
{
	 if (bus == nullptr || channel < 0 || channel >= static_cast<int>(bus->channels.size()) ||
        bus->channels[channel] == nullptr || bus->channels[channel]->is_closed) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }

    coro_bus_channel *ch = bus->channels[channel];
    if (ch->data.empty()) {
        coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
        return -1;
    }

    unsigned to_recv = capacity < ch->data.size() ? capacity : ch->data.size();

    for (unsigned i = 0; i < to_recv; ++i) {
        data[i] = ch->data.front();
        ch->data.pop_front();
    }

	for (unsigned i = 0; i < to_recv && !rlist_empty(&ch->send_queue.coros); ++i) {
		wakeup_queue_wakeup_first(&ch->send_queue);
	}

    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    return to_recv;
}

#endif
