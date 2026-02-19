#include "parser.h"

#include <assert.h>
#include <stdio.h>
#include <unistd.h>
#include <vector>
#include <fcntl.h>
#include <sys/wait.h>
#include <algorithm>


static int builtin_exit_code(const command& cmd) {
    if (cmd.exe == "cd") {
        if (cmd.args.empty()) {
            fprintf(stderr, "cd: missing argument\n");
            return 1;
        }
        if (chdir(cmd.args[0].c_str()) == -1) {
            perror("cd");
            return 1;
        }
        return 0;
    }
	if (cmd.exe == "exit") {
        if (cmd.args.empty()) {
            return 0;
        }
        return atoi(cmd.args[0].c_str());
    }
    return -1;
}

static int handle_single_builtin(const command& cmd, int* last_exit_code, int* is_exit_parent) {
    int code = builtin_exit_code(cmd);
    if (code == -1) {
        return -1;
    }
    if (cmd.exe == "exit" && cmd.args.empty()) {
        *is_exit_parent = 1;
    }
    *last_exit_code = code;
    return code;
}

static void setup_child_io(int prev_read, int pipe_fd[2], size_t i, size_t n,
                            enum output_type out_type, const std::string& out_file) {
    if (prev_read != -1) {
        dup2(prev_read, STDIN_FILENO);
        close(prev_read);
    }

    if (i < n - 1) {
        dup2(pipe_fd[1], STDOUT_FILENO);
        close(pipe_fd[1]);
        if (pipe_fd[0] != -1) {
			close(pipe_fd[0]);
		}
    } else {
        if (out_type != OUTPUT_TYPE_STDOUT) {
            int flags = O_WRONLY | O_CREAT;
            flags |= (out_type == OUTPUT_TYPE_FILE_NEW) ? O_TRUNC : O_APPEND;
            int fd = open(out_file.c_str(), flags, 0666);
            if (fd == -1) {
                perror("open");
                exit(1);
            }
            dup2(fd, STDOUT_FILENO);
            close(fd);
        }
    }

    if (prev_read != -1) {
		close(prev_read);
	}
    if (pipe_fd[0] != -1 && pipe_fd[0] != prev_read) {
		close(pipe_fd[0]);
	}
    if (pipe_fd[1] != -1 && pipe_fd[1] != STDOUT_FILENO) {
		close(pipe_fd[1]);
	}
}

static std::vector<char*> build_argv(const command& cmd) {
    std::vector<char*> argv;
    argv.push_back(const_cast<char*>(cmd.exe.c_str()));
    for (const auto& arg : cmd.args) {
        argv.push_back(const_cast<char*>(arg.c_str()));
    }
    argv.push_back(nullptr);
    return argv;
}

static void run_builtin_or_exec(const command& cmd) {
    int code = builtin_exit_code(cmd);
    if (code != -1) {
        exit(code);
    }

    std::vector<char*> argv = build_argv(cmd);
    execvp(cmd.exe.c_str(), argv.data());
    fprintf(stderr, "%s: command not found\n", cmd.exe.c_str());
    exit(127);
}

static int wait_for_children(std::vector<pid_t>& pids, pid_t last_pid) {
    int last_status = 0;
    while (!pids.empty()) {
        int status;
        pid_t done = waitpid(-1, &status, 0);
        if (done == -1) {
            if (errno == ECHILD) {
                break;
            }
            perror("waitpid");
            break;
        }
        auto it = std::find(pids.begin(), pids.end(), done);
        if (it != pids.end()) {
            if (done == last_pid) {
                if (WIFEXITED(status)) {
					last_status = WEXITSTATUS(status);
				} else if (WIFSIGNALED(status)) {
					last_status = 128 + WTERMSIG(status);
				} else {
					last_status = 1;
				}
            }
            pids.erase(it);
        }
    }
    return last_status;
}

static int execute_pipeline(const std::vector<const expr*>& cmds, enum output_type out_type, const std::string& out_file,
                            bool is_background, int *last_exit_code) {
	size_t n = cmds.size();
    std::vector<pid_t> pids;
    int prev_read = -1;
    int exit_code_override = -1;

	for (size_t i = 0; i < n; ++i) {
		int pipe_fd[2] = {-1, -1};
        if (i < n - 1) {
            if (pipe(pipe_fd) == -1) {
                perror("pipe");
                if (prev_read != -1) {
                    close(prev_read);
                }
                return -1;
            }
        }

        const command& cmd = *cmds[i]->cmd;
        if (!(is_background) && i == n - 1 && cmd.exe == "exit") {
            exit_code_override = cmd.args.empty() ? 0 : atoi(cmd.args[0].c_str());
            if (prev_read != -1) {
                close(prev_read);
            }
            if (pipe_fd[0] != -1) {
                close(pipe_fd[0]);
            }
            if (pipe_fd[1] != -1) {
                close(pipe_fd[1]);
            }
            break;
        }

		pid_t pid = fork();
		if (pid == -1) {
			perror("fork");
			if (pipe_fd[0] != -1) {
                close(pipe_fd[0]);
            }
			if (pipe_fd[1] != -1) {
                close(pipe_fd[1]);
            }
            if (prev_read != -1) {
                close(prev_read);
            }
			return -1;
		}

		if (pid == 0) {
			setup_child_io(prev_read, pipe_fd, i, n, out_type, out_file);
			run_builtin_or_exec(cmd);
		} else {
			pids.push_back(pid);

			if (prev_read != -1) {
				close(prev_read);
				prev_read = -1;
			}
			if (i < n - 1) {
				prev_read = pipe_fd[0];
				close(pipe_fd[1]);
			} else {
				if (pipe_fd[0] != -1) {
					close(pipe_fd[0]);
				}
				if (pipe_fd[1] != -1) {
					close(pipe_fd[1]);
				}
			}
		}
	}

    if (prev_read != -1) {
        close(prev_read);
    }

    if (is_background) {
        return 0;
    }

    if (exit_code_override != -1) {
        while (!pids.empty()) {
            int status;
            pid_t done = waitpid(-1, &status, 0);
            if (done == -1) {
                if (errno == ECHILD) {
                    break;
                }
                perror("waitpid");
                break;
            }
            auto it = std::find(pids.begin(), pids.end(), done);
            if (it != pids.end()) {
                pids.erase(it);
            }
        }
        *last_exit_code = exit_code_override;
        return 0;
    }

    pid_t last_pid = pids.empty() ? -1 : pids.back();
    *last_exit_code = wait_for_children(pids, last_pid);
    return 0;
}

static int execute_sequence(const struct command_line *line, int* last_exit_code, int* is_exit_parent) {
    const auto& exprs = line->exprs;
    if (exprs.empty()) {
        return 0;
    }

    int last_result = 0;
    auto it = exprs.begin();

    std::vector<const expr*> group;
    while (it != exprs.end() && it->type != EXPR_TYPE_AND && it->type != EXPR_TYPE_OR) {
        if (it->type == EXPR_TYPE_COMMAND) {
            group.push_back(&(*it));
        }
        ++it;
    }

    bool last_group = (it == exprs.end());

    int group_result;
    int should_exit = 0;
    if (group.size() == 1 && line->out_type == OUTPUT_TYPE_STDOUT) {
        int builtin_ret = handle_single_builtin(*group[0]->cmd, &group_result, is_exit_parent);
        if (builtin_ret != -1) {
            should_exit = builtin_ret;
            last_result = group_result;
        } else {
            if (execute_pipeline(group, last_group ? line->out_type : OUTPUT_TYPE_STDOUT, last_group ? line->out_file : "", false, &group_result) == -1) {
                group_result = 1;
            }
            last_result = group_result;
        }
    } else {
        if (execute_pipeline(group, last_group ? line->out_type : OUTPUT_TYPE_STDOUT, last_group ? line->out_file : "", false, &group_result) == -1) {
            group_result = 1;
        }
        last_result = group_result;
    }
    if (should_exit) {
        return should_exit;
    }

    while (it != exprs.end()) {
        enum expr_type op = it->type;
        ++it;

        std::vector<const expr*> next_group;
        while (it != exprs.end() && it->type != EXPR_TYPE_AND && it->type != EXPR_TYPE_OR) {
            if (it->type == EXPR_TYPE_COMMAND) {
                next_group.push_back(&(*it));
            }
            ++it;
        }
        last_group = (it == exprs.end());

        bool do_execute = false;
        if ((op == EXPR_TYPE_AND && last_result == 0) || (op == EXPR_TYPE_OR && last_result != 0)) {
            do_execute = true;
        }

        if (do_execute) {
            int next_result;
            if (next_group.size() == 1 && line->out_type == OUTPUT_TYPE_STDOUT) {
                int builtin_ret = handle_single_builtin(*next_group[0]->cmd, &next_result, is_exit_parent);
                if (builtin_ret != -1) {
                    should_exit = builtin_ret;
                    last_result = next_result;
                } else {
                    if (execute_pipeline(next_group, last_group ? line->out_type : OUTPUT_TYPE_STDOUT, last_group ? line->out_file : "", false, &next_result) == -1) {
                        next_result = 1;
                    }
                    last_result = next_result;
                }
            } else {
                if (execute_pipeline(next_group, last_group ? line->out_type : OUTPUT_TYPE_STDOUT, last_group ? line->out_file : "", false, &next_result) == -1) {
                    next_result = 1;
                }
                last_result = next_result;
            }
            if (should_exit) {
                return should_exit;
            }
        }
    }

    *last_exit_code = last_result;
    return 0;
}


static int
execute_command_line(struct parser *p, const struct command_line *line, int* last_exit_code, int* is_exit_parent)
{
    if (line->is_background) {
        pid_t pid = fork();
        if (pid == -1) {
            perror("fork");
            return 0;
        }
        if (pid == 0) {
            int child_last = 0, child_exit = 0;
            int should = execute_sequence(line, &child_last, &child_exit);
            parser_delete(p);
            if (should != 0) {
                exit(should);
            }
            if (child_exit) {
                exit(child_last);
            }
            exit(0);
        }
        return 0;
    } else {
        return execute_sequence(line, last_exit_code, is_exit_parent);
    }
}

int
main(void)
{
	const size_t buf_size = 1024;
	char buf[buf_size];
	int rc;
	struct parser *p = parser_new();
	int last_exit_code = 0;

	while ((rc = read(STDIN_FILENO, buf, buf_size)) > 0) {
		parser_feed(p, buf, rc);
		struct command_line *line = NULL;
		while (true) {
			enum parser_error err = parser_pop_next(p, &line);
			if (err == PARSER_ERR_NONE && line == NULL)
				break;
			if (err != PARSER_ERR_NONE) {
				printf("Error: %d\n", (int)err);
				continue;
			}
            int is_exit_parent = 0;
			int should_exit = execute_command_line(p, line, &last_exit_code, &is_exit_parent);
			delete line;
            if ((should_exit == 0 && is_exit_parent) || should_exit != 0) {
                parser_delete(p);
                exit(should_exit);
            }
		}
	}
	parser_delete(p);
    while (waitpid(-1, NULL, 0) != -1) {}
	return last_exit_code;
}
