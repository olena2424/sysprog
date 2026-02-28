#include "userfs.h"

#include "rlist.h"

#include <stddef.h>
#include <string>
#include <vector>
#include <cstring>

enum {
	BLOCK_SIZE = 512,
	MAX_FILE_SIZE = 1024 * 1024 * 100,
};

/** Global error code. Set from any function on any error. */
static ufs_error_code ufs_error_code = UFS_ERR_NO_ERR;

struct block {
	/** Block memory. */
	char memory[BLOCK_SIZE];
	/** A link in the block list of the owner-file. */
	rlist in_block_list = RLIST_LINK_INITIALIZER;

	/* PUT HERE OTHER MEMBERS */
};

struct file {
	/**
	 * Doubly-linked intrusive list of file blocks. Intrusiveness of the
	 * list gives you the full control over the lifetime of the items in the
	 * list without having to use double pointers with performance penalty.
	 */
	rlist blocks = RLIST_HEAD_INITIALIZER(blocks);
	/** How many file descriptors are opened on the file. */
	int refs = 0;
	/** File name. */
	std::string name;
	/** A link in the global file list. */
	rlist in_file_list = RLIST_LINK_INITIALIZER;
	size_t size;
    bool alive;
};

/**
 * Intrusive list of all files. In this case the intrusiveness of the list also
 * grants the ability to remove items from any position in O(1) complexity
 * without having to know their iterator.
 */
static rlist file_list = RLIST_HEAD_INITIALIZER(file_list);

struct filedesc {
	file *atfile;
	size_t file_offset;
    block* cur_block;
    size_t offset_in_block;
	int mode;
};

/**
 * An array of file descriptors. When a file descriptor is
 * created, its pointer drops here. When a file descriptor is
 * closed, its place in this array is set to NULL and can be
 * taken by next ufs_open() call.
 */
static std::vector<filedesc*> file_descriptors;

enum ufs_error_code
ufs_errno()
{
	return ufs_error_code;
}

static block*
first_block(file* f) {
    if (rlist_empty(&f->blocks)) {
		return nullptr;
	}
    return rlist_entry(rlist_first(&f->blocks), struct block, in_block_list);
}

static block*
next_block(file* f, block* cur) {
    if (cur == nullptr) {
		return nullptr;
	}
    struct rlist* next = rlist_next(&cur->in_block_list);
    if (next == &f->blocks) {
		return nullptr;
	}
    return rlist_entry(next, struct block, in_block_list);
}

int
ufs_open(const char *filename, int flags)
{
	file* file_to_open = nullptr;
	file* f;
	rlist_foreach_entry(f, &file_list, in_file_list) {
        if (f->alive && f->name == filename) {
            file_to_open = f;
            break;
        }
    }

	if (!file_to_open) {
		if (flags & UFS_CREATE) {
			file_to_open = new file;
			file_to_open->refs = 0;
			file_to_open->name = filename;
			file_to_open->size = 0;
			file_to_open->alive = true;
			rlist_create(&file_to_open->blocks);
			rlist_add_tail_entry(&file_list, file_to_open, in_file_list);
		} else {
			ufs_error_code = UFS_ERR_NO_FILE;
			return -1;
		}
	}

	filedesc* fdsc = new filedesc;
	fdsc->atfile = file_to_open;
	fdsc->file_offset = 0;
	fdsc->cur_block = first_block(file_to_open);
	fdsc->offset_in_block = 0;

	int access_mode = flags & (UFS_READ_ONLY | UFS_WRITE_ONLY | UFS_READ_WRITE);
    if (access_mode == 0) {
		access_mode = UFS_READ_WRITE;
	}
    fdsc->mode = access_mode;

	++(file_to_open->refs);

	int tfd = -1;
	for (size_t i = 0; i < file_descriptors.size(); ++i) {
		if (file_descriptors[i] == nullptr) {
			tfd = (int) i;
			file_descriptors[i] = fdsc;
			break;
		}
	}
	if (tfd == -1) {
		tfd = (int) file_descriptors.size();
		file_descriptors.push_back(fdsc);
	}

	return tfd;
}

ssize_t
ufs_write(int fd, const char *buf, size_t size)
{
	if (fd < 0 || fd >= (int) file_descriptors.size() || file_descriptors[fd] == nullptr) {
		ufs_error_code = UFS_ERR_NO_FILE;
		return -1;
	}

	filedesc* cur_fd = file_descriptors[fd];
	file* cur_file = cur_fd->atfile;

	if (!(cur_fd->mode & UFS_WRITE_ONLY) && cur_fd->mode != UFS_READ_WRITE) {
        ufs_error_code = UFS_ERR_NO_PERMISSION;
        return -1;
    }

	size_t new_file_size = std::max(cur_file->size, cur_fd->file_offset + size);
	if (new_file_size > MAX_FILE_SIZE) {
		ufs_error_code = UFS_ERR_NO_MEM;
		return -1;
	}

	size_t to_write = size;
    block* cur_block = cur_fd->cur_block;
    size_t off = cur_fd->offset_in_block;
    size_t file_off = cur_fd->file_offset;

    while (to_write > 0) {
		if (cur_block == nullptr || off == BLOCK_SIZE) {
			if (cur_block == nullptr) {
				cur_block = first_block(cur_file);
			} else {
				cur_block = next_block(cur_file, cur_block);
			}
			if (cur_block == nullptr) {
				cur_block = new block;
				rlist_add_tail_entry(&cur_file->blocks, cur_block, in_block_list);
			}
			off = 0;
		}
		size_t mem_to_copy = std::min(BLOCK_SIZE - off, to_write);
		std::memcpy(cur_block->memory + off, buf, mem_to_copy);
		off += mem_to_copy;
		file_off += mem_to_copy;
		buf += mem_to_copy;
		to_write -= mem_to_copy;
		if (file_off > cur_file->size) {
			cur_file->size = file_off;
		}
	}

	cur_fd->cur_block = cur_block;
	cur_fd->offset_in_block = off;
	cur_fd->file_offset = file_off;

	return size;
}

ssize_t
ufs_read(int fd, char *buf, size_t size)
{
	if (fd < 0 || fd >= (int) file_descriptors.size() || file_descriptors[fd] == nullptr) {
		ufs_error_code = UFS_ERR_NO_FILE;
		return -1;
	}

	filedesc* cur_fd = file_descriptors[fd];
	file* cur_file = cur_fd->atfile;

    if (!(cur_fd->mode & UFS_READ_ONLY) && cur_fd->mode != UFS_READ_WRITE) {
        ufs_error_code = UFS_ERR_NO_PERMISSION;
        return -1;
    }

	if (cur_fd->file_offset >= cur_file->size) {
		return 0;
	}

	size_t to_read = std::min(size, cur_file->size - cur_fd->file_offset);
	size_t success_read = to_read;
    block* cur_block = cur_fd->cur_block;
    size_t off = cur_fd->offset_in_block;
    size_t file_off = cur_fd->file_offset;

    while (to_read > 0) {
		if (cur_block == nullptr || off == BLOCK_SIZE) {
			if (cur_block == nullptr) {
				cur_block = first_block(cur_file);
			} else {
				cur_block = next_block(cur_file, cur_block);
			}
			if (cur_block == nullptr) {
				break;
			}
			off = 0;
		}
		size_t mem_to_copy = std::min(BLOCK_SIZE - off, to_read);
		std::memcpy(buf, cur_block->memory + off, mem_to_copy);
		off += mem_to_copy;
		file_off += mem_to_copy;
		buf += mem_to_copy;
		to_read -= mem_to_copy;
	}

	cur_fd->cur_block = cur_block;
	cur_fd->offset_in_block = off;
	cur_fd->file_offset = file_off;

	return success_read;
}

int
ufs_close(int fd)
{
	if (fd < 0 || fd >= (int) file_descriptors.size() || file_descriptors[fd] == nullptr) {
		ufs_error_code = UFS_ERR_NO_FILE;
		return -1;
	}

	filedesc* cur_fd = file_descriptors[fd];
	file* cur_file = cur_fd->atfile;

	--(cur_file->refs);
	delete cur_fd;
	file_descriptors[fd] = nullptr;

	if (cur_file->refs == 0 && !cur_file->alive) {
        rlist_del_entry(cur_file, in_file_list);
        while (!rlist_empty(&cur_file->blocks)) {
            block* b = rlist_entry(rlist_first(&cur_file->blocks), struct block, in_block_list);
            rlist_del(&b->in_block_list);
            delete b;
        }
        delete cur_file;
    }

	return 0;
}

int
ufs_delete(const char *filename)
{
	file* file_to_del = nullptr;
    file* f;
    rlist_foreach_entry(f, &file_list, in_file_list) {
        if (f->alive && f->name == filename) {
            file_to_del = f;
            break;
        }
    }
    if (!file_to_del) {
        ufs_error_code = UFS_ERR_NO_FILE;
        return -1;
    }

    file_to_del->alive = false;
    if (file_to_del->refs == 0) {
        rlist_del_entry(file_to_del, in_file_list);
        while (!rlist_empty(&file_to_del->blocks)) {
            block* b = rlist_entry(rlist_first(&file_to_del->blocks), struct block, in_block_list);
            rlist_del(&b->in_block_list);
            delete b;
        }
        delete file_to_del;
    }
    return 0;
}

#if NEED_RESIZE

int
ufs_resize(int fd, size_t new_size)
{
	if (fd < 0 || fd >= (int) file_descriptors.size() || file_descriptors[fd] == nullptr) {
        ufs_error_code = UFS_ERR_NO_FILE;
        return -1;
    }
    filedesc* cur_fd = file_descriptors[fd];
    file* cur_file = cur_fd->atfile;

    if (!(cur_fd->mode & UFS_WRITE_ONLY) && cur_fd->mode != UFS_READ_WRITE) {
        ufs_error_code = UFS_ERR_NO_PERMISSION;
        return -1;
    }

    if (new_size > MAX_FILE_SIZE) {
        ufs_error_code = UFS_ERR_NO_MEM;
        return -1;
    }

    if (new_size == cur_file->size) {
        return 0;
    }

    if (new_size > cur_file->size) {
        size_t blocks_needed = (new_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
        size_t cur_blocks = (cur_file->size + BLOCK_SIZE - 1) / BLOCK_SIZE;
        for (size_t i = cur_blocks; i < blocks_needed; ++i) {
            block* new_block = new block;
            rlist_add_tail_entry(&cur_file->blocks, new_block, in_block_list);
        }
        cur_file->size = new_size;
    } else {
        for (size_t i = 0; i < file_descriptors.size(); ++i) {
            filedesc* desc = file_descriptors[i];
            if (desc && desc->atfile == cur_file && desc->file_offset > new_size) {
                desc->file_offset = new_size;
                if (new_size == 0) {
                    desc->cur_block = nullptr;
                    desc->offset_in_block = 0;
                } else {
                    size_t off = 0;
                    block* b;
                    rlist_foreach_entry(b, &cur_file->blocks, in_block_list) {
                        size_t block_start = off;
                        size_t block_end = off + BLOCK_SIZE;
                        if (new_size > block_start && new_size <= block_end) {
                            desc->cur_block = b;
                            desc->offset_in_block = new_size - block_start;
                            break;
                        }
                        off += BLOCK_SIZE;
                    }
                }
            }
        }

        size_t new_blocks = (new_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
        size_t cur_blocks = (cur_file->size + BLOCK_SIZE - 1) / BLOCK_SIZE;
        if (new_blocks < cur_blocks) {
            size_t to_remove = cur_blocks - new_blocks;
            block *b, *tmp;
            rlist_foreach_entry_safe_reverse(b, &cur_file->blocks, in_block_list, tmp) {
                if (to_remove == 0) break;
                rlist_del(&b->in_block_list);
                delete b;
                --to_remove;
            }
        }
        cur_file->size = new_size;
    }
    return 0;
}

#endif

void
ufs_destroy(void)
{
	for (size_t i = 0; i < file_descriptors.size(); ++i) {
        delete file_descriptors[i];
    }
    std::vector<filedesc*>().swap(file_descriptors);

    while (!rlist_empty(&file_list)) {
        file* f = rlist_entry(rlist_first(&file_list), struct file, in_file_list);
        rlist_del_entry(f, in_file_list);
        while (!rlist_empty(&f->blocks)) {
            block* b = rlist_entry(rlist_first(&f->blocks), struct block, in_block_list);
            rlist_del(&b->in_block_list);
            delete b;
        }
        delete f;
    }
}
