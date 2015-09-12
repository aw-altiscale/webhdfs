/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <sys/types.h>
#include <execinfo.h>
#include <signal.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <pwd.h>
#include <grp.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <sys/statvfs.h>


#include "webhdfs.h"

#define __ceil_div(a, b)            (((a) + (b) - 1) / (b))

/* ============================================================================
 *  user/group utils
 */
#define GROUP_SIZE_MAX      (1024)      /* sysconf(_SC_GETGR_R_SIZE_MAX) */
#define USER_SIZE_MAX       (1024)      /* sysconf(_SC_GETPW_R_SIZE_MAX) */

int __hdfs_user_uid (const char *name, uid_t *uid) {
    char buffer[USER_SIZE_MAX];
    struct passwd *tmp;
    struct passwd pwd;

    if (getpwnam_r(name, &pwd, buffer, USER_SIZE_MAX, &tmp))
        return(1);

    *uid = pwd.pw_uid;
    return(0);
}

char *__hdfs_user_name (uid_t uid) {
    char buffer[USER_SIZE_MAX];
    struct passwd *tmp;
    struct passwd pwd;

    if (getpwuid_r(uid, &pwd, buffer, USER_SIZE_MAX, &tmp))
        return(NULL);

    return(pwd.pw_name ? strdup(pwd.pw_name) : NULL);
}

int __hdfs_group_gid (const char *name, gid_t *gid) {
    char buffer[GROUP_SIZE_MAX];
    struct group *tmp;
    struct group grp;

    if (getgrnam_r(name, &grp, buffer, GROUP_SIZE_MAX, &tmp))
        return(1);

    *gid = grp.gr_gid;
    return(0);
}

char *__hdfs_group_name (gid_t gid) {
    char buffer[GROUP_SIZE_MAX];
    struct group *tmp;
    struct group grp;

    if (getgrgid_r(gid, &grp, buffer, GROUP_SIZE_MAX, &tmp))
        return(NULL);

    return(grp.gr_name ? strdup(grp.gr_name) : NULL);
}

/* ============================================================================
 *  stat/utils utils
 */
#define HDFS_DEFAULT_GID            99
#define HDFS_DEFAULT_UID            99

#define __hdfs_is_dir(stat)         (!strcmp((stat)->type, "DIRECTORY"))
#define __hdfs_is_file(stat)        (!strcmp((stat)->type, "FILE"))

void __hdfs_stat (const webhdfs_fstat_t *hdfs_stat, struct stat *stat) {
    memset(stat, 0, sizeof(struct stat));

    if (__hdfs_user_uid(hdfs_stat->owner, &(stat->st_uid)))
        stat->st_uid = HDFS_DEFAULT_UID;
    if (__hdfs_group_gid(hdfs_stat->group, &(stat->st_gid)))
        stat->st_gid = HDFS_DEFAULT_GID;

    if (__hdfs_is_dir(hdfs_stat)) {
        stat->st_mode    = S_IFDIR | hdfs_stat->permission;
        stat->st_nlink   = 2;
        stat->st_size    = 4096;
        stat->st_blksize = 4096;
    } else /* if (__hdfs_is_file(hdfs_stat)) */ {
        stat->st_mode    = S_IFREG | hdfs_stat->permission;
        stat->st_nlink   = 1;
        stat->st_size    = hdfs_stat->length;
        stat->st_blksize = hdfs_stat->block;
    }

    stat->st_blocks  = __ceil_div(stat->st_size, stat->st_blksize);
    stat->st_atime   = hdfs_stat->atime;
    stat->st_mtime   = hdfs_stat->mtime;
    stat->st_ctime   = hdfs_stat->mtime;
}

int webhdfs_compat_statfs (webhdfs_t *fs, const char *path, struct statvfs *stat) {
    if (0) {
        stat->f_bsize   = 0x0000;       /* file system block size */
        stat->f_frsize  = 0x0000;       /* fragment size */
        stat->f_blocks  = 0x0000;       /* size of fs in f_frsize units */
        stat->f_bfree   = 0x0000;       /* # free blocks */
        stat->f_bavail  = 0x0000;       /* # free blocks for unprivileged users */
        stat->f_files   = 0x0000;       /* # inodes */
        stat->f_ffree   = 0x0000;       /* # free inodes */
        stat->f_favail  = 0x0000;       /* # free inodes for unprivileged users */
        stat->f_fsid    = 0x0000;       /* file system ID */
        stat->f_flag    = 0x0000;       /* mount flags */
        stat->f_namemax = 0x0000;       /* maximum filename length */
    }

    return(-1);
}

int webhdfs_compat_chmod (webhdfs_t *fs,
                                 const char *path,
                                 mode_t mode) {
    if (webhdfs_chmod(fs, path, mode))
        return(-ENOENT);
    return(0);
}

int webhdfs_compat_chown (webhdfs_t *fs,
                                 const char *path,
                                 uid_t uid,
                                 gid_t gid) {
    char *group;
    char *user;
    int ret;

    if ((user = __hdfs_user_name(uid)) == NULL) {
        return(-EIO);
    }

    if ((group = __hdfs_group_name(gid)) == NULL) {
        free(user);
        return(-EIO);
    }

    ret = 0;
    if (webhdfs_chown(fs, path, user, group))
        ret = -EIO;

    free(group);
    free(user);
    return(ret);
}

int webhdfs_compat_mkdir (webhdfs_t *fs,
                                 const char *path,
                                 mode_t mode) {
    if (webhdfs_mkdir(fs, path, mode))
        return(-EIO);
    return(0);
}

int webhdfs_compat_rmdir (webhdfs_t *fs,
                                 const char *path) {
    if (webhdfs_rmdir(fs, path, 1))
        return(-EIO);
    return(0);
}

int webhdfs_compat_stat (webhdfs_t *fs, const char *path, struct stat *stat) {
    webhdfs_fstat_t *hdfs_stat;

    if ((hdfs_stat = webhdfs_stat(fs, path)) != NULL) {
        __hdfs_stat(hdfs_stat, stat);
        webhdfs_fstat_free(hdfs_stat);
        return(0);
    }

    return(-ENOENT);
}

int webhdfs_compat_unlink (webhdfs_t *fs, const char *path) {
    if (webhdfs_unlink(fs, path))
        return(-EIO);
    return(0);
}

int webhdfs_compat_rename (webhdfs_t *fs,const char *path, const char *newpath) {
    if (webhdfs_rename(fs, path, newpath))
        return(-EIO);
    return(0);
}
