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

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/errno.h>
#include <sys/param.h>

#include <webhdfs/webhdfs.h>
#include <webhdfs/compat.h>
#include "webhdfs_p.h"
#include "globals.h"
#include "posix.h"

int __POSIX_FD_CNT=0;
webhdfs_file_t **__POSIX_FD;

int open(const char *path, int flags, ...)
{
    va_list ap;
    int mode;
    webhdfs_file_t *file;

    if ((flags & O_CREAT) != 0) {
            va_start(ap, flags);
            mode = va_arg(ap, int);
            va_end(ap);
    } else {
            mode = 0;
    }

    /* we bascially ignore mode */
    printf("calling file open\n");
    if ((file = webhdfs_file_open(__WEBHDFS_POSIX, path)) == NULL)
        return(-EIO);

    realloc(__POSIX_FD,(__POSIX_FD_CNT+1)*sizeof(webhdfs_file_t));
    __POSIX_FD[__POSIX_FD_CNT]=file;

    printf("fd: %u\n",__POSIX_FD_CNT);
    return (__POSIX_FD_CNT);
}

int close(int filedes)
{
    webhdfs_file_close(__POSIX_FD[filedes]);
    return(0);
}

int fstat(int filedes, struct stat *stat) {
    char npath[MAXPATHLEN];
    realpath(__POSIX_FD[filedes]->path,npath);
    return(webhdfs_compat_stat(__WEBHDFS_POSIX,npath,stat));
}

int fchdir(int filedes)
{
    /* we should really check that our fildes is a dir, but that slows us down */
    return(0);
}
