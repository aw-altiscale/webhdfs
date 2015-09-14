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
#include "globals.h"
#include "posix.h"

int webhdfs_posix_bootstrap()
{
        /* Setup webhdfs config */
    __WEBHDFS_POSIX_CONF=webhdfs_easy_bootstrap();

    if ((__WEBHDFS_POSIX = webhdfs_connect(__WEBHDFS_POSIX_CONF)) == NULL) {
        return(-2);
    }
    return(0);
}

int chmod(const char *path, mode_t mode)
{
    char npath[MAXPATHLEN];
    realpath(path,npath);
    return(webhdfs_compat_chmod(__WEBHDFS_POSIX,npath,mode));
}

int chown(const char *path, uid_t uid, gid_t gid)
{
    char npath[MAXPATHLEN];
    realpath(path,npath);
    return(webhdfs_compat_chown(__WEBHDFS_POSIX,npath,uid,gid));
}

char *getcwd(char *buf, size_t len)
{
    char ourbuf[MAXPATHLEN];

    snprintf(ourbuf,MAXPATHLEN,"%s",webhdfs_home_dir(__WEBHDFS_POSIX));
    if (strlen (ourbuf) >= len) {
      errno = ERANGE;
      return 0;
    }
    if (!buf) {
       buf = (char*)malloc(len);
       if (!buf) {
           errno = ENOMEM;
       return 0;
       }
    }
    strcpy (buf, ourbuf);
    return buf;
}

int mkdir(const char *path, mode_t mode)
{
    char npath[MAXPATHLEN];
    realpath(path,npath);
    return(webhdfs_compat_mkdir(__WEBHDFS_POSIX,npath,mode));
}

int rename(const char *path, const char *newpath) {
    char opath[MAXPATHLEN],npath[MAXPATHLEN];
    realpath(path,opath);
    realpath(newpath,npath);
    return(webhdfs_compat_rename(__WEBHDFS_POSIX,opath,npath));
}

int rmdir(const char *path)
{
    char npath[MAXPATHLEN];
    realpath(path,npath);
    return(webhdfs_compat_rmdir(__WEBHDFS_POSIX,npath));
}

int stat(const char *path, struct stat *stat) {
    char npath[MAXPATHLEN];
    realpath(path,npath);
    return(webhdfs_compat_stat(__WEBHDFS_POSIX,npath,stat));
}

int statfs (const char *path, struct statvfs *stat) {
    char npath[MAXPATHLEN];
    realpath(path,npath);
    return(webhdfs_compat_statfs(__WEBHDFS_POSIX,npath,stat));
}

int unlink(const char *path) {
    char npath[MAXPATHLEN];
    realpath(path,npath);
    return(webhdfs_compat_unlink(__WEBHDFS_POSIX,npath));
}
