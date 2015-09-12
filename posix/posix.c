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

#include "webhdfs.h"
#include "compat.h"
#include "globals.h"
#include "posix.h"

webhdfs_conf_t *__WEBHDFS_CONF;
webhdfs_t *__WEBHDFS;

int webhdfs_posix_bootstrap()
{
        /* Setup webhdfs config */
    __WEBHDFS_CONF=webhdfs_easy_bootstrap();

    if ((__WEBHDFS = webhdfs_connect(__WEBHDFS_CONF)) == NULL) {
        return(-2);
    }
    return(0);
}

int chmod (const char *path, mode_t mode)
{
    return(webhdfs_compat_chmod(__WEBHDFS,path,mode));
}

int chown (const char *path, uid_t uid, gid_t gid)
{
    return(webhdfs_compat_chown(__WEBHDFS,path,uid,gid));
}


int mkdir(const char *path, mode_t mode)
{
    return(webhdfs_compat_mkdir(__WEBHDFS,path,mode));
}

int rename(const char *path, const char *newpath) {
    return(webhdfs_compat_rename(__WEBHDFS,path,newpath));
}

int rmdir(const char *path)
{
    return(webhdfs_compat_rmdir(__WEBHDFS,path));
}

int stat (const char *path, struct stat *stat) {
    return(webhdfs_compat_stat(__WEBHDFS,path,stat));
}

int statfs (webhdfs_t *fs, const char *path, struct statvfs *stat) {
    return(webhdfs_compat_statfs(__WEBHDFS,path,stat));
}

int unlink(const char *path) {
    return(webhdfs_compat_unlink(__WEBHDFS,path));
}
