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

#ifndef _WEBHDFS_COMPAT_H_
#define _WEBHDFS_COMPAT_H_

#include <sys/stat.h>
#include <sys/statvfs.h>

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

int webhdfs_compat_statfs (webhdfs_t *fs, const char *path, struct statvfs *stat);

int webhdfs_compat_chmod (webhdfs_t *fs,
                                 const char *path,
                                 mode_t mode);


int webhdfs_compat_chown (webhdfs_t *fs,
                              const char *path,
                              uid_t uid,
                              gid_t gid);

int webhdfs_compat_mkdir (webhdfs_t *fs,
                               const char *path,
                               mode_t mode);

int webhdfs_compat_rmdir (webhdfs_t *fs,
                                const char *path);

int webhdfs_compat_stat (webhdfs_t *fs, const char *path, struct stat *stat);

int webhdfs_compat_unlink (webhdfs_t *fs, const char *path);

int webhdfs_compat_rename (webhdfs_t *fs,const char *path, const char *newpath);


#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */

#endif /* _WEBHDFS_COMPAT_H_ */

