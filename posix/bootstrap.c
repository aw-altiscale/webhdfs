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

#include "globals.h"
#include "proxy.h"


void webhdfs_conf_defaults ()
{
    struct passwd *pw = getpwuid(getuid());
    char *hostname=malloc(HOST_NAME_MAX);

    if ((POSIX_PROXY_CONF = webhdfs_conf_alloc()) == NULL) {
        perror("webhdfs conf object");
        return(NULL);
    }

    POSIX_PROXY_CONF->hdfs_host = strdup("YAJL_GET_STRING(v)");

    POSIX_PROXY_CONF->hdfs_user = strdup(pw->pw_name);

    POSIX_PROXY_CONF->webhdfs_port = 50070;

    return(conf);
}

void webhdfs_easy_bootstrap(void)
{
    struct passwd *pw = getpwuid(getuid());
    char *configdir = pw->pw_dir;
    char *filename=malloc(256);

    snprintf(filename,255,"%s/.webhdfsrc.json",configdir);

    if (access(filename,F_OK) == -1) {
        configdir=getenv("HADOOP_CONFIG_DIR");
        snprintf(filename,255,"%s/webhdfs.json",configdir);
        if (access(filename,F_OK) == -1) {
            configdir=getenv("HADOOP_PREFIX");
            snprintf(filename,255,"%s/webhdfs.json",configdir);
            if (access(filename,F_OK) == -1) {
                webhdfs_conf_defaults(void);
                return(void);
            }
        }
    }

    /* Setup webhdfs config */
    POSIX_PROXY_CONF=webhdfs_conf_load(filename);
}
