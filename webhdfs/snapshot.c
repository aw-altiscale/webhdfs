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

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include <yajl/yajl_tree.h>

#include "webhdfs_p.h"
#include "webhdfs.h"

int webhdfs_create_snapshot (webhdfs_t *fs, const char *path, const char *name) {
    webhdfs_req_t req;
    yajl_val node, v;

    webhdfs_req_open(&req, fs, path);
    webhdfs_req_set_args(&req, "op=CREATESNAPSHOT&snapshotname=%s", name);
    webhdfs_req_exec(&req, WEBHDFS_REQ_PUT);
    node = webhdfs_req_json_response(&req);
    webhdfs_req_close(&req);

    if ((v = webhdfs_response_exception(node)) != NULL) {
        yajl_tree_free(node);
        return(1);
    }

    if ((v = webhdfs_response_boolean(node)) != NULL) {
        int failure = YAJL_IS_FALSE(v);
        yajl_tree_free(node);
        return(failure);
    }

    yajl_tree_free(node);
    return(2);
}

int webhdfs_delete_snapshot (webhdfs_t *fs, const char *path, const char *name) {
    webhdfs_req_t req;
    yajl_val node, v;

    webhdfs_req_open(&req, fs, path);
    webhdfs_req_set_args(&req, "op=DELETESNAPSHOT&snapshotname=%s", name);
    webhdfs_req_exec(&req, WEBHDFS_REQ_PUT);
    node = webhdfs_req_json_response(&req);
    webhdfs_req_close(&req);

    if ((v = webhdfs_response_exception(node)) != NULL) {
        yajl_tree_free(node);
        return(1);
    }

    if ((v = webhdfs_response_boolean(node)) != NULL) {
        int failure = YAJL_IS_FALSE(v);
        yajl_tree_free(node);
        return(failure);
    }

    yajl_tree_free(node);
    return(2);
}


int webhdfs_rename_snapshot (webhdfs_t *fs, const char *path, const char *oldname, const char *newname) {
    webhdfs_req_t req;
    yajl_val node, v;

    webhdfs_req_open(&req, fs, path);
    webhdfs_req_set_args(&req, "op=RENAMESNAPSHOT&oldsnapshotname=%s&snapshotname=%s", oldname, newname);
    webhdfs_req_exec(&req, WEBHDFS_REQ_PUT);
    node = webhdfs_req_json_response(&req);
    webhdfs_req_close(&req);

    if ((v = webhdfs_response_exception(node)) != NULL) {
        yajl_tree_free(node);
        return(1);
    }

    if ((v = webhdfs_response_boolean(node)) != NULL) {
        int failure = YAJL_IS_FALSE(v);
        yajl_tree_free(node);
        return(failure);
    }

    yajl_tree_free(node);
    return(2);
}