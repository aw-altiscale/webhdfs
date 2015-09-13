#include <webhdfs/webhdfs.h>

int main (int argc, char **argv) {
    webhdfs_conf_t *conf;
    webhdfs_t *fs;

    /* Setup webhdfs config */
    conf = webhdfs_conf_alloc();
    webhdfs_conf_set_server(conf, "10.248.3.78", 50070, 0);
    webhdfs_conf_set_user(conf, "aw");

    /* Connect to WebHDFS */
    fs = webhdfs_connect(conf);

    webhdfs_create_snapshot(fs, "/user/aw","test1");

    /* Disconnect from WebHDFS */
    webhdfs_disconnect(fs);
    webhdfs_conf_free(conf);

    return(0);
}

