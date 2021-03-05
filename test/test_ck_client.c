#include <stdio.h>
#include <stdlib.h>
#include <dlfcn.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <time.h>
#include <stddef.h>
#include <stdint.h>
#include "../clickhouse/ck_client.h"

#define BD_IP "127.0.0.1"
#define DB_PORT 9000
#define DB_DATABASE "default"
#define DB_USERNAME "user"
#define DB_PASSWORD "123456"

char *create_tables = "CREATE TABLE test(datadate Date, padding UInt32) ENGINE = MergeTree() PARTITION BY toYYYYMM(datadate) ORDER BY datadate SETTINGS index_granularity = 65536";
#define DB_LIB_PATH "/lib64/libclickhouse_client.so"

#pragma pack(1)

typedef struct client_module_s
{
    void *(*ck_client_init)(char *, int, char *, char *, char *);
    int (*ck_client_execute)(void *, char *, int, void *);
    int (*ck_client_destory)(void *);
    void (*ck_client_free)(void *);
    void *ck_handle;

} client_module_s;

client_module_s *test_init()
{
    client_module_s *client_module = malloc(sizeof(client_module_s));
    if (!client_module)
    {
        return NULL;
    }

    void *handler = dlopen(DB_LIB_PATH, RTLD_LAZY);
    if (handler == NULL)
    {
        free(client_module);
        return NULL;
    }

    client_module->ck_client_init = dlsym(handler, "ck_client_init");
    client_module->ck_client_execute = dlsym(handler, "ck_client_execute");
    client_module->ck_client_destory = dlsym(handler, "ck_client_destory");
    client_module->ck_client_free = dlsym(handler, "ck_client_free");

    dlclose(handler);
    return client_module;
}

int test_create_table(client_module_s *client_module, char *create_sql)
{
    int ret = 0;
    client_module->ck_handle = client_module->ck_client_init(BD_IP, DB_PORT, DB_USERNAME, DB_PASSWORD, DB_DATABASE);
    if (!client_module->ck_handle)
    {
        return -1;
    }
    ck_result_s *result = malloc(sizeof(ck_result_s));
    if (!result)
    {
        ret = -1;
        goto end;
    }
    memset(result, 0, sizeof(ck_result_s));
    ret = client_module->ck_client_execute(client_module->ck_handle, create_tables, 1, (void *)result);
    printf("result->ret_code = %d\n", result->ret_code);
    printf("result->ret_datas = %s\n", (char *)result->ret_datas);

end:
    client_module->ck_client_destory(client_module->ck_handle);
    client_module->ck_client_free(result);
    free(result);
    return ret;
}

int test_show_create_table(client_module_s *client_module)
{
    int ret = 0;
    client_module->ck_handle = client_module->ck_client_init(BD_IP, DB_PORT, DB_USERNAME, DB_PASSWORD, DB_DATABASE);
    if (!client_module->ck_handle)
    {
        return -1;
    }
    ck_result_s *result = malloc(sizeof(ck_result_s));
    if (!result)
    {
        ret = -1;
        goto end;
    }
    memset(result, 0, sizeof(ck_result_s));
    ret = client_module->ck_client_execute(client_module->ck_handle, "drop TABLE test", 1, (void *)result);
    printf("ret = %d\n", ret);
    if (result->ret_code != 0 || strncasecmp(create_tables, (char *)result->ret_datas, result->ret_len) != 0)
    {
        printf("result->ret_code = %d\n", result->ret_code);
        printf("result->ret_datas = %s\n", (char *)result->ret_datas);
        ret = -1;
        goto end;
    }

end:
    client_module->ck_client_destory(client_module->ck_handle);
    client_module->ck_client_free(result);
    free(result);
    return ret;
}

int main()
{
    client_module_s *client_module = test_init();
    int ret;
    if (!client_module)
    {
        return -1;
    }
    ret = test_create_table(client_module, create_tables);
    if (ret < 0)
    {
       goto end;
    }
    ret =  test_show_create_table(client_module);
    if (ret < 0)
    {
        goto end;
    }
    /*ret = test_create_table(client_module, create_tables);
    if (ret != -1)
    {
        goto end;
    }*/
    printf("test success!\n");

end:
    free(client_module);
    return ret;
}