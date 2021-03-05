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
#define DB_DATABASE "ncdatas"
#define DB_USERNAME "ncompass"
#define DB_PASSWORD "3edc2wsx1qaz"

char *create_tables = "CREATE TABLE ncdatas.data_17_test(`datasource_id` UInt64,`datacenter_id` UInt64,`web_ifid` UInt64,`vm_datasource_id` UInt64,`src_mac` UInt64,`dst_mac` UInt64,`app_id` UInt64,`src_site_id` UInt64,`dst_site_id` UInt64,`client_app_id` UInt64,`app_group_id` UInt64,`src_site_group_id` UInt64,`dst_site_group_id` UInt64,`tenant_id` UInt64,`tenant_group_id` UInt64,`src_mac_vendor` UInt16,`dst_mac_vendor` UInt16,`l3_protocol` UInt16,`l4_protocol` UInt16,`src_country` UInt16,`src_province` UInt16,`src_city` UInt16,`src_isp` UInt16,`dst_country` UInt16,`dst_province` UInt16,`dst_city` UInt16,`dst_isp` UInt16,`vlanid_outer` UInt16,`vlanid_inner` UInt16,`dst_port` UInt16,`network_position` UInt16,`vxlanid` UInt32,`qos` UInt32,`vtep1` UInt32,`vtep2` UInt32,`vtep1_id` UInt64,`vtep2_id` UInt64,`mpls` UInt32,`xff_ip` UInt32,`src_port` UInt16,`ip_version` UInt16,`real_ip` UInt32,`src_ip` IPv6,`dst_ip` IPv6,`dst_ip_port` String,`ip_ip` String,`domain` String,`yara_rule` String,`url` String,`ntw_in_pkts` UInt32,`ntw_out_pkts` UInt32,`ntw_in_bytes` UInt64,`ntw_out_bytes` UInt64,`stream_in_pkts` UInt32,`stream_out_pkts` UInt32,`stream_in_bytes` UInt64,`stream_out_bytes` UInt64,`syn_pkts` UInt32,`synack_pkts` UInt32,`huge_pkts` UInt32,`mid_pkts` UInt32,`tiny_pkts` UInt32,`session_cnt_tot` UInt32,`cnct_request_cnt` UInt32,`cnct_resp_cnt` UInt32,`new_session_cnt` UInt32,`concurrence_cnt` UInt32,`cnct_server_retrans_cnt` UInt32,`cnct_client_retrans_cnt` UInt32,`cnct_server_reset_cnt` UInt32,`cnct_time_tot` UInt32,`cnct_detection_cnt` UInt32,`cnct_server_list_lack` UInt32,`cnct_client_retry_lack` UInt32,`cnct_normal_cnt` UInt32,`cnct_client_no_resp` UInt32,`cnct_server_no_resp` UInt32,`cnct_client_failed_other` UInt32,`cnct_server_failed_other` UInt32,`cnct_client_failed` UInt32,`cnct_server_failed` UInt32,`cnct_abnormal_syn` UInt32,`cnct_abnormal_synack` UInt32,`client_ntw_time_tot` UInt32,`server_ntw_time_tot` UInt32,`ntw_time_cnt` UInt32,`server_no_response_cnt` UInt32,`client_retrans_cnt` UInt32,`server_retrans_cnt` UInt32,`client_side_drop_cnt` UInt32,`server_side_drop_cnt` UInt32,`client_rst_cnt` UInt32,`server_rst_cnt` UInt32,`server_resp_time_tot` UInt32,`server_resp_cnt_tot` UInt32,`server_peak_resp_time` UInt32,`server_resp_fast_cnt` UInt32,`server_resp_slow_cnt` UInt32,`server_resp_timeout_cnt` UInt32,`user_experience_time_tot` UInt32,`user_experience_cnt_tot` UInt32,`client_tiny_window_cnt` UInt32,`server_tiny_window_cnt` UInt32,`client_payload_bytes` UInt32,`server_payload_bytes` UInt32,`client_tiny_ws_keep_time` UInt32,`server_tiny_ws_keep_time` UInt32,`sack_cnt` UInt32,`pcap_bytes` UInt32,`client_retrans_time` UInt32,`server_retrans_time` UInt32,`client_discnct_request_cnt` UInt32,`server_discnct_request_cnt` UInt32,`first_payload_time` UInt32,`disconnect_timeout` UInt32,`port_reuse_cnt` UInt32,`mediator_attack_cnt` UInt32,`client_td_time` UInt32,`server_td_time` UInt32,`client_td_cnt` UInt32,`server_td_cnt` UInt32,`client_awd_time` UInt32,`server_awd_time` UInt32,`client_awd_cnt` Int16,`server_awd_cnt` Int16,`client_min_mss` UInt8,`server_min_mss` UInt8,`client_max_sack_cnt` Int8,`server_max_sack_cnt` Int8,`session_start_ts` Float64,`session_end_ts` Float64,`disconnect_cnt` Float32,`bcast_pkts` Float32,`mcast_pkts` Int64,`ucast_pkts` Int32,`datatime` Int32,`datadate` Date,`padding` UInt16)\
ENGINE = MergeTree() PARTITION BY toYYYYMM(datadate) ORDER BY datadate";

#define DB_LIB_PATH "/home/project/my_project/clickhosue_client/build/clickhouse/libclickhouse_client.so"

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
    ret = client_module->ck_client_execute(client_module->ck_handle, "drop TABLE data_17_test", 1, (void *)result);
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
   /* ret = test_create_table(client_module, create_tables);
    if (ret < 0)
    {
       goto end;
    }*/
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