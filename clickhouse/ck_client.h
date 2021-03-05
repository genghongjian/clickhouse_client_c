#ifndef _CK_CLIENT_H_
#define _CK_CLIENT_H_

#ifdef __cplusplus
extern "C"
{
#endif
    enum Code
    {
        Void = 0,
        Int8,
        Int16,
        Int32,
        Int64,
        UInt8,
        UInt16,
        UInt32,
        UInt64,
        Float32,
        Float64,
        String,
        FixedString,
        DateTime,
        Date,
        Array,
        Nullable,
        Tuple,
        Enum8,
        Enum16,
        UUID,
        IPv4,
        IPv6,
    };

    /*列的描述*/
    typedef struct columns_desc_s
    {
        int col_len; //列的长度
        void *column_datas;//数据
    } columns_desc_s;

    /*
    用来存放数据的结构
*/
    typedef struct ck_result_s
    {
        int ret_code;    //数据库返回码
        int row_cnt;
        int columns_cnt; //列的数目
        columns_desc_s *columns;
		int ret_len;
        void *ret_datas;   //存放返回数据
    }  ck_result_s;

    /*
    *@function name: ck_init
    *@Description：连接clickhouse服务器
    *@param serip：服务器IP
    *@param port：服务器端口
    *@param user：用户名
    *@param password：密码
    *@param database：数据库名称
    *@return: 成功返回一个操作句柄， 失败 NULL
    */
    void *ck_client_init(char *serip, int port, char *user, char *password, char *database);

    /*
    *@function name:ck_execute_query
    *@Description：执行一条sql语句
    *@param query：sql语句
    *@param type：查询类型：0：select 查询，非0：其他查询
    *@param data：指向存放数据库返回数据内存空间
    *@return: 成功 0 ，失败 -1
    */
    int ck_client_execute(void *handle, char *query, int type, void *datas);

	/*
	*@function name:ck_client_destory
	*@Description：销毁ck客户端相关信息
	*@param ck_handle：客户端操作句柄
	*@return: 成功 0 ，失败 -1
	*/
    int ck_client_destory(void *ck_handle);

	/*
	*@function name:ck_client_resp_code
	*@Description：返回数据库返回码
	*@param result：返回信息的结构
	*/

	int ck_client_resp_code(void *result);


	/*
	*@function name:ck_client_resp_len
	*@Description：返回数据库返回数据的长度
	*@param result：返回信息的结构
	*/

	int ck_client_resp_len(void *result);


	/*
	*@function name:ck_client_resp_data
	*@Description：返回数据库返回数据的长度
	*@param result：返回信息的结构
	*/

	void *ck_client_resp_data(void *result);
	/*
	*@function name:ck_client_free
	*@Description：销毁返回相关信息
	*@param result_data：返回信息的结构
	*/
	void ck_client_free(void *result);

#ifdef __cplusplus
}
#endif
#endif // _CK_CLIENT_H