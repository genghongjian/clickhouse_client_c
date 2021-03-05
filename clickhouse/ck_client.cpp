#include "client.h"
#include "error_codes.h"
#include "types/types.h"
#include "types/type_parser.h"
#include "columns/string.h"
#include "columns/ip4.h"
#include "columns/ip6.h"
#include <stdexcept>
#include <iostream>
#include <string>
#include "ck_client.h"

using namespace clickhouse;
using namespace std;


/*
*@function name:ck_client_init
*@Description：连接clickhouse服务器
*@param serip：服务器IP
*@param port：服务器端口
*@param user：用户名
*@param password：密码
*@param database：数据库名称
*@return: 成功返回一个操作句柄， 失败 NULL
*/
void *ck_client_init(char *serip, int port, char *user, char *password, char *database)
{
    if (!serip || port < 0 || port > 65535 || !user || !password || !database)
    {
        return NULL;
    }

    try
    {
        auto *handle = new Client(ClientOptions().SetHost(serip).SetPingBeforeQuery(true).SetPort(port).SetDefaultDatabase(database).SetUser(user).SetPassword(password));
        return static_cast<void *>(handle);
    }
    catch (...)
    {
        std::cerr << "connet ck server error!" << std::endl;
        return NULL;
    }
}

/*
*@function name:exception_callbak_func
*@Description： 出现异常的回调函数，将返回的数据放到data中
*@param block：存放返回数据的
*@param data：指向存放数据库返回数据内存空间
*/

void exception_callbak_func(const Exception &exception, char *data)
{

    if (!data)
    {
        std::cout << "data is null:" << std::endl;
        return;
    }
    ck_result_s *query_data = (ck_result_s *)data;
	query_data->ret_code = exception.code;
	query_data->ret_len = exception.display_text.length() + 1;
	query_data->ret_datas = malloc(query_data->ret_len);

	memcpy(query_data->ret_datas, exception.display_text.c_str(), query_data->ret_len);
	return;	
}


/*
*@function name:query_callbak_func
*@Description：执行普通查询的回调函数，将返回的数据放到data中
*@param block：存放返回数据的类
*@param data：指向存放数据库返回数据内存空间
*/

void query_callbak_func(const Block &block, char *data)
{
	 //std::cout << "row_num:" << block.GetRowCount() << std::endl;
	// std::cout << "GetColumnName:" << block.GetColumnName(0) << std::endl;
	 
	/*for (Block::Iterator bi(block); bi.IsValid(); bi.Next())
	{
		std::cout << bi.Name()<<"("<< bi.Type()->GetName()<<" "<<bi.Type()->GetCode()<<")"<<std::endl;
	}*/
	 
    if (!data || block.GetRowCount() == 0)
    {
        std::cout << "data is null:" << std::endl;
        return;
    }
    ck_result_s *query_data = (ck_result_s *)data;

	int length = 0, type_code;

	Block::Iterator bi(block);
	type_code = bi.Type()->GetCode();
	if(type_code == String)
	{
		length = ((*block[0]->As<ColumnString>())[0]).length();
	}
	else if(type_code == UInt64)
	{
		length = 100;
	}
	else
	{
		std::cout << "unkonw GetCode :" <<type_code<< std::endl;
	}

	if(length <= 0)
	{
		return;
	}
	query_data->ret_len = length + 1;
	query_data->ret_datas = malloc(length + 1);
	memset(query_data->ret_datas, 0, query_data->ret_len);
	if(!query_data->ret_datas)
	{
		return;
	}
	char *datas = (char *)query_data->ret_datas;
	if(type_code == String)
	{
		length  = ((*block[0]->As<ColumnString>())[0]).length();
		memcpy(datas, (void *)(string((*block[0]->As<ColumnString>())[0]).c_str()), length);
	}
	else if(type_code == UInt64)
	{
		snprintf(datas, query_data->ret_len, "%ld", (*block[0]->As<ColumnUInt64>())[0]);
		query_data->ret_len  = strlen(datas);
	}
	query_data->ret_code = 0;
}

template <typename ColumnType>
static __inline__ __attribute__((always_inline)) void write_number_column_data(
   const Block &block, size_t column_index, size_t column_size, size_t row_num, char *data)
{
    char *offset_data = data;
	for(size_t i = 0; i < row_num; i++)
	{
		//std::cout<<"column_size"<<column_size<<std::endl;
		//std::cout<<(*block[column_index]->As<ColumnType>())[i]<<std::endl;
		memcpy(offset_data, (void *)&(*block[column_index]->As<ColumnType>())[i], column_size);
    	offset_data += column_size;
	}

}

//最多占用1G内存
#define MAX_COLUMNS_MEMORY 	1073741824

static int ajust_columns_mem(ck_result_s *result_data, int row_cnt)
{
	uint64_t row_lenght = 0, totla_size;
	columns_desc_s * column;
	
	for(int i = 0; i< result_data->columns_cnt; i++)
	{
		column= &result_data->columns[i];
		row_lenght += column->col_len;
	}
	totla_size = row_lenght * (result_data->row_cnt + row_cnt);
	if(totla_size >= MAX_COLUMNS_MEMORY)
	{
		std::cout << "alloc memory over " <<MAX_COLUMNS_MEMORY <<std::endl;
		return -1;
	}
	
	for(int i = 0; i< result_data->columns_cnt; i++)
	{
		 column= &result_data->columns[i];
		if(column->col_len <= 0)
		{
			return -1;
		}
		if(result_data->row_cnt == 0)//第一次申请
		{
			column->column_datas = malloc(column->col_len * row_cnt);
			if(!column->column_datas)
			{
				return -1;
			}
			memset(column->column_datas, 0, column->col_len * row_cnt);
		}
		else
		{
			column->column_datas = realloc(column->column_datas, column->col_len * (row_cnt + result_data->row_cnt));
			if(!column->column_datas)
			{
				return -1;
			}
		}
	}
	return row_cnt;
}

/*
*@function name:select_callbak_func
*@Description：执行查询的回调函数，将返回的数据放到data中
*@param block：存放返回数据的类
*@param data：指向存放数据库返回数据内存空间
*/

void select_callbak_func(const Block &block, char *data)
{
    /*for (Block::Iterator bi(block); bi.IsValid(); bi.Next())
     {
         std::cout << bi.Name()<<"("<< bi.Type()->GetName()<<" "<<bi.Type()->GetCode()<<")"<<std::endl;
     }*/

    if (!data)
    {
        std::cout << "data is null:" << std::endl;
        return;
    }
    ck_result_s *result_data = (ck_result_s *)data;
    int column_num = block.GetColumnCount();
    int row_num = block.GetRowCount();

    //std::cout << "row_num:" << row_num << std::endl;
    //std::cout << "column_num:" << column_num << std::endl;
	//std::cout << "input column num = " << result_data->columns_cnt<< std::endl;
    if (row_num <= 0)
    {
        return;
    }

    if (column_num != result_data->columns_cnt)
    {
        std::cout << "input column num = " << result_data->columns_cnt << "return column num  " << column_num << std::endl;
        return;
    }

	row_num = ajust_columns_mem(result_data, row_num);
	if(row_num < 0)
	{
		return;
	}

	int index  = 0;	
    for (Block::Iterator bi(block); bi.IsValid(); bi.Next())
    {
		columns_desc_s *column = &result_data->columns[index];
		char *column_datas = (char*)column->column_datas + (result_data->row_cnt * column->col_len);
         switch (bi.Type()->GetCode())
        {
        case Int8:
			write_number_column_data<ColumnInt8>(block, index, sizeof(int8_t), row_num,column_datas);
            break;
        case Int16:
			write_number_column_data<ColumnInt16>(block, index, sizeof(int16_t),row_num,column_datas);
            break;
        case Int32:
			write_number_column_data<ColumnInt32>(block, index, sizeof(int32_t),row_num,column_datas);
            break;
        case Int64:
			write_number_column_data<ColumnInt64>(block, index, sizeof(int64_t),row_num,column_datas);
            break;
        case UInt8:
			write_number_column_data<ColumnUInt8>(block, index, sizeof(uint8_t),row_num,column_datas);
            break;
        case UInt16:
			write_number_column_data<ColumnUInt16>(block, index, sizeof(uint16_t),row_num,column_datas);
            break;
        case UInt32:
			write_number_column_data<ColumnUInt32>(block, index, sizeof(uint32_t),row_num,column_datas);
            break;
        case UInt64:
			write_number_column_data<ColumnUInt64>(block, index, sizeof(uint64_t),row_num,column_datas);
            break;
        case Float32:
			write_number_column_data<ColumnFloat32>(block, index, sizeof(float),row_num,column_datas);
            break;
        case Float64:
			write_number_column_data<ColumnFloat64>(block, index, sizeof(double),row_num,column_datas);
            break;
        case String:
           	for(int i = 0; i < row_num; i++)
           	{
				memcpy(column_datas, (void *)(string((*block[index]->As<ColumnString>())[i]).c_str()), column->col_len);
    			column_datas += column->col_len;
			}
            break;
        case DateTime:
			write_number_column_data<ColumnUInt32>(block, index, sizeof(uint32_t),row_num,column_datas);
            break;
        case Date:
        {
            //auto col_date = block[column_count]->As<ColumnNullable>();
            //auto std::time_t t = col_date->Nested()->As<ColumnDate>()->At(i);
            //std::cout << "Date ：" << column_count << endl;
            //std::cout << "Date ：" << (*block[column_count]->As<ColumnUInt16>()->At(i)) << endl;
            // memcpy(row_data, (void *)&(*block[column_count]->As<ColumnUInt16>()->At(i)), columns_data->columns[column_count].col_len);
            break;
        }
        case IPv4:
        {
			for(int i = 0; i < row_num; i++)
           	{
				in_addr addr = ((*block[index]->As<ColumnIPv4>()).At(i));
				memcpy(column_datas, (void *)&addr, column->col_len);
    			column_datas += column->col_len;
			}
            break;
        }
        case IPv6:
        {
			for(int i = 0; i < row_num; i++)
           	{
				in6_addr addr = ((*block[index]->As<ColumnIPv6>()).At(i));
				memcpy(column_datas, (void *)&addr, column->col_len);
    			column_datas += column->col_len;
			}
            break;
        }
        case FixedString:
        case Array:
        case Nullable:
        case Tuple:
        case Enum8:
        case Enum16:
        case UUID:
            std::cout << "Unable to parse GetCode:" << bi.Type()->GetCode() << std::endl;
            break;
        default:
            std::cout << "unkown GetCode:" << bi.Type()->GetCode() << std::endl;
            return;
        }
        index++;
    }
	result_data->row_cnt += row_num;
	result_data->ret_code = 0;

}

/*
*@function name:ck_client_execute
*@Description：执行一条sql语句
*@param query：sql语句
*@param type：查询类型：0：select 查询，非0：其他查询
*@param data：指向存放数据库返回数据内存空间
*@return: 成功 0 ，失败 -1
*/
int ck_client_execute(void *handle, char *query, int type, void *datas)
{
    if (!query || !datas || !handle)
    {
        return -1;
    }
 	Query query_(query);
	
	
    auto *client = static_cast<Client *>(handle);

    std::cout<<query<<std::endl;
    if (type == 0)
    {
		
		query_.SetOnData(select_callbak_func, (char*)datas);
    }
    else
    {
       query_.SetOnData(query_callbak_func, (char*)datas);
    }

	query_.OnException(exception_callbak_func, (char*)datas);
    try
    {
        //std::cout<<query<<std::endl;
       // client->Select(query, execute_callback, (char *)datas);
        client->Select(query_);
        return 0;
    }
    catch (...)
    {
        std::cerr << "client select error!" << std::endl;
        return -1;
    }
}

/*
*@function name:ck_client_destory
*@Description：销毁ck客户端相关信息
*@param ck_handle：客户端操作句柄
*@return: 成功 0 ，失败 -1
*/

int ck_client_destory(void *ck_handle)
{
    if (!ck_handle)
    {
        return -1;
    }

    try
    {
        auto *client = static_cast<Client *>(ck_handle);
        delete client;
        return 0;
    }
    catch (...)
    {
        std::cerr << "client destory error!" << std::endl;
        return -1;
    }
}

/*
*@function name:ck_client_resp_code
*@Description：返回数据库返回码
*@param result：返回信息的结构
*/

int ck_client_resp_code(void *result)
{
   ck_result_s *result_data = (ck_result_s*)result;
   
   return result_data->ret_code;
}


/*
*@function name:ck_client_resp_len
*@Description：返回数据库返回数据的长度
*@param result：返回信息的结构
*/

int ck_client_resp_len(void *result)
{
   ck_result_s *result_data = (ck_result_s*)result;
   
   return result_data->ret_len;
}

/*
*@function name:ck_client_resp_data
*@Description：返回数据库返回数据的长度
*@param result：返回信息的结构
*/

void *ck_client_resp_data(void *result)
{
   ck_result_s *result_data = (ck_result_s*)result;
   
   return result_data->ret_datas;
}


/*
*@function name:ck_client_free
*@Description：销毁返回相关信息
*@param result_data：返回信息的结构
*/

void ck_client_free(void *result)
{
   columns_desc_s * columns;
   ck_result_s *result_data = (ck_result_s*)result;
   if(!result_data)
   {
   		return;
   }
   for(int i = 0; i < result_data->columns_cnt; i++)
   {
   		columns = &result_data->columns[i];
		if(columns->column_datas)
		{
			free(columns->column_datas);
		}
   }

   if(result_data->ret_datas)
   {
   		free(result_data->ret_datas);
   }
}


