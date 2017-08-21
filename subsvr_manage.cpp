#include <string.h>
#include <strings.h>
#include <pthread.h>
#include <string>
#include <map>
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include "match.h"
#include "subsvr_manage.h"
#include "agent_server.h"

static uint32_t COMMON_MINUTESECONDES = 360;

static int s_redis_update_count = 0;		
static int s_redis_exit = 0;
static int dss_server_port = 6611;
static int pull_thread_startflag = 0;
//通过环境变量获取配置
//和环境变量相关
static char rediscenter_ip[48] = {0,};
static char rediscenter_iplist[142] = {0,};
static const char*  serverarea_name = NULL;
static const char*  vendor_name = NULL;
static char s_server_type[64] = {0,};

//设置三个service type 以避免同一台服务器上有多个服务类型
static char server_type1[16] = {0,};
static char server_type2[16] = {0,};
static char server_type3[16] = {0,};

time_t begin_time = 0;

static char  dssaccess_server_ip[48] = {0,};

static int  rediscenter_port = 5141;

static void update_serverinfo_to_redis(redisContext* connect,std::string server_type)
{
	if (connect) 
	{
		if (0 == redis_multi(connect)) 
		{	
			std::string dssaccess_server_key(
				server_type + "_" + std::string(dssaccess_server_ip));
			std::string key_map(server_type + "Map");
			std::string dssaccess_server_port;
			std::string dssaccess_server_status;
			char tps_run_time[48];
			
			time_t cur_time = ::time(NULL); 
			time_t up_time = cur_time - begin_time;
			sprintf(tps_run_time,"%d",(uint32_t)up_time);
			
			//获取当前服务连接数
			int peer_map_size = get_server_Status();
			
			std::stringstream ss;
			ss << dss_server_port;
			ss >> dssaccess_server_port;
			ss.clear();
			
			ss << peer_map_size;
			ss >> dssaccess_server_status;

			int ret = redis_hset(connect,key_map.c_str(),dssaccess_server_ip, VALUE_DSSACCESSSERVER_ONLINE);
			
			ret += redis_hset(connect,dssaccess_server_key.c_str(),
							FIELD_DSSACCESSSERVER_IP, dssaccess_server_ip);
			ret += redis_hset(connect,dssaccess_server_key.c_str(),
							FIELD_DSSACCESSSERVER_PORT, dssaccess_server_port.c_str());
			ret += redis_hset(connect,dssaccess_server_key.c_str(),
							FIELD_DSSACCESSSERVER_AREA, serverarea_name);
			ret += redis_hset(connect,dssaccess_server_key.c_str(),
							FIELD_DSSACCESSSERVER_VENDORNAME, vendor_name);
			ret += redis_hset(connect,dssaccess_server_key.c_str(),
							FIELD_DSSACCESSSERVER_STATUS,dssaccess_server_status.c_str());
			ret += redis_hset(connect,dssaccess_server_key.c_str(),
							FIELD_DSSACCESSSERVER_ACTIVEINDEX, dssaccess_server_status.c_str());
							
			ret += redis_hset(connect,dssaccess_server_key.c_str(),
							FIELD_DSSACCESSSERVER_RETOK, "0");
			ret += redis_hset(connect,dssaccess_server_key.c_str(),
							FIELD_DSSACCESSSERVER_RETERROR,"0");
			ret += redis_hset(connect,dssaccess_server_key.c_str(),
							FIELD_DSSACCESSSERVER_RUNSECONDS, tps_run_time);
			
			ret += redis_expire(connect,dssaccess_server_key.c_str(), COMMON_MINUTESECONDES);
			if (ret < 0) 
			{
				redis_discard(connect);
			} 
			else 
			{
				redis_exec(connect);
			}
		}
	}
}

static int get_env_param()
{
	const char* temp_server_ip = getenv("RedisCenter");
	if(temp_server_ip != NULL)
	{
		strcpy(rediscenter_ip,temp_server_ip);
	}
	serverarea_name = getenv("ServerArea");
	if(NULL == serverarea_name)
	{
		serverarea_name = "Asia:China:Beijing";
	}
	vendor_name = getenv("VendorName");
	if(NULL == vendor_name)
	{
		vendor_name = "General";
	}
	const char* temp_type = getenv("ServerType");
	if(NULL == temp_type)
	{
		strcpy(s_server_type,"RpsCmd:RpsVoIP:RpsAV");
	}
	else
	{
		strcpy(s_server_type,temp_type);
	}
	
	//分离出单个的服务类型	
	char *temp1 = strstr(s_server_type,":");
	if(temp1 != NULL)
	{
		char *temp = NULL;
		*temp1 = '\0';
		strcpy(server_type1,s_server_type);
		temp = temp1+1;
		char *temp2 = strstr(temp,":");
		if(temp2 == NULL)
		{
			strcpy(server_type2,temp);
		}
		else
		{
			*temp2 = '\0';
			strcpy(server_type2,temp);
			temp = temp2+1;
			strcpy(server_type3,temp);
		}
	}
	else
	{
		strcpy(server_type1,s_server_type);
	}	
	printf("type1 = %s,type2 = %s,type3 = %s\n",server_type1,server_type2,server_type3);

	//从环境变量里面获取iplist
	const char *tmp_server_iplist = getenv("RedisList");
	if(tmp_server_iplist != NULL)
	{
		strcpy(rediscenter_iplist,tmp_server_iplist);
	}
	else
	{
		strcpy(rediscenter_iplist,rediscenter_ip);
	}
	printf("@@@@@@@@@@rediscenter iplist = %s\n",rediscenter_iplist);
	return 0;
}

static void* redis_update_thread(void* arg)
{
	while(!s_redis_exit)
	{	
		redisContext* connect = redis_connect(rediscenter_ip, rediscenter_port);
		if (connect) 
		{
			//按照不同的服务类型写数据库
			if(strlen(server_type1) != 0)
			{
				std::string server_type = std::string(server_type1);
				update_serverinfo_to_redis(connect,server_type);
			}
			if(strlen(server_type2) != 0)
			{
				std::string server_type = std::string(server_type2);
				update_serverinfo_to_redis(connect,server_type);
			}
			if(strlen(server_type3) != 0)
			{
				std::string server_type = std::string(server_type3);
				update_serverinfo_to_redis(connect,server_type);
			}
			
			redisFree(connect);
			s_redis_update_count++;
			sleep(COMMON_MINUTESECONDES/2);	
		}
		else
		{
			if(s_redis_update_count==0)
				sleep(1);
			else
				sleep(COMMON_MINUTESECONDES);
		}
	}
	return NULL;
}

static void* pull_serverinfo_thread(void* arg)
{
	//定期拉取服务信息
	int ret = 0;
	pull_thread_startflag = 1;
	std::string rps_type;
	rps_type.clear();
	
	if(strlen(server_type1) != 0)
	{
		std::string server_type = std::string(server_type1);
		rps_type = server_type;
	}
	
	if(strlen(server_type2) != 0)
	{
		std::string server_type = std::string(server_type2);
		rps_type = rps_type + "_" + server_type;
	}
	
	if(strlen(server_type3) != 0)
	{
		std::string server_type = std::string(server_type3);
		rps_type = rps_type + "_" + server_type;
	}
	
	printf("rps type:%s\n",rps_type.c_str());
	while(1)
	{
		ret = refresh_server_info(rediscenter_iplist,(char *)rps_type.c_str());
		if(ret < 0)
		{
			sleep(90);
		}
		else
		{
			sleep(180);	
		}	
	}
	return NULL;
}

int	start_subsvr_manage(const char *m_server_ip,const char *m_cfgserver_ip)
{	
	begin_time = ::time(NULL);
	if(NULL != m_server_ip)
	{
		strcpy(dssaccess_server_ip,m_server_ip);
	}
	else
	{
		return -1;
	}

	if(NULL != m_cfgserver_ip)
	{
		strcpy(rediscenter_ip,m_cfgserver_ip);
	}
	else
	{
		return -1;
	}
	
	pthread_t tid[2];
	pthread_attr_t attr[2];
	pthread_attr_init(&attr[0]);
	pthread_attr_init(&attr[1]);
	
	//创建线程，定期更新数据库中dssaccess的状态信息
	pthread_create(&tid[0], &attr[0], redis_update_thread, NULL);
	
	//创建线程，定期拉取服务信息
	pthread_create(&tid[1], &attr[1], pull_serverinfo_thread, NULL);

	
	int cost = 0;
	while (((s_redis_update_count <= 0)||(pull_thread_startflag<=0))&&(cost < 20))
	{
		printf("wait for redis_update\n");
		sleep(1);	
		cost++;	
	}
	if(cost >= 20)
	{
		printf("wait for redis_update timeout\n");
		return -1;
	}
	
	return 0;
}

int	stop_subsvr_manage()
{
	s_redis_exit = 1;
	sleep(1);	
	return 0;
}

int get_param(char *cfgredisip)
{
	//获取环境变量
	get_env_param();
	if((NULL == cfgredisip))
	{
		return -1;
	}
	if(strlen(rediscenter_ip) > 0)
	{
		strcpy(cfgredisip,rediscenter_ip);
	}
	else
	{
		return -1;
	}
	return 0;
}
