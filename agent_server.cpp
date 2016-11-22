//
//geyijun create @ 20150921
//
//当前是单线程版本，因为多线程版本存在安全问题
//因为当从一个conn收到数据透明传递给另一个conn的时候
//这两个conn 是属于不同的thread 的，这样其上的回调函数
//的执行语境可能在不同的线程中，会导致竞争状态参数
//例如: peer_timeout_cb 和backend_cb 就会在不同的线程中执行。

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <assert.h>
#include <string>
#include <map>
#include<unistd.h>
#include <evhtp.h>
#include <event2/event_struct.h>
#include <hiredis/hiredis.h>
#include <hiredis/async.h>
#include <hiredis/adapters/libevent.h>
#include <json/json.h>
#include <time.h>
#include <fcntl.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "redis_define.h"
#include "redis_wrap.h"
#include "subsvr_manage.h"
#include "agent_server.h"

//日志模块(没有使用最新版本，因为gcc4.7以上的才支持C++11)2
//https://github.com/easylogging/easyloggingpp/blob/master/README.md
#define ELPP_DEBUG_ASSERT_FAILURE	//配置文件读取失败终止启动
#define ELPP_STACKTRACE_ON_CRASH		//可以挪到makefile中去
#define MAX_LINE    1024	//一次读取的缓冲区消息   
#define HIGH_WATER  4096	//设置读数据的高水位

#include "easylogging++.h"
INITIALIZE_EASYLOGGINGPP	

#define VERSION "V1.0.00.1"
//配置定义
#define DEBUG_FLAG	0

static time_t	s_startup_time = 0;
typedef struct peer_info{
	struct bufferevent *bev;
	char stod[128];
	char dtos[128];
	uint32_t updatetime;
	struct event timer;
}peer_info_t;

//单向连接信息映射表
typedef std::map<std::string,peer_info_t *> peercon_map_t;
//双向连接信息映射表
typedef std::map<struct bufferevent *,struct bufferevent  *> addresscon_map_t;
//设置连接的过期时间
static int PEER_SESSION_TIMEOUT = 180;

//对象管理表
static peercon_map_t 	s_peer_map;
static pthread_mutex_t	s_lock_peer_map;		//互斥保护用的

static addresscon_map_t s_address_map;
static pthread_mutex_t	s_lock_address_map;		//互斥保护用的

static char data_center_ip[48] = {0,};		//用来存放数据中心IP的

static char   *  s_server_ip    = NULL;	
static uint16_t s_server_port    = 6604;	

static evbase_t   * s_evbase  = NULL;
static event 	  *s_event = NULL;

//获取接收缓冲区的大小
int getrecv_buffer(int fd)
{
	//获取发送缓冲区的大小
	int rcvbuf_len;
    int len = sizeof(rcvbuf_len);
    if( getsockopt(fd, SOL_SOCKET, SO_RCVBUF, (void *)&rcvbuf_len, (socklen_t *)&len ) < 0 ){
        perror("getsockopt: ");
        return -1;
    }
	return rcvbuf_len;
}
//获取发送缓冲区的大小
int getsend_buffer(int fd)
{
	//获取发送缓冲区的大小
	int sendbuf_len;
    int len = sizeof(sendbuf_len);
    if( getsockopt(fd, SOL_SOCKET, SO_SNDBUF, (void *)&sendbuf_len, (socklen_t *)&len ) < 0 ){
        perror("getsockopt: ");
        return -1;
    }
	return sendbuf_len;
}
//设置发送缓冲区的大小
int setsend_buffer(int fd,int length)
{
	int len = sizeof(length);
	if( setsockopt( fd, SOL_SOCKET, SO_SNDBUF, (void *)&length, len ) < 0 ){
		perror("setsend_buffer getsockopt: ");
		return -1;
	}
	return 0;
}
//设置接收缓冲区的大小
int setrecv_buffer(int fd,int length)
{
	int len = sizeof(length);
	if( setsockopt( fd, SOL_SOCKET, SO_RCVBUF, (void *)&length, len ) < 0 ){
		perror("setrecv_buffer getsockopt: ");
		return -1;
	}
	return 0;
}
//获取服务器的状态
int get_server_Status()
{
	int length = 0;
	pthread_mutex_lock(&s_lock_peer_map);
	length = s_peer_map.size();
	pthread_mutex_unlock(&s_lock_peer_map);
	return length;
}

int set_socket_reuse(int sockfd)
{	
	int one = 1;
	if(sockfd < 0){
		LOG(ERROR)<<"sockfd is unavailable" <<sockfd;
		return -1;
	}
	return setsockopt(sockfd,SOL_SOCKET,SO_REUSEADDR,(void *)&one,sizeof(one));
}

//设置fd非阻塞
int set_socket_nonbloc(int sockfd)
{
	int flag = -1;
	if(sockfd < 0){
		LOG(ERROR)<<"set_socket_nonbloc sockfd is unavailable" <<sockfd;
		return -1;
	}
	if((flag = fcntl(sockfd,F_GETFL,NULL)) < 0){
		return -1;
	}
	if(fcntl(sockfd,F_SETFL,flag|O_NONBLOCK) < 0){
		return -1;
	}
	return 0;
}

static peer_info_t * get_peer_obj(const char* session)
{
	peer_info_t * peerinfo = NULL;
	if(NULL == session)
	{
		return NULL;
	}
	pthread_mutex_lock(&s_lock_peer_map);
	peercon_map_t::iterator it = s_peer_map.find(session);
	if(it != s_peer_map.end())
	{
		peerinfo = (*it).second;
	}
	pthread_mutex_unlock(&s_lock_peer_map);
	
	return peerinfo;
}

static struct bufferevent * get_address_obj(struct bufferevent * bev)
{
	if(NULL == bev)
	{
		return NULL;
	}
	struct bufferevent * desdev = NULL;
	pthread_mutex_lock(&s_lock_address_map);
	addresscon_map_t::iterator it = s_address_map.find(bev);
	if(it != s_address_map.end())
	{
		desdev = (*it).second;
	}
	pthread_mutex_unlock(&s_lock_address_map);
	return desdev;
}

static int	insert_peer_obj(const char* srctodes, peer_info_t *peerinfo)
{
	if((srctodes == NULL)&&(peerinfo == NULL))
	{
		return -1;
	}
	
	//LOG(DEBUG)<<"to insert the new peer" << srctodes;
	pthread_mutex_lock(&s_lock_peer_map);
	s_peer_map.insert(peercon_map_t::value_type(srctodes,peerinfo));
	pthread_mutex_unlock(&s_lock_peer_map);
	return 0;
}

static int	insert_address_obj(struct bufferevent * srcbev,struct bufferevent *desbev)
{	
	if((srcbev == NULL)&&(desbev == NULL))
	{
		return -1;
	}
	//LOG(DEBUG)<<"insert_address_obj to insert the new peer";
	pthread_mutex_lock(&s_lock_address_map);
	s_address_map.insert(addresscon_map_t::value_type(srcbev,desbev));
	pthread_mutex_unlock(&s_lock_address_map);
	return 0;
}

static int	erase_peer_obg(const char *srctodes)
{
	//LOG(DEBUG)<<"erase_peer_obg "<<srctodes;
	pthread_mutex_lock(&s_lock_peer_map);
	s_peer_map.erase(srctodes);
	pthread_mutex_unlock(&s_lock_peer_map);
	return 0;
}

static int erase_address_obj(struct bufferevent *srcbev)
{
	if(NULL == srcbev)
	{
		return -1;
	}
	pthread_mutex_lock(&s_lock_address_map);
	s_address_map.erase(srcbev);
	pthread_mutex_unlock(&s_lock_address_map);
	return 0;
}

static int free_all_con(peer_info_t *peerinfo)
{
	struct bufferevent *desbev = NULL;
	struct bufferevent *srcbev = peerinfo->bev;
	LOG(DEBUG)<<"to free all 111111"<<peerinfo->stod;
	//关闭源端资源
	erase_peer_obg(peerinfo->stod);
	erase_address_obj(srcbev);
	bufferevent_free(srcbev);
	if(event_initialized(&peerinfo->timer))		
		event_del(&peerinfo->timer);	
	//关闭对端资源
	peer_info_t * destinfo = get_peer_obj(peerinfo->dtos);
	if(destinfo != NULL)
	{
		LOG(DEBUG)<<"to free all 222222"<<peerinfo->dtos;
		desbev = destinfo->bev;
		erase_peer_obg(destinfo->stod);
		erase_address_obj(desbev);
		bufferevent_free(desbev);
		if(event_initialized(&destinfo->timer))		
			event_del(&destinfo->timer);
		free(destinfo);
		destinfo = NULL;
	}
	free(peerinfo);
	peerinfo = NULL;
	return 0;
}

static void peer_timeout_cb(evutil_socket_t fd, short event, void *arg)
{
	peer_info_t * peerinfo = (peer_info_t *)arg;
	if(NULL == peerinfo)
	{
		return;
	}
	free_all_con(peerinfo);
	return;
}

/*  
	agent_write_cb:
	如果发送缓冲区域为空但是接收缓冲区还有数据agent_write_cb就被回调把数
	据发出去而不必等待下一次触发agent_read_cb 用来减小延时提高效率
*/
void agent_write_cb(struct bufferevent *bev, void *arg) 
{
	peer_info_t *peerobj = (peer_info_t *)arg;
	//判断对话是否已建立如果会话已建立就转发数据
	size_t input_len =  evbuffer_get_length(bev->input);
	struct bufferevent *desbev = get_address_obj(bev);
	if((desbev != NULL)&&(input_len > 0))
	{
		//透传数据
		bufferevent_write_buffer(desbev, bev->input);
	}
	return;
}

void agent_read_cb(struct bufferevent *bev, void *arg) 
{	
	//节点对象
	peer_info_t *peerobj = (peer_info_t *)arg;
	//判断对话是否已建立如果会话已建立就转发数据
	struct bufferevent *desbev = get_address_obj(bev);
	if(desbev != NULL)
	{
		//透传数据
		//	evutil_socket_t fd = bufferevent_getfd(desbev);
		//	evbuffer_write(bev->input,fd);
		bufferevent_write_buffer(desbev, bev->input);
	}
	else
	{
		//会话未建立，检查是否是连接请求
		if(evbuffer_find(bev->input,(u_char*)"XXEE",4) != NULL)
		{
			int n,len;
			char content[MAX_LINE+1] = {0,};
			while (n = bufferevent_read(bev, content, MAX_LINE),n > 0)
			{ 
				LOG(DEBUG)<<"content "<<content<<" n = "<<n;
				content[n] = '\0'; 
				len = n;
			}
			content[len-4] = '\0';
			Json::Reader 	reader;
			Json::Value 	requestValue;
			if(reader.parse(content, requestValue) == false)
			{
				char temp[64] = "{\"ErrorNum\": \"400\"}XXEE";    
				int length = strlen(temp); 
				bufferevent_write(bev,temp,length);
				return;
			}
			//检查连接包的有效性
			if((requestValue.isObject())&&(requestValue.isMember("AuthCode")) &&(requestValue.isMember("SrcUuid")) \
				&&(requestValue.isMember("DestUuid"))&&(requestValue.isMember("SessionId")))
			{
				std::string SrcUuid = requestValue["SrcUuid"].asCString();
				std::string DestUuid = requestValue["DestUuid"].asCString();
				std::string SessionId = requestValue["SessionId"].asCString();
				std::string SrcToDes = SrcUuid + "_" + DestUuid + "_" + SessionId;
				std::string DesToSrc =  DestUuid + "_" + SrcUuid + "_" +SessionId;
				//检查连接是否存在
				peer_info_t *mappeer = get_peer_obj(SrcToDes.c_str());
				if(NULL == mappeer) //如果不存在说明是一条新连接
				{
					strncpy(peerobj->stod,SrcToDes.c_str(),sizeof(peerobj->stod));
					strncpy(peerobj->dtos,DesToSrc.c_str(),sizeof(peerobj->dtos));
					mappeer = peerobj;
					insert_peer_obj(SrcToDes.c_str(),mappeer);
					LOG(INFO)<<"new connect peer "<<SrcUuid.c_str();
				}
				else if(mappeer->bev != peerobj->bev) //同一个设备同一个会话id,有两条连接，则关闭新连接
				{
					LOG(INFO)<<"different connect use the same id"<<SrcToDes.c_str();
					//断开新连接
					bufferevent_free(peerobj->bev);
					//删除新连接的超时定时器
					if(event_initialized(&peerobj->timer))		
						event_del(&peerobj->timer);
					//释放新连接的内存
					free(peerobj);
					peerobj = NULL;
					return ;
				}
				//查看对方会话是否建立
				peer_info_t *destpeer = get_peer_obj(DesToSrc.c_str());
				if(destpeer != NULL)
				{
					insert_address_obj(mappeer->bev,destpeer->bev);
					insert_address_obj(destpeer->bev,mappeer->bev);
					char temp[64] = "{\"ErrorNum\": \"200\"}XXEE";    
					int length = strlen(temp);  
					bufferevent_write(mappeer->bev,temp,length);	//响应
					bufferevent_write(destpeer->bev,temp,length);	//通知对端已连接上线
					LOG(INFO)<<"create connect session sucess src: "<<SrcUuid.c_str()<<" dest: "<<DestUuid.c_str()<<" sessionid: "<<SessionId.c_str();
				}
				else
				{
					LOG(INFO)<<"sernumber: "<<SrcUuid.c_str()<<"online"<<"  destnumber: "<<DestUuid.c_str()<<" not one line";
				}
			}
			else
			{
				const char *temp = "bad request";    
				int length = strlen(temp);  
				bufferevent_write(bev,temp,length);
				return;
			}
		}
	}

	//更新源端超时定时器
	struct timeval newtime;
    evutil_gettimeofday(&newtime, NULL);
	if(newtime.tv_sec - peerobj->updatetime > 10)
	{
		if(event_initialized(&peerobj->timer))		
			event_del(&peerobj->timer);	
		event_assign(&peerobj->timer,s_evbase, -1, 0, peer_timeout_cb, (void*)peerobj);	
		struct timeval tv;	
		evutil_timerclear(&tv);	
		tv.tv_sec = PEER_SESSION_TIMEOUT;
		event_add(&peerobj->timer, &tv);
		peerobj->updatetime = newtime.tv_sec;
	}
	//更新对端超时定时器
	peer_info_t *despeer = get_peer_obj(peerobj->dtos);
	if(despeer != NULL)
	{
		if(newtime.tv_sec - despeer->updatetime > 10)
		{
			//更新对端的超时定时器
			if(event_initialized(&despeer->timer))		
				event_del(&despeer->timer);	
			event_assign(&despeer->timer,s_evbase, -1, 0, peer_timeout_cb, (void*)despeer);	
			struct timeval tv;	
			evutil_timerclear(&tv);	
			tv.tv_sec = PEER_SESSION_TIMEOUT;
			event_add(&despeer->timer, &tv);
			despeer->updatetime = newtime.tv_sec;
		}
	}
	else
	{
		LOG(INFO)<<"dest peer not online "<<peerobj->dtos;
	}
}

//连接出错回调
void agent_error_cb(struct bufferevent *bev, short event, void *arg) 
{    
	peer_info_t * peerinfo = (peer_info_t *)arg;
	if( NULL == peerinfo)
	{
		LOG(ERROR)<<"connect error peer is null";
		return ;
	}   
	if (event & BEV_EVENT_TIMEOUT) {        
		LOG(ERROR)<<"Timed out "<<peerinfo->stod; //if bufferevent_set_timeouts() called    
	}
	else if (event & BEV_EVENT_EOF){        
		LOG(ERROR)<<"connection closed "<<peerinfo->stod;   
	}    
	else if (event & BEV_EVENT_ERROR){        
		LOG(ERROR)<<"some other error "<<peerinfo->stod;    
	}    
	free_all_con(peerinfo);
}

//接受客户端的连接请求回调
static void agent_accept_cb(int sockfd, short event_type,void *arg)
{
	int fd;	
	struct sockaddr_in sin;	
	socklen_t slen;	
	slen = sizeof(sin);
	LOG(DEBUG)<<"####accept listen socket fd = "<<sockfd;
	fd = accept(sockfd, (struct sockaddr *)&sin, &slen);	
	if (fd < 0){
		printf("ERROR: accept: ");	
		return;
	}
	set_socket_nonbloc(fd);
	LOG(DEBUG)<<"####accept fd = "<<fd;
	//创建一个节点信息
	peer_info_t *peerobj = (peer_info_t *)calloc(sizeof(peer_info_t),1);
	assert(peerobj);
	
	//设置超时定时器
	if(event_initialized(&peerobj->timer))		
		event_del(&peerobj->timer);	
	event_assign(&peerobj->timer,s_evbase, -1, 0, peer_timeout_cb, (void*)peerobj);	
	struct timeval tv;	
	evutil_timerclear(&tv);	
	tv.tv_sec = PEER_SESSION_TIMEOUT;
	event_add(&peerobj->timer, &tv); 

	//创建一个bufferevent事件，绑定s_evbase	
	struct bufferevent *bev = bufferevent_socket_new(s_evbase,fd,BEV_OPT_CLOSE_ON_FREE); 
	peerobj->bev = bev;
	bufferevent_setwatermark(bev,EV_READ,0,HIGH_WATER);			//设置读的高水位为HIGH_WATER
	bufferevent_setcb(bev,agent_read_cb, agent_write_cb, agent_error_cb,peerobj); //设置回调函数	
	bufferevent_enable(bev, EV_READ|EV_WRITE|EV_PERSIST); //开启base
}

static const char * optstr = "hi:s:a:p:f:c:d:v:t:e:";
static const char * help   =
	"Options: \n"
	"  -h         : This help text\n"
	"  -i 	<str> : Local Http Server IP \n"
	"  -s 	<int> : Local Http Server Port \n"
	"  -a 	<str> : Status Redis IP\n"
	"  -p 	<int> : Status Redis Port\n"
	"  -c 	<str> : Authcode Redis IP\n"
	"  -d 	<int> : Authcode Redis Port\n"
	"  -v 	<int> : Peer Keepalive Interval\n"
	"  -t 	<int> : Peer Keepalive Timeout\n"
	"  -e	<int> : Redis ExpireTime\n";
 
static int parse_args(int argc, char ** argv) 
{
	extern char * optarg;
	int           c;
	while ((c = getopt(argc, argv, optstr)) != -1) {
	    switch (c) {
	        case 'h':
	            printf("Usage: %s [opts]\n%s", argv[0], help);
	            return -1;
	        case 'i':
	            s_server_ip = strdup(optarg);
	            break;
	        case 's':
	            s_server_port = atoi(optarg);
	            break;
			case 't':
	            PEER_SESSION_TIMEOUT = atoi(optarg);
	            break;
	        default:
	            printf("Unknown opt %s\n", optarg);
	            return -1;
	    }
	}
	return 0;
} 

void rolloutHandler(const char* filename, std::size_t size) 
{
	static unsigned int idx;
	// SHOULD NOT LOG ANYTHING HERE BECAUSE LOG FILE IS CLOSED!
	std::cout << "************** Rolling out [" << filename << "] because it reached [" << size << " bytes]" << std::endl;
	// BACK IT UP
	std::stringstream ss;
	ss << "mv " << filename << " ./logs-backup/log-backup-" << ++idx;
	system(ss.str().c_str());
}

int main(int argc, char ** argv) 
{
	//日志模块配置 ERROR|WARNING|INFO|DEBUG
	el::Loggers::addFlag(el::LoggingFlag::StrictLogFileSizeCheck);
	el::Configurations conf("logger.conf");
    el::Loggers::reconfigureAllLoggers(conf);
    el::Helpers::installPreRollOutCallback(rolloutHandler);

	system("mkdir -p ./logs-backup/");
	system("mkdir -p ./logs/");
	s_startup_time = ::time(NULL);
	
	//忽略一些信号
	signal(SIGPIPE,SIG_IGN);  
	
	if (parse_args(argc, argv) < 0) 
	{
		LOG(ERROR) << "parse_args:failed";
	    exit(1);
	}

	LOG(INFO) << "process has been started current version is "<< VERSION;
	LOG(INFO) << "server_ip=["<<s_server_ip<<"],server_port=["<<s_server_port<<"]";

	//获取数据中心地址
	get_param(data_center_ip);
	start_subsvr_manage(s_server_ip,data_center_ip);
	
	//清零全局map表
	s_address_map.clear();	//地址表
	s_peer_map.clear();		//节点表
	pthread_mutex_init(&s_lock_address_map,NULL);	
	pthread_mutex_init(&s_lock_peer_map,NULL);	
	int listen_fd = socket(AF_INET,SOCK_STREAM,0);
	if(listen_fd < 0){
		LOG(ERROR) << "create listen fd failed";
		exit(1);
	}
	
	set_socket_reuse(listen_fd);
	set_socket_nonbloc(listen_fd);
	struct sockaddr_in address;
	address.sin_family = AF_INET;
	address.sin_port = htons(6611);
	address.sin_addr.s_addr = inet_addr("0.0.0.0");
	if(bind(listen_fd,(const struct sockaddr *)&address,sizeof(address)) == -1){
		LOG(ERROR)<<"bind failed";
		return -1;
	}
	
	if(listen(listen_fd,8192) == -1)
	{
		LOG(ERROR)<<"listen failed";
		return -1;
	}
	
	setrecv_buffer(listen_fd,20000);
	int send_len = getsend_buffer(listen_fd);
	if(send_len < 50000*2)
		setsend_buffer(listen_fd,50000);
	
	LOG(INFO)<<"listening ......";
	s_evbase  = event_base_new();
	s_event = event_new(s_evbase,listen_fd,EV_READ|EV_PERSIST,agent_accept_cb,NULL);
	event_add(s_event,NULL);
	event_base_dispatch(s_evbase);
	LOG(INFO) <<"Clean exit";
	event_base_free(s_evbase);
	
	//日志模块清理
	el::Helpers::uninstallPreRollOutCallback();
	return 0;		
}	


