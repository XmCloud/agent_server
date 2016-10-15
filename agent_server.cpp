//
//geyijun create @ 20150921
//
//µ±Ç°ÊÇµ¥Ïß³Ì°æ±¾£¬ÒòÎª¶àÏß³Ì°æ±¾´æÔÚ°²È«ÎÊÌâ
//ÒòÎªµ±´ÓÒ»¸öconnÊÕµ½Êı¾İÍ¸Ã÷´«µİ¸øÁíÒ»¸öconnµÄÊ±ºò
//ÕâÁ½¸öconn ÊÇÊôÓÚ²»Í¬µÄthread µÄ£¬ÕâÑùÆäÉÏµÄ»Øµ÷º¯Êı
//µÄÖ´ĞĞÓï¾³¿ÉÄÜÔÚ²»Í¬µÄÏß³ÌÖĞ£¬»áµ¼ÖÂ¾ºÕù×´Ì¬²ÎÊı
//ÀıÈç: peer_timeout_cb ºÍbackend_cb ¾Í»áÔÚ²»Í¬µÄÏß³ÌÖĞÖ´ĞĞ¡£

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

//ÈÕÖ¾Ä£¿é(Ã»ÓĞÊ¹ÓÃ×îĞÂ°æ±¾£¬ÒòÎªgcc4.7ÒÔÉÏµÄ²ÅÖ§³ÖC++11)2
//https://github.com/easylogging/easyloggingpp/blob/master/README.md
#define ELPP_DEBUG_ASSERT_FAILURE	//ÅäÖÃÎÄ¼ş¶ÁÈ¡Ê§°ÜÖÕÖ¹Æô¶¯
#define ELPP_STACKTRACE_ON_CRASH		//¿ÉÒÔÅ²µ½makefileÖĞÈ¥
#define MAX_LINE    8192	//Ò»´Î¶ÁÈ¡µÄ»º³åÇøÏûÏ¢   

#include "easylogging++.h"
INITIALIZE_EASYLOGGINGPP	

#define VERSION "V1.0.00.1"
//ÅäÖÃ¶¨Òå
#define DEBUG_FLAG	0

static time_t	s_startup_time = 0;
typedef struct peer_info{
	struct bufferevent *bev;
	char stod[128];
	char dtos[128];
	struct event timer;
}peer_info_t;
//µ¥ÏòÁ¬½ÓĞÅÏ¢Ó³Éä±í
typedef std::map<std::string,peer_info_t *> peercon_map_t;
//Ë«ÏòÁ¬½ÓĞÅÏ¢Ó³Éä±í
typedef std::map<struct bufferevent *,struct bufferevent  *> addresscon_map_t;
//ÉèÖÃÁ¬½ÓµÄ¹ıÆÚÊ±¼ä
static int PEER_SESSION_TIMEOUT = 180;

//¶ÔÏó¹ÜÀí±í
static peercon_map_t 	s_peer_map;
static pthread_mutex_t	s_lock_peer_map;		//»¥³â±£»¤ÓÃµÄ

static addresscon_map_t s_address_map;
static pthread_mutex_t	s_lock_address_map;		//»¥³â±£»¤ÓÃµÄ


static char   *  s_server_ip    = NULL;	
static uint16_t s_server_port    = 6604;	

static evbase_t   * s_evbase  = NULL;
static event 	  *s_event = NULL;

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
	
	LOG(DEBUG)<<"to insert the new peer" << srctodes;
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
	LOG(DEBUG)<<"insert_address_obj to insert the new peer";
	pthread_mutex_lock(&s_lock_address_map);
	s_address_map.insert(addresscon_map_t::value_type(srcbev,desbev));
	pthread_mutex_unlock(&s_lock_address_map);
	return 0;
}

static int	erase_peer_obg(const char *srctodes)
{
	LOG(DEBUG)<<"erase_peer_obg "<<srctodes;
	pthread_mutex_lock(&s_lock_peer_map);
	s_peer_map.erase(srctodes);
	pthread_mutex_unlock(&s_lock_peer_map);
	return 0;
}

static int erase_address_obj(struct bufferevent *srcbev)
{
	LOG(DEBUG)<<"erase address";
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
	//¹Ø±ÕÔ´¶Ë×ÊÔ´
	erase_peer_obg(peerinfo->stod);
	erase_address_obj(srcbev);
	bufferevent_free(srcbev);
	if(event_initialized(&peerinfo->timer))		
		event_del(&peerinfo->timer);	
	//¹Ø±Õ¶Ô¶Ë×ÊÔ´
	peer_info_t * destinfo = get_peer_obj(peerinfo->dtos);
	if(destinfo != NULL)
	{
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


void agent_read_cb(struct bufferevent *bev, void *arg) 
{	
	//½Úµã¶ÔÏó
	peer_info_t *peerobj = (peer_info_t *)arg;
	//×°ÔØÏûÏ¢ÄÚÈİ
	char content[MAX_LINE+1] = {0,};    
	int n,len;    
	//¶ÁÈ¡ÏûÏ¢
	while (n = bufferevent_read(bev, content, MAX_LINE),n > 0)
	{ 
		LOG(DEBUG)<<"content "<<content<<"n ="<<n;
		content[n] = '\0'; 
		len = n;
	}
	//¼ì²éÊÇ·ñÊÇÁ¬½ÓÇëÇó
	if(strcmp(content+len-4,"XXEE") == 0)
	{
		content[len-4] = '\0';
		Json::Reader 	reader;
		Json::Value 	requestValue;
		if(reader.parse(content, requestValue) == false)
		{
			LOG(INFO)<<"#########request is bad,to return";
			char temp[64] = "{\"ErrorNum\": \"400\"}XXEE";    
			int length = strlen(temp); 
			bufferevent_write(bev,temp,length);
			return;
		}
		//¼ì²éÁ¬½Ó°üµÄÓĞĞ§ĞÔ
		if((requestValue.isObject())&&(requestValue.isMember("AuthCode")) &&(requestValue.isMember("SrcUuid")) \
			&&(requestValue.isMember("DestUuid"))&&(requestValue.isMember("SessionId")))
		{
			std::string SrcUuid = requestValue["SrcUuid"].asCString();
			std::string DestUuid = requestValue["DestUuid"].asCString();
			std::string SessionId = requestValue["SessionId"].asCString();
			std::string SrcToDes = SrcUuid + "_" + DestUuid + "_" + SessionId;
			std::string DesToSrc =  DestUuid + "_" + SrcUuid + "_" +SessionId;
			//¼ì²éÁ¬½ÓÊÇ·ñ´æÔÚ
			peer_info_t *mappeer = get_peer_obj(SrcToDes.c_str());
			if(NULL == mappeer) //Èç¹û²»´æÔÚËµÃ÷ÊÇÒ»ÌõĞÂÁ¬½Ó
			{
				LOG(INFO)<<"new connect peer "<<peerobj->stod;
				strncpy(peerobj->stod,SrcToDes.c_str(),sizeof(peerobj->stod));
				strncpy(peerobj->dtos,DesToSrc.c_str(),sizeof(peerobj->dtos));
				mappeer = peerobj;
				insert_peer_obj(SrcToDes.c_str(),mappeer);
			}
			else if(mappeer->bev != peerobj->bev) //Í¬Ò»¸öÉè±¸Í¬Ò»¸ö»á»°id,ÓĞÁ½ÌõÁ¬½Ó£¬Ôò¹Ø±ÕĞÂÁ¬½Ó
			{
				LOG(INFO)<<"different connect use the same id"<<SrcToDes.c_str();
				//¶Ï¿ªĞÂÁ¬½Ó
				bufferevent_free(peerobj->bev);
				//É¾³ıĞÂÁ¬½ÓµÄ³¬Ê±¶¨Ê±Æ÷
				if(event_initialized(&peerobj->timer))		
					event_del(&peerobj->timer);
				//ÊÍ·ÅĞÂÁ¬½ÓµÄÄÚ´æ
				free(peerobj);
				peerobj = NULL;
				return ;
			}
			/*
			else if(mappeer->bev != peerobj->bev) //Í¬Ò»¸öÉè±¸Í¬Ò»¸ö»á»°id,ÓĞÁ½ÌõÁ¬½Ó£¬ÔòÉ¾³
			{
				LOG(INFO)<<"different connect use the same id"<<SrcToDes.c_str();
				//¶Ï¿ªÀÏÁ¬½Ó
				bufferevent_free(mappeer->bev);
				//É¾³ıÀÏÁ¬½ÓµÄ¶¨Ê±Æ÷
				if(event_initialized(&mappeer->timer))		
					event_del(&mappeer->timer);
				//ĞŞ¸Ä»á»°ÔÚpeer mapÖĞ¶ÔÓ¦µÄ¶ÔÏó
				s_peer_map[SrcToDes] = peerobj;
				
				//É¾³ıÀÏÁ¬½ÓÔÚµØÖ·±íÖĞµÄÓ³Éä¹ØÏµ
				erase_address_obj(mappeer->bev);
				peer_info_t *destpeer = get_peer_obj(DesToSrc.c_str()); //¼ì²é¶Ô¶ËÁ¬½ÓÊÇ·ñ´æÔÚ
				if(destpeer != NULL)		//Èç¹û´æÔÚĞŞ¸ÄÕû¸ö»á»°µÄÓ³Éä
				{
					erase_address_obj(mappeer->bev);		//É¾³ısrc_dest µÄÓ³Éä¹ØÏµ
					s_address_map[destpeer->bev] = bev;				//ĞŞ¸Ädest_src µÄÓ³Éä¹ØÏµ
					insert_address_obj(bev,destpeer->bev);	//Ìí¼ÓĞÂµÄÓ³Éä¹ØÏµ
				}
				//ÊÍ·ÅµôÀÏÁ¬½ÓµÄ½Úµã
				LOG(INFO)<<"to free the old one"<<SrcToDes.c_str();
				free(mappeer);
				strncpy(peerobj->stod,SrcToDes.c_str(),sizeof(peerobj->stod));
				strncpy(peerobj->dtos,DesToSrc.c_str(),sizeof(peerobj->dtos));
				mappeer = peerobj;
			}
			*/
			//²é¿´¶Ô·½»á»°ÊÇ·ñ½¨Á¢
			LOG(INFO)<<"to find dest "<<DesToSrc.c_str();
			peer_info_t *destpeer = get_peer_obj(DesToSrc.c_str());
			if(destpeer != NULL)
			{
				insert_address_obj(mappeer->bev,destpeer->bev);
				insert_address_obj(destpeer->bev,mappeer->bev);
				char temp[64] = "{\"ErrorNum\": \"200\"}XXEE";    
				int length = strlen(temp);  
				bufferevent_write(mappeer->bev,temp,length);	//ÏìÓ¦
				bufferevent_write(destpeer->bev,temp,length);	//Í¨Öª¶Ô¶ËÒÑÁ¬½ÓÉÏÏß
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
	else
	{
		//Í¸´«Êı¾İ
		struct bufferevent *desbev = get_address_obj(bev);
		if(desbev != NULL)
		{
			bufferevent_write(desbev,content,len);
		}
		else
		{
			return;
		}
	}

	LOG(INFO)<<"to updat source timer"<<peerobj->stod;
	//¸üĞÂÔ´¶Ë³¬Ê±¶¨Ê±Æ÷
	if(event_initialized(&peerobj->timer))		
		event_del(&peerobj->timer);	
	event_assign(&peerobj->timer,s_evbase, -1, 0, peer_timeout_cb, (void*)peerobj);	
	struct timeval tv;	
	evutil_timerclear(&tv);	
	tv.tv_sec = PEER_SESSION_TIMEOUT;
	event_add(&peerobj->timer, &tv);

	LOG(INFO)<<"to updat dest timer"<<peerobj->dtos;
	//¸üĞÂ¶Ô¶Ë³¬Ê±¶¨Ê±Æ÷
	peer_info_t *despeer = get_peer_obj(peerobj->dtos);
	if(despeer != NULL)
	{
		//¸üĞÂ¶Ô¶ËµÄ³¬Ê±¶¨Ê±Æ÷
		if(event_initialized(&despeer->timer))		
			event_del(&despeer->timer);	
		event_assign(&despeer->timer,s_evbase, -1, 0, peer_timeout_cb, (void*)despeer);	
		struct timeval tv;	
		evutil_timerclear(&tv);	
		tv.tv_sec = PEER_SESSION_TIMEOUT;
		event_add(&despeer->timer, &tv);
	}
	else
	{
		LOG(INFO)<<"to updat dest timer"<<peerobj->dtos<< "not exists";
	}
}

//Á¬½Ó³ö´í»Øµ÷
void agent_error_cb(struct bufferevent *bev, short event, void *arg) 
{    
	peer_info_t * peerinfo = (peer_info_t *)arg;
	if( NULL == peerinfo)
	{
		LOG(ERROR)<<"connect error peer is null";
		return ;
	}
	evutil_socket_t fd = bufferevent_getfd(bev);    
	printf("fd = %u, ", fd);    
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

//½ÓÊÜ¿Í»§¶ËµÄÁ¬½ÓÇëÇó»Øµ÷
static void agent_accept_cb(int sockfd, short event_type,void *arg)
{
	int fd;	
	struct sockaddr_in sin;	
	socklen_t slen;	
	fd = accept(sockfd, (struct sockaddr *)&sin, &slen);	
	if (fd < 0){
		printf("ERROR: accept: ");	
		return;
	}
	LOG(INFO)<<"ACCEPT: fd = "<<fd;	

	//´´½¨Ò»¸ö½ÚµãĞÅÏ¢
	peer_info_t *peerobj = (peer_info_t *)calloc(sizeof(peer_info_t),1);
	assert(peerobj);
	
	//ÉèÖÃ³¬Ê±¶¨Ê±Æ÷
	if(event_initialized(&peerobj->timer))		
		event_del(&peerobj->timer);	
	event_assign(&peerobj->timer,s_evbase, -1, 0, peer_timeout_cb, (void*)peerobj);	
	struct timeval tv;	
	evutil_timerclear(&tv);	
	tv.tv_sec = PEER_SESSION_TIMEOUT;
	event_add(&peerobj->timer, &tv);

	//´´½¨Ò»¸öbuffereventÊÂ¼ş£¬°ó¶¨s_evbase	
	struct bufferevent *bev = bufferevent_socket_new(s_evbase,fd,BEV_OPT_CLOSE_ON_FREE); 
	peerobj->bev = bev;
	bufferevent_setcb(bev, agent_read_cb, NULL, agent_error_cb, peerobj); //ÉèÖÃ»Øµ÷º¯Êı	
	bufferevent_enable(bev, EV_READ|EV_WRITE|EV_PERSIST); //¿ªÆôbase
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

int set_socket_reuse(int sockfd)
{	
	int one = 1;
	if(sockfd < 0){
		LOG(ERROR)<<"sockfd is unavailable" <<sockfd;
		return -1;
	}
	return setsockopt(sockfd,SOL_SOCKET,SO_REUSEADDR,(void *)&one,sizeof(one));
}

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

int main(int argc, char ** argv) 
{
	//ÈÕÖ¾Ä£¿éÅäÖÃ ERROR|WARNING|INFO|DEBUG
	el::Loggers::addFlag(el::LoggingFlag::StrictLogFileSizeCheck);
	el::Configurations conf("logger.conf");
    el::Loggers::reconfigureAllLoggers(conf);
    el::Helpers::installPreRollOutCallback(rolloutHandler);

	system("mkdir -p ./logs-backup/");
	system("mkdir -p ./logs/");
	s_startup_time = ::time(NULL);
	
	//ºöÂÔÒ»Ğ©ĞÅºÅ
	signal(SIGPIPE,SIG_IGN);  
	
	if (parse_args(argc, argv) < 0) 
	{
		LOG(ERROR) << "parse_args:failed";
	    exit(1);
	}

	LOG(INFO) << "process has been started current version is "<< VERSION;
	LOG(INFO) << "server_ip=["<<s_server_ip<<"],server_port=["<<s_server_port<<"]";
	
	//ÇåÁãÈ«¾Ömap±í
	s_address_map.clear();	//µØÖ·±í
	s_peer_map.clear();		//½Úµã±í
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
	address.sin_port = htons(6609);
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
	LOG(INFO)<<"listening ......";
	s_evbase  = event_base_new();
	s_event = event_new(s_evbase,listen_fd,EV_READ|EV_PERSIST,agent_accept_cb,NULL);
	event_add(s_event,NULL);
	event_base_dispatch(s_evbase);
	LOG(INFO) <<"Clean exit";
	event_base_free(s_evbase);
	
	//ÈÕÖ¾Ä£¿éÇåÀí
	el::Helpers::uninstallPreRollOutCallback();
	return 0;		
}	


