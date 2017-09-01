#Dockerfile
FROM daocloud.io/geyijun/basic_image_super:v0.03
MAINTAINER geyijun<geyijun@xiongmaitech.com>

COPY supervisord.conf /etc/supervisord.conf
COPY logger.conf /xm_workspace/xmcloud3.0/AgentServer/
COPY libmatch.so /xm_workspace/xmcloud3.0/AgentServer/
COPY agent_server /xm_workspace/xmcloud3.0/AgentServer/
COPY reclaim_log /xm_workspace/xmcloud3.0/AgentServer/
COPY *.sh /xm_workspace/xmcloud3.0/AgentServer/
RUN chmod 777 /xm_workspace/xmcloud3.0/AgentServer/*

WORKDIR /xm_workspace/xmcloud3.0/AgentServer/

EXPOSE 6611
CMD ["supervisord"]
