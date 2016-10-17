#Dockerfile
FROM daocloud.io/geyijun/basic_image:v0.03
MAINTAINER geyijun<geyijun@xiongmaitech.com>

COPY logger.conf /xm_workspace/xmcloud3.0/AgentServer/
COPY libmatch.so /xm_workspace/xmcloud3.0/AgentServer/
COPY agent_server /xm_workspace/xmcloud3.0/AgentServer/
COPY *.sh /xm_workspace/xmcloud3.0/AgentServer/
RUN chmod 777 /xm_workspace/xmcloud3.0/AgentServer/*

WORKDIR /xm_workspace/xmcloud3.0/AgentServer/

EXPOSE 6608
#ENTRYPOINT ["agent_access"]
CMD ["./start_agentserver.sh"] 
