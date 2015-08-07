Doradus Docker image 
====================

This demonstrates how to deploy and use Doradus (https://github.com/dell-oss/Doradus) as an Docker image with external NOSQL DB server.


Running Doradus connecting to Dynamo DB on ECS
----------------------------------------------
1. Follow this doc if you want to install Docker on EC2 Amazon instance

    http://docs.aws.amazon.com/AmazonECS/latest/developerguide/docker-basics.html#install_docker


2. Launch a container called "doradus" in the background at the port you prefer

        $ docker run -p <DOCKER_HOST_PORT>:<RESTPORT> -d --name doradus -e "AWS_ACCESS_KEY_ID=XXXXXX” -e "AWS_SECRET_ACCESS_KEY=XXXXXX” -e "RESTPORT=<RESTPORT>" -e "REGION=<REGION>” traduong1/doradus-docker

        For ex:
        docker run -p 8080:1234 -d --name doradus -e "XXXXX" -e "AWS_SECRET_ACCESS_KEY=XXXXX" -e "RESTPORT=1234" -e "REGION=XXXXX" traduong1/docker-doradus
        
        This would map port 1234 inside our container to port 8080 on the docker host. 
        
        where --name is to give the container name and  -d is to run Doradus server in the back ground. 
        
        You can use $docker logs -f doradus to tail the logs of Doradus server
        
Running Doradus connecting to external Cassandra
------------------------------------------------
1. Launch an external Cassandra DB instance

2. Launch a container called "doradus" in the background at the port you prefer, for ex

        $ docker run -p 8080:8080 --name doraduscass -e "RESTPORT=8080" -e "CASSANDRA_NODE_IP=127.0.0.1" -e "CASSANDRA_NODE_PORT=9160" -e "CASSANDRA_SUPERUSER_NAME=cassandra" -e "CASSANDRA_SUPERUSER_PW=cassandra" traduong1/doradus-docker
             
        You can use $docker logs -f doradus to tail the logs of Doradus server        
              
Verifying
---------

   Invoke this URL to test if Doradus server is up and running

        http://<docker_host>:<PORT>/_config
        
        For the example container above, it is available at the 8080 port http://<docker_host>:8080/_config