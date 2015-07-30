Doradus Docker image on Amazon ECS
==================================

This demonstrates how to deploy and use Doradus (https://github.com/dell-oss/Doradus) as an Docker image with external Dynamo DB.

Running on Docker
----------------

1. Launch a container called doradus in the background at the port you prefer

        $ docker run -p <DOCKER_HOST_PORT>:<RESTPORT> -d --name doradus -e "AWS_ACCESS_KEY_ID=XXXXXX” -e "AWS_SECRET_ACCESS_KEY=XXXXXX” -e "RESTPORT=<RESTPORT>" -e "REGION=<REGION>” traduong1/docker-doradus

        For ex:
        docker run -p 8080:1234 -d --name doradus -e "XXXXX" -e "AWS_SECRET_ACCESS_KEY=XXXXX" -e "RESTPORT=1234" -e "REGION=XXXXX" traduong1/docker-doradus
        
        This would map port 1234 inside our container to port 8080 on the docker host. 
        
        where --name is to give the container name and  -d is to run Doradus server in the back ground. 
        
        You can use $docker logs docker logs -f doradus to tail the logs of Doradus server
              
2. Test

   Invoke this URL to list all applications under Doradus

        http://<docker_host>:<PORT>/_config
        
        For the example above, it is available at the 8080 port http:///<docker_host>:8080/_config