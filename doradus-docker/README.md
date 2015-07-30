Doradus Docker image on Amazon ECS
==================================

This demonstrates how to deploy and use Doradus (https://github.com/dell-oss/Doradus) as an Docker image with external Dynamo DB.

Running on Docker
----------------

1. Launch a container called doradus in the background at the port you prefer

        docker run -p <PORT>:<PORT> -d --name doradus -e "AWS_ACCESS_KEY_ID=XXXXXX” -e "AWS_SECRET_ACCESS_KEY=XXXXXX” -e "RESTPORT=<PORT>" -e "REGION=<REGION>” traduong1/docker-doradus

2. Test

   Invoke this URL to list all applications under Doradus

        http://<docker_host>:<PORT>/_applications