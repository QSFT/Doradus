#!/usr/bin/env bash

echo "env variables AWS_REGION for DynamoDB or CASSANDRA_NODE_IP for Cassandra required"
cd /opt/doradus

if [ -n "$AWS_REGION" ]
then
    echo "using DynamoDB connection info"
    CLASSPATH="./target/classes:./target/dependency/*:../doradus-jetty/target/classes:../doradus-jetty/target/dependency/*"
    JVM_PARAMS="-Xmx1G -Dddb.region=$AWS_REGION"
    PARAMS="-dbservice com.dell.doradus.db.dynamodb.DynamoDBService -restport $RESTPORT"
    cd doradus-dynamodb
elif [ -n "$CASSANDRA_NODE_IP" ]
    then
        echo "using CassandraDB connection info"
        echo "dbhost=$CASSANDRA_NODE_IP"
        echo "dbport=$CASSANDRA_NODE_PORT"    
        echo "dbuser=$CASSANDRA_SUPERUSER_NAME"    
        echo "dbpassword=$CASSANDRA_SUPERUSER_PW"                
        CLASSPATH="./target/classes:target/dependency/*"
        JVM_PARAMS="-Xmx1G"
        PARAMS="-restport $RESTPORT -dbhost $CASSANDRA_NODE_IP -dbport $CASSANDRA_NODE_PORT -dbuser $CASSANDRA_SUPERUSER_NAME -dbpassword $CASSANDRA_SUPERUSER_PW"
        cd doradus-jetty
    else
        echo "No DB connection info" 
fi

echo Starting Doradus...
java -cp $CLASSPATH $JVM_PARAMS com.dell.doradus.core.DoradusServer $PARAMS