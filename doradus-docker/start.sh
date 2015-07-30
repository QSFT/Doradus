#!/usr/bin/env bash
#PORT=$1
#REGION=$2

CLASSPATH="./target/classes:./target/dependency/*:../doradus-jetty/target/classes:../doradus-jetty/target/dependency/*"
JVM_PARAMS="-Xmx1G -Dddb.region=$REGION"
PARAMS="-dbservice com.dell.doradus.db.dynamodb.DynamoDBService -restport $RESTPORT"

echo Starting Doradus...
cd /opt/doradus
cd doradus-dynamodb

java -cp $CLASSPATH $JVM_PARAMS com.dell.doradus.core.DoradusServer $PARAMS
#java -cp ./target/classes:./target/dependency/*:../doradus-jetty/target/classes:../doradus-jetty/target/dependency/* -Xmx1G -DDDB_PROFILE_NAME=doradus -Dddb.region=us-west-1 com.dell.doradus.core.DoradusServer -dbservice com.dell.doradus.db.dynamodb.DynamoDBService -restport $PORT
