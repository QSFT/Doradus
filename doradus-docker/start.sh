#!/usr/bin/env bash

CLASSPATH="./target/classes:./target/dependency/*:../doradus-jetty/target/classes:../doradus-jetty/target/dependency/*"
JVM_PARAMS="-Xmx1G -Dddb.region=$REGION"
PARAMS="-dbservice com.dell.doradus.db.dynamodb.DynamoDBService -restport $RESTPORT"

echo Starting Doradus...
cd /opt/doradus
cd doradus-dynamodb

java -cp $CLASSPATH $JVM_PARAMS com.dell.doradus.core.DoradusServer $PARAMS