#!/bin/bash
# Linux Script to start Cassandra and Doradus

if [ ! -d cassandra ]
then
    echo "Installing and configuring Cassandra 2.0.7  for Doradus..."
    curl -OL http://downloads.datastax.com/community/dsc-cassandra-2.0.7-bin.tar.gz
    tar -xzvf dsc-cassandra-2.0.7-bin.tar.gz    
    mv dsc-cassandra-2.0.7 cassandra
    mkdir cassandra-data
    cd cassandra-data
    mkdir data
    mkdir saved_caches
    mkdir commitlog
    touch system.log
    cd ../cassandra/conf/
    sed -e 's,/var/lib/cassandra,./cassandra-data,' -i "" cassandra.yaml 
    sed -e 's,/var/log/cassandra,./cassandra-data,' -i "" log4j-server.properties
    cd ../..
fi   
    
#Start Cassandra
if ! ps ax | grep -v grep | grep "cassandra" >/dev/null 2>&1
then   
    echo "Starting Cassandra 2.0.7..."
    cassandra/bin/cassandra
fi    

while true
do
  sleep 5
  if ps ax | grep -v grep | grep "cassandra"  >/dev/null 2>&1
  then
    break
  fi
done

echo "Cassandra has started..."

#Start Doradus

cd doradus
java -cp ./*:./resources/*:./dependency/* com.dell.doradus.core.DoradusServer > /dev/null 2>&1 &

while true
do
  sleep 2
  if ps ax | grep -v grep | grep "doradus"  >/dev/null 2>&1
  then
    break
  fi
done

echo "Doradus has started..."

java -cp ./*:./resources/*:./dependency/* com.dell.doradus.client.utils.HelloSpider localhost 1123

echo "Verify HelloSpider application created at http://localhost:1123/_applications"