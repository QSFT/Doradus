REM Windows Script to start Cassandra and Doradus

REM Start Cassandra
if not exist cassandra (
    echo "Installing and configuring Cassandra 2.0.7 for Doradus"
    .\tools\wget http://downloads.datastax.com/community/dsc-cassandra-2.0.7-bin.tar.gz
    tools\7za.exe x -tgzip dsc-cassandra-2.0.7-bin.tar.gz  && tools\7za.exe  x dsc-cassandra-2.0.7-bin.tar    
    ren dsc-cassandra-2.0.7 cassandra
    mkdir cassandra-data
    cd cassandra-data
    mkdir data
    mkdir saved_caches
    mkdir commitlog
    .\tools\touch system.log
    cd ..\cassandra\conf
    .\tools\sed -e 's,/var/lib/cassandra,./cassandra-data,' -i "" cassandra.yaml 
    .\tools\sed -e 's,/var/log/cassandra,./cassandra-data,' -i "" log4j-server.properties
    cd ..\..
)   

start cassandra\bin\cassandra

timeout 10
echo "Cassandra has started..."

REM Start Doradus
cd doradus
start java -cp .\*;.\resources\*;.\dependency\* com.dell.doradus.core.DoradusServer

timeout 5
echo "Doradus has started..."

java -cp .\*;.\resources\*;.\dependency\* com.dell.doradus.client.utils.HelloSpider localhost 1123

echo "Verify HelloSpider application created at http://localhost:1123/_applications"