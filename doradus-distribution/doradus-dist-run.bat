REM Windows Script to start Cassandra and Doradus

REM Start Cassandra
echo "Starting Cassandra 2.0.7..."
start cassandra\bin\cassandra

timeout 5
echo "Cassandra has started..."

REM Start Doradus

cd doradus
java -cp .\*;.\resources\*;.\dependency\* com.dell.doradus.core.DoradusServer


echo "Doradus has started..."

java -cp -cp .\*;.\resources\*;.\dependency\* com.dell.doradus.client.utils.HelloSpider localhost 1123

echo "Verify HelloSpider application created at http://localhost:1123/_applications"