#!/bin/bash
# Linux Script to setup and start Cassandra instance
if [ ! -d cassandra ]
then

	curl -OL http://downloads.datastax.com/community/dsc-cassandra-2.0.7-bin.tar.gz
	tar -xzvf dsc-cassandra-2.0.7-bin.tar.gz
	rm *.tar.gz
	mv dsc-cassandra-* cassandra
	mkdir cassandra-data
	cd cassandra-data
	mkdir data
	mkdir saved_caches
	mkdir commitlog
	touch system.log
	cd ../cassandra/conf/
	sed -ig 's,/var/lib/cassandra,./cassandra-data,' cassandra.yaml	
	sed -ig 's,/var/log/cassandra,./cassandra-data,' log4j-server.properties
	cd ../..

fi
cd cassandra
bin/cassandra