#!/bin/bash
# Linux Script to build and start Doradus

mvn clean install dependency:copy-dependencies -Dgpg.skip=true -Dmaven.javadoc.skip=true
cd doradus-jetty
java -cp ../doradus-server/target:target/classes:target/dependency/* com.dell.doradus.core.DoradusServer
