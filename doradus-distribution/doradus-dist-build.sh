#!/bin/bash
# Linux Script to build Doradus distribution

DIST_VERSION=2.4.0
DIST_FILE_NAME=Doradus-distribution-$DIST_VERSION.tar

#Build Doradus
cd ..
mvn clean install dependency:copy-dependencies -Dgpg.skip=true -Dmaven.javadoc.skip=true

cd doradus-distribution

#Clean up the doradus directory to place the new build binaries
if [ -d doradus ]
then
    rm -rf doradus
fi


#Create build directories and copy all necessary files for doradus-dist-run script to run successfully

mkdir doradus
cd doradus
mkdir dependency
mkdir resources
cp ../../doradus-jetty/target/doradus-jetty*.jar .
cp ../../doradus-jetty/target/dependency/*.jar dependency/
cp ../../doradus-server/src/main/resources/* resources/
cp ../../doradus-client/target/doradus-client-*.jar dependency/

cd ..

#Create the final distribution file
tar -cvf $DIST_FILE_NAME --exclude="*._*" --exclude="*build.sh" --exclude="*.bat" --exclude="*.zip" --exclude="./tools" --exclude="$DIST_FILE_NAME" .

#remove the build directories
rm -rf doradus

echo "Doradus Distribution created"
ls -la Doradus-distribution-*.tar
