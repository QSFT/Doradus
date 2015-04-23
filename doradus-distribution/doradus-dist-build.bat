rem Windows Script to build Doradus distribution

set DIST_VERSION=2.4
set DIST_FILE_NAME=Doradus-distribution-%DIST_VERSION%.zip

rem Build Doradus
cd ..
rem mvn clean install dependency:copy-dependencies -Dgpg.skip=true -Dmaven.javadoc.skip=true

cd doradus-distribution

rem Clean up the doradus directory to place the new build binaries
rmdir /Q -doradus

rem Create directories and copy all necessary files for doradus-dist-run script to run successfully

mkdir doradus
cd doradus
mkdir dependency
mkdir resources
copy ..\..\doradus-jetty\target\doradus-jetty*.jar .
copy ..\..\doradus-jetty\target\dependency\*.jar dependency
copy ..\..\doradus-server\src\main\resources\* resources
copy ..\..\doradus-client\target\doradus-client-*.jar dependency

cd ..

rem Create the final distribution file
7za a %DIST_FILE_NAME% -r .

echo "Doradus Distribution created"
dir Doradus-distribution-*.zip      

