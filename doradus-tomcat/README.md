Doradus under Tomcat 7 WebServer
================================

Follow these simple steps to use Doradus with Tomcat 7 Web Server.  

- Install and run Tomcat 7

  Download and install Tomcat 7 from https://tomcat.apache.org/tomcat-7.0-doc/appdev/installation.html on your machine at directory which will be used as <TOMCAT_HOME> in this document
  
  Start Tomcat

       Changed the port 80 to available port, for ex 8080 in server.xml     
       Go to <TOMCAT_HOME>/bin and run $./catalina.sh run for Mac/Linux 
       or $catalina run for Windows

- Build the war file for doradus-tomcat webapp  

       Override the Doradus configuration settings if necessary by copying the custom doradus.yaml to doradus-tomcat/src/main/resources
 
       Modify web.xml under src/main/webapp/WEB-INF to name the URL prefix as ROOT or _api or something else and use that name for the command below
       
       Run this maven command under doradus-tomcat directory, for example
            
           for having Doradus at ROOT level

           $ mvn compile war:war -Dwar.warName=ROOT
           
           for having Doradus at URL prefix level

           $ mvn clean compile war:war -Dwar.warName=_api

       Copy the war file (ROOT.war or _api.war, for example) from./target to <TOMCAT_HOME>/webapps. The doradus-tomcat app will be deployed when the war file automatically gets exploded, then Dordus will be started at 8080 port.
      
- Launch Doradus Tomcat application at port 8080

  The URL is        

       http://localhost:8080/_applications for Doradus at ROOT level
       or http://localhost:8080/_api/_applications for Doradus at prefix level.
