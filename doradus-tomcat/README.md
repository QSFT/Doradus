#Running Doradus under Tomcat 7

Follow these steps to use Doradus with Tomcat 7 Web Server.  

- Download and install [Tomcat 7](https://tomcat.apache.org/tomcat-7.0-doc/appdev/installation.html) on your machine. The root directory will be known as `<TOMCAT_HOME>` below.
  
- Copy the latest `doradus.yaml` file from `./doradus-server/src/main/resources` to `./doradus-tomcat/src/main/resources` and comment out the `webserver_class` parameter. Example:

	`# webserver_class: com.dell.doradus.server.JettyWebServer`
  
- Choose whether to run Doradus in URL paths as `ROOT` (no prefix) or with a URI prefix. The `web.xml` file under `./doradus-tomcat/src/main/webapp/WEB-INF` is preconfigured to support `ROOT` or the URI prefix `_api`. If you'd like to use a different URI prefix, modify this file and add another `<servlet-mapping>` section that matches your prefix.

- Generate a `war` file by using the following Maven command, substituting your chosen URI prefix for `ROOT`:

````
	$ cd doradus-tomcat
	$ mvn compile war:war -Dwar.warName=ROOT
````

- Copy the war file (e.g., `ROOT.war` or `_api.war`) from `./target` to `<TOMCAT_HOME>/webapps`.
      
-  Make any other Tomcat configuration changes needed. For example, you can change the default port (8080) to port 80 by editing the file `<TOMCAT_HOME>config/server.xml`.

- Run Tomcat from the folder `<TOMCAT_HOME>/bin`. On Mac and Linux enter `catalina.sh run`. On Windows just enter `catalina`. Tomcat will expand the `war` file you copied, creating a folder with the same name.
       
- Test that Doradus is running using a browser and the REST command `GET /_applications`. If you're using an API prefix, add it to all URLs. For example, if Tomcat is running locally on port 8080 and you're using the API prefix `_api`, the full URL is:

````
	http://localhost:8080/_api/_applications
````

