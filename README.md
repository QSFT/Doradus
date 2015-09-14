#Doradus In The Wild
Just learning about Doradus? Here are some links to recent presentations:

- **Doradus at Strata+Hadoop World**: Doradus was presented at [Strata+Hadoop World 2015](http://strataconf.com/big-data-conference-ca-2015/) in San Jose, CA. Slides for the session, titled *[One Billion Objects in 2GB: Big Data Analytics on Small Clusters with Doradus OLAP](http://strataconf.com/big-data-conference-ca-2015/public/schedule/detail/38276)*, can be found here: [http://www.slideshare.net/randyguck/strata-presentation-doradus](http://www.slideshare.net/randyguck/strata-presentation-doradus). Be sure to check out the slide notes, which provide extra details. If you attended the conference or have an O'Reilly acccount, you can watch the video of the session [here](http://player.oreilly.com/videos/9781491924143).

- **O'Reilly Webcast**: Doradus OLAP was highlighted in an [O'Reilly webcast](http://www.oreilly.com/pub/e/3478). If you missed the session, you can still register and get access to the recorded session.

#What is Doradus?
New to Doradus? There's a quick overview in this wiki page:
[Doradus Overview](https://github.com/dell-oss/Doradus/wiki/Overview). The Wiki pages also have
detailed information on these key topics:
[Doradus OLAP](https://github.com/dell-oss/Doradus/wiki/Doradus%20OLAP%20Databases),
[Doradus Spider](https://github.com/dell-oss/Doradus/wiki/Doradus-Spider-Databases), and
[Doradus Administration](https://github.com/dell-oss/Doradus/wiki/Doradus-Administration). If you
like nice big PDF files, see the [docs](https://github.com/dell-oss/Doradus/tree/master/docs)
folder. Keep reading to find out what's new and tips for downloading, building, and running
Doradus.

#What's New?
The v2.4 release is now available! Here's a summary of some of the new features:

- **OLAP optimizations**: Several performance and space optimizations were implemented for Doradus OLAP, including the `olap_search_threads` parameter, which allows parallel shard searching for multi-shard queries.

- **OLAP merging**: OLAP applications can now request automatic background merging with the auto-merge option. This option is compatible with explicit, REST command-based merge requests, and merge tasks are distributed in a multi-instance cluster.

- **OLAP live data queries**: Data batches that have not yet been merged can be included in object and aggregate queries by adding the parameter `&uncommitted=true` to the URI. There are a few caveats with this option, but when updates mostly add new objects to shards, this option allows you to query "live" data.

- **OLAP new query functions**: OLAP supports a new `ROUNDUP` function for aggregate queries, and new set quantification functions: `EQUALS`, `INTERSECTS`, `DISJOINT`, and `CONTAINS`.

- **New Logging Service**: A new storage service is available, optimized for immutable, time-series log data. This service can store up to 500K events/second/node, uses very little disk space, and provides fast searching and aggregate queries.

- **Docker support**: In addition to the OpenShift environment, Doradus can now be hosted as a Docker application using its embedded Jetty server. The Docker implementation also supports the AWS Elastic Container Service (ECS), giving Doradus automatic load balancing and failover.

- **Logging for Docker apps**: A Logstash-based Docker image is available to capture Docker application log files and store them using the new Doradus Logging service.

- **Dory client**: The doradus-client package offers a new Java POJO client we call "Dory". The primary interface class is `com.dell.doradus.dory.DoradusClient`. This generic client can call all REST commands for all active storage services, including future ones. It uses the builder pattern for constructing application schemas, update batches, and queries. You can find an example application at `com.dell.doradus.client.utils.HelloDory`.

- **REST command metadata**: You can get a list of all available REST commands with the new command: `GET /_commands`. (Add `?format=json` to the URI to see the result in JSON.) The name, description, and parameters of each command are described.

- **Multi-tenant features**: New multi-tenant features have been added such as the ability to define tenant-specific users with explicit permission lists.

- **Config command**: Get the server's current version and parameter settings with the REST command: `GET /_config`.

- **Java 1.8**: Doradus is now compiled with and requires Java 1.8.

#Doradus Components
Doradus consists of following components:

- **doradus-client**: This is an optional module that allows Java clients to access a Doradus
  server using plain old Java objects (POJOs). It hides the REST API and provides basic features
  for connecting/reconnecting, connection pools, message compression, and exception handling.
  Requests and results are passed as simple Java classes. The doradus-client module provides two
  different approaches for creating client applications:
  
  1)`com.dell.doradus.client.Client`: This class uses custom `Session` objects for accessing
     storage service-specific commands. For example, `SpiderSession.addBatch()` can be used to add
     a batch of data to a Spider application. See `com.dell.doradus.client.utils.HelloSpider` for
     an example application that uses this interface.
     
  2)`com.dell.doradus.dory.DoradusClient`: This is the main class for the "Dory" client, which
     is a generic interface that can call any command for any application. See the application
     `com.dell.doradus.client.utils.HelloDory` for an example of how to use this approach.

- **doradus-common**: This module consists of common classes used by both the client and server
  modules.
  
- **doradus-distribution**: This is a precompiled version of a recent Doradus release with scripts
  to install Cassandra and Doradus and get them running.
  
- **doradus-docker**: This module provides a `Dockerfile` and instructions on how to use Doradus
  as a Docker application.
  
- **doradus-dynamodb**: This module provides a concrete `DBService` implementation that allows
  Dorauds to use AWS DynamoDB for persistence instead of Cassandra. This module is still under
  development and should be considered experimental.
  
- **doradus-jetty**: This module embeds the [Jetty](http://eclipse.org/jetty/) web server,
  allowing Doradus to run as a standalone process.
  
- **doradus-regression-tests**: This module contains a custom regression test application and
  numerous test case files. We add new tests as new features are implemented and when bugs are
  found.

- **doradus-server**: This project houses the core Doradus server. The server can be started as a
  standalone application with or without the embedded Jetty server, or it can be embedded in another
  application. It reads doradus.yaml for configuration options.
  
- **doradus-tomcat**: This module provides interface code and instructions on how to use Doradus
  with Apache Tomcat. This provides an alternate way to serve the REST API instead of the
  embedded Jetty server.
  
#Requirements
Doradus requires Java 1.8 or higher and Cassandra 2.x.

#Installing Cassandra
You can use an existing Cassandra installation or download Cassandra on your own. Any 2.x release
should work, though we've tested Doradus the most with 2.0.x. The protocol that Doradus uses to
communicate with Cassandra (Thrift or CQL) as well as the Cassandra host name(s) and port are
configured in the doradus.yaml.

#Building Doradus
Doradus supports Maven and Ant builds, though each places binaries and config files in slightly
different directory structures. To build using Maven, from the root folder enter:

	mvn clean install dependency:copy-dependencies -Dgpg.skip=true

To build Doradus using Ant, just enter `ant` from the root directory. See the
[Doradus Administration](https://github.com/dell-oss/Doradus/blob/master/docs/Doradus%20Administration.pdf)
document for more details on building Doradus.

#Configuring Doradus

All default configuration come from the `doradus.yaml` file. You can edit it directly or
override parameters in command line arguments by prefixing each parameter name with a '-' and
specifying the parameter value next. For example, doradus.yaml defines `restport: 1123`, which
sets the REST API listening port number. To override this to 5711, add the command line
argument:

    java -cp ... com.dell.doradus.core.DoradusServer -restport 5711
    
When Doradus runs, it connects to the Cassandra server(s) as configured in doradus.yaml. If
Cassandra is not running when the server starts, it will try connecting every 5 seconds until
successful. In the mean time, REST requests that require the database connection will receive a
503 response.

See the [Doradus Administration](https://github.com/dell-oss/Doradus/blob/master/docs/Doradus%20Administration.pdf) documentation for more information about configurating and running Doradus.

#Running Doradus
If you build Doradus with Maven, you can run Doradus as a stand-alone process using the embedded
Jetty server to handle REST commands. From the `doradus-jetty` folder enter:

	java -cp ../doradus-server/target:target/classes:target/dependency/* com.dell.doradus.core.DoradusServer
	
For historic reasons, the Ant build uses a slightly different folder structure. To run Doradus
with the embedded Jetty server after an Ant build, from the `doradus-server` folder enter:

	java -cp ./lib/*:./config/* com.dell.doradus.core.DoradusServer
	
Since Doradus is stateless, multiple instances can be run against the same Cassandra cluster.

#Accessing Doradus
A browser can be used to access Doradus REST API commands. For example, to list applications that
have been defined:

    http://localhost:1123/_applications
    
An *application* is the name Doradus uses for what other databases might call a *schema*. To create
a minimal application (managed by Doradus Spider by default), you can use the following curl command:

	curl -H "content-type: text/xml" -d '<application name="Stuff"/>' http://localhost:1123/_applications

Doradus supports XML and JSON for all commands; below is the same command using JSON:

	curl -H "content-type: application/json" -d '{"Stuff": {}}' http://localhost:1123/_applications

These commands create a Spider-managed application called `Stuff`. Initially, the application has
no predefined tables. However, an application option called `AutoTables` defaults to `true`, so new
tables are created automatically as they are referenced. Spider also supports dynamically-added
fields, so you can immediately create a new table and add some objects to it with the following POST
command and message. We'll use JSON for this example:

	POST /Stuff/Messages
	{"batch": {
		"docs": [
			{"doc": {
				"Subject": "Here's a subject",
				"Body": "Here a body"
			}},
			{"doc": {
				"Subject": "Here's another subject",
				"Body": "Here's another body"
			}}
		]
	}}

This command creates a new table called `Messages` and adds two new objects. The `Subject` and
`Body` fields are indexed as full text fields, and each object is assigned a default `_ID`. You
can fetch all objects in the table using this REST command:

	GET /Stuff/Message/_query?q=*

You can use a browser or your favorite HTTP library to send REST commands. Alternatively, Java
applications can use the Doradus Client library to hide the REST API and use plain old Java
objects (POJOs). See the `doradus-client` Java docs for more information.

#Running Regression Tests

The **doradus-regression-tests** folder contains a custom regression test application and
a set of test scripts. To run regression tests, first modify the file
`./doradus-regression-tests/src/main/resources/config.xml`, if necessary, to point to the
`regression-tests` base directory. Example:

	<DEFINE name="tests.root" value="/Users/JDoe/Doradus/doradus-regression-tests/src/main/regression-tests"/>

Under the `regression-tests` folder, subfolders have names such as `bugs` and `features`.
Within a subfolder, a test is defined by the following files:

* `<name>.test.xml`: This file defines the instructions to be carried-out by the test
  processor for the test called `<name>`.

* `<name>.defs.xml`: This optional file defines schemas and input data blocks that can be
  referenced by the test called `<name>`.

* `<name>.result.txt`: This file contains the expected output for the test called `<name>`.

The regression test processor's main is `com.dell.doradus.testprocessor.Program` and requires
no parameters. Here's a simple script that can be used from the ./doradus-regression-tests
folder to run the tests:

	java -cp ./target/classes:./target/dependency/* com.dell.doradus.testprocessor.Program

If a test fails, the test processor will create two files:

* `<name>.xresult.txt`: This file contains the actual output generated by the test.

* `<name>.xdiff.txt`: This file compares the expected and actual output and flags lines
  that are added to (+) or deleted from (-) the expected results.

The test processor also creates an HTML report with the file name defined in `config.xml`
as `<report>`. This provides a quick overview of the test run, including differences for
failed tests.

You can limit the test suite to specific tests by modifying `config.xml`. For example,
to run only test `bd.010.SPIDER` in the `bugs` directory only:

	<DEFINE name="tests.root"
	   value="C:\DELL-OSS\Doradus\doradus-regression-tests\src\main\regression-tests\bugs"/>
	<test-suite root="${tests.root}">
    	<include>
        	<dir path=".">
            	<test name="bd.010.SPIDER"/>
			</dir>
		</include>
	</test-suite>

#Resources
The following are the primary Doradus resources:

- **Source code**: Source code can be downloaded here, from this Github project:

		https://github.com/dell-oss/Doradus
       
- **Documentation**: Our Github project includes extensive Wiki pages for Doradus OLAP, Spider,
  and Administration. Alternatively, you'll find complete PDF versions of the same information
  from the following folder:

		https://github.com/dell-oss/Doradus/docs
    
- **Issues**: Please feel free to post bug reports and feature enhancements in the Github Issues
  area:
  
		https://github.com/dell-oss/Doradus/issues
    
- **Downloads**: Source, binary, and Java doc bundles can be downloaded from Maven Central by
  searching for `Doradus`. Example:
  
  		http://search.maven.org/#search%7Cga%7C1%7Cdoradus

#License
Doradus is available under the Apache License Version 2.0. See LICENSE.txt or
[http://www.apache.org/licenses/](http://www.apache.org/licenses/) for a copy of this license.
