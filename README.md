#Doradus at Strata 2015
Doradus was presented at [Strata+Hadoop World 2015](http://strataconf.com/big-data-conference-ca-2015/) in San Jose, CA. Slides for the session, titled *[One Billion Objects in 2GB: Big Data Analytics on Small Clusters with Doradus OLAP](http://strataconf.com/big-data-conference-ca-2015/public/schedule/detail/38276)*, can be found here: [http://www.slideshare.net/randyguck/strata-presentation-doradus](http://www.slideshare.net/randyguck/strata-presentation-doradus). Be sure to check out the slide notes, which provide extra details. If you attended the conference or have an
O'Reilly acccount, you can watch the video of the session [here](http://player.oreilly.com/videos/9781491924143).

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
The v2.3 release is now available! This release contains many new features. Here's a summary of some of them:

- **OpenShift Support**: Doradus can be run in an OpenShift cartridge. See [this Wiki note](https://github.com/dell-oss/Doradus/wiki#openshift-cartridge) and the [doradus-openshift-cartridge](https://github.com/TraDuong1/openshift-origin-cartridge-doradus) Github project.

- **Multi-tenant features**: Doradus can now run in *multi-tenant* mode, in which each *tenant*
  is mapped to a Cassandra keyspace. Each tenant's data is physically isolated, and each tenant
  can request its own replication factor. Expect more features to support multi-tenant operation
  in coming releases.

- **OLAP multi-threaded merging**: OLAP can now use multiple threads to merge shards. In
  doradus.yaml, `olap_merge_threads` specifies the number of threads to use. Another new option,
  `olap_compression_threads`, can be used to speed-up compression before data segments are
  written to Cassandra. These two features can significantly speed-up shard merging time.

- **Faster OLAP updates**: A new indexing algorithm is used for loading OLAP batches, yielding a
  20-50% improvement in data load time. This improvement is amplified when loading larger batches.
  In bulk load scenarios, we have tested batch loads **up to 1 million objects/second** on a single node.

- **Multiple sort fields**: Object queries for both Spider and OLAP applications can now use
  multiple sort fields. Example: `&o=LastName DESC,FirstName ASC`.

- **Aggregate query improvements**: New aggregate query features have been added for both Spider
  and OLAP applications. For example, both support new `FIRST` and `LAST` grouping functions. OLAP
  applications can use a new `SETS` function for creating arbitrary grouping sets. `INCLUDE` and
  `EXCLUDE` can now be combined in grouping expressions. Several other improvements have also been
  made.

- **OLAP metric computations in object queries**: Object queries for OLAP applications can now
  compute metric functions for each perspective object using the metric parameter: `&m`.

- **Optional application key**: The application `key` property is now optional and can be excluded
  in schema definitions.

- **Data aging check frequency**: For both OLAP and Spider applications, the frequency of the
  background data aging task is now controlled by the `data-check-frequency` option. The
  `schedules` section is no longer used in schemas.

- **Other Simplifications**: Other minor Spider and OLAP features have been removed. (We're always
  thinking about unused/marginal features to remove to keep the code simpler.)

Many other enhancements and changes have been also been made for v2.3. See the **Recent Changes**
 sections in the Wiki pages or PDF documents.

#What's Even Newer?
Immediately after finishing-up the v2.3 relesase, we restructured the project tree a
little for two reasons:

1. We reorganized the folder structure to more closely follow the Maven [Standard Directory
   Layout](https://maven.apache.org/guides/introduction/introduction-to-the-standard-directory-layout.html).
   So, for example, `.yaml` and `.properties` files reside in a `resources` folder instead of a
   `config` folder.
   
2. We separated out the embedded Jetty server into its own folder. This allows the core Doradus
   server to run within other web servers such as Tomcat while avoiding jar file conflicts.
   
3. We've combined the regression test app and test cases into a new project, called
   *doradus-regression-tests*.

#Doradus Components
Doradus consists of following components:

- **doradus-client**: This is an optional module that allows Java clients to access a Doradus
  server using plain old Java objects (POJOs). It hides the REST API and provides basic features
  for connecting/reconnecting, connection pools, message compression, and exception handling.
  Requests and results are passed as simple Java classes.

- **doradus-common**: This module consists of common classes used by both the client and server
  modules.
  
- **doradus-distribution**: This is a precompiled version of Doradus with scripts to install
  Cassandra and Doradus and get them running. This is brand new and still under construction. More
  info soon!
  
- **doradus-jetty**: This module embeds the [Jetty](http://eclipse.org/jetty/) web server,
  allowing Doradus to run as a standalone process. By moving Jetty dependencies to this project,
  Doradus can be used with other web servers without conflicts.
  
- **doradus-regression-tests**: This module contains a custom regression test application and
  numerous test case files. We add new tests as new features are implemented and when bugs are
  found.

- **doradus-server**: This project houses the core Doradus server. The server can be started as a
  standalone application with or without the embedded Jetty server, or it can be embedded in another
  application. It reads doradus.yaml for configuration options.
  
#Requirements
Doradus requires Java 1.7 or higher and Cassandra 2.x.

#Installing Cassandra
You can use an existing Cassandra installation or download Cassandra on your own. Any 2.x release
should work, though we've tested Doradus the most with 2.0.x. The protocol that Doradus uses to
communicate with Cassandra (Thrift or CQL) as well as the Cassandra host name(s) and port are
configured in the doradus.yaml.

#Building Doradus
Doradus supports Maven and Ant builds, though each places binaries and config files in slightly
different directory structures. To build using Maven, from the root folder enter:

	mvn clean install dependency:copy-dependencies -Dgpg.skip=true

To build Doradus using Ant, just enter `ant` from the root directory.

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

#Known Issues
- With Doradus OLAP, currently there is no coordination between Doradus instances to serialize
  *merge* commands to the same shard. For now, user applications should send all *merge* requests
  to the same Doradus instance.

- Currently, Doradus Spider uses a very simplistic query execution strategy. (That means we have
  not implemented any kind of cost-based query optimization.) Instead, it naïvely executes clauses
  in left-to-right order. This means you'll generally get better query performance by specifying
  your most *selective* clause first. For example, in the query expression `A >= foo AND B = bar`,
  the second clause is more selective, meaning it (probably) selects fewer objects. If the first
  clause selects a lot of objects, the query will generally execute faster if you reverse the
  clauses: `B = bar AND A >= foo`.
 
#License
Doradus is available under the Apache License Version 2.0. See LICENSE.txt or
[http://www.apache.org/licenses/](http://www.apache.org/licenses/) for a copy of this license.
