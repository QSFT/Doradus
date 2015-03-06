#Doradus at Strata 2015
Doradus was presented at [Strata+Hadoop World 2015](http://strataconf.com/big-data-conference-ca-2015/) in San Jose, CA. Slides for the session, titled *[One Billion Objects in 2GB: Big Data Analytics on Small Clusters with Doradus OLAP](http://strataconf.com/big-data-conference-ca-2015/public/schedule/detail/38276)*, can be found here: [http://www.slideshare.net/randyguck/strata-presentation-doradus](http://www.slideshare.net/randyguck/strata-presentation-doradus). Be sure to check out the slide notes, which provide extra details.

#What's New?
We're putting the finishing touches on the new v2.3 release. This release contains many new features: here's a summary of some of them:

- **OpenShift Support**: Doradus can be run in an OpenShift cartridge. See [this Wiki note](https://github.com/dell-oss/Doradus/wiki#openshift-cartridge) and the [doradus-openshift-cartridge](https://github.com/TraDuong1/openshift-origin-cartridge-doradus) Github project.

- **OLAP multi-threaded merging**: OLAP can now use multiple threads to merge shards. In doradus.yaml, `olap_merge_threads` specifies the number of threads to use. This feature can significantly speed-up the shard merge time.

- **Faster OLAP updates**: A new indexing algorithm is used for loading OLAP batches, yielding a 20-50% improvement in data load time. This improvement is amplified when loading larger batches.

- **Multiple sort fields**: Object queries for both Spider and OLAP applications can now use multiple sort fields. Example: `&o=LastName DESC,FirstName ASC`.

- **Aggregate query improvements**: New aggregate query features have been added for both Spider and OLAP applications. For example, both support new `FIRST` and `LAST` grouping functions. OLAP applications can use a new `SETS` function for creating arbitrary grouping sets. `INCLUDE` and `EXCLUDE` can now be combined in grouping expressions. Several other improvements have also been made.

- **OLAP metric computations in object queries**: Object queries for OLAP applications can now compute metric functions for each perspective object using the metric parameter: `&m`.

- **Optional application key**: The application `key` property is now optional and can be excluded in schema definitions.

- **Data aging check frequency**: For both OLAP and Spider applications, the frequency of the background data aging task is now controlled by the `data-check-frequency` option. The `schedules` section is no longer used in schemas.

- **Other Simplifications**: Other minor Spider and OLAP features have been removed. (We're always thinking about unused/marginal features to remove to keep the code simpler.)

Many other enhancements and changes have been also been made for v2.3. See the *Recent Changes* sections in the Wiki pages or PDF documents.

The next sections provide an overview of Doradus, its architecture, and its features.
	
#Overview
Doradus is a REST service that extends a Cassandra NoSQL database with a
graph-based data model, advanced indexing and search features, and a REST API.
The Doradus query language (DQL) extends Lucene full-text queries with graph
navigation features such as link paths, quantifiers, and transitive searches.

#Architecture
Doradus is a pure Java application that can run as a daemon, Windows service, or
console application. The REST API is provided by an embedded Jetty server. Each
instance is a pure "peer"; multiple instances can access the same Cassandra
cluster. A common practice is to run one Doradus and one Cassandra instance on
each node. Each Doradus instance can be configured to rotate requests through
multiple Cassandra instances. Doradus currently accesses Cassandra nodes using
either the Thrift API or CQL.

#Storage Services
A Doradus database cluster can host multiple schemas, called *applications*.
Each application chooses a *storage service* to manage its data. Doradus offers
two storage services, which offer different storage and performance features to
benefit different types of applications.

##OLAP Service
The Doradus OLAP storage service is best suited for structured, immutable or
semi-mutable data such as time-oriented data: log records, events, messages,
etc. It offers very dense space storage and high-performance analytical queries.
Application data is partitioned into cubes called *shards*, which are typically
time-based (e.g., one day's data per shard). Field values are stored as arrays
that are compressed, loaded and scanned in memory, and cached on an LRU basis.
Query speed is very fast, typically millions of objects per second.

##Spider Service
The Doradus Spider service is best suited for unstructured/semi-structured data
or data that is highly mutable: document stores, message graphs, directories,
etc. It uses indexing techniques such as trie trees to support fully-inverted
tables. Spider offers fine-grained updates, immediate indexing,
table-level sharding, and other features that benefit full text applications.
   
#Data Model and Query Language
Doradus uses an object/graph model. Each application has its own schema, which
defines its tables. The accessible unit of a table is an *object*, which has a
unique *ID*. Objects can have *scalar fields*, such as text and integers, and
*link fields*, which form bidirectional relationships between objects. Doradus
ensures referential integrity of relationships. Doradus OLAP requires all tables
and fields to be predefined in the schema. Doradus Spider allows
dynamically-added tables and dynamically-defined fields per object.

The Doradus query language (DQL) supports Lucene-like full text clauses such as
*terms*, *phrases*, and *ranges*. Using link fields, DQL also supports *path
expressions*, which can use *quantifiers*, *filters*, and *transitive* searches.
DQL can be used in *object queries*, which return matching objects, and
*aggregate queries*, which perform metric calculations on selected objects,
optionally grouped with an arbitrary number of levels.

Both OLAP and Spider offer automatic data aging.

Doradus is designed to leverage the advantages of NoSQL technology. It supports
idempotent (repeatable) updates, dynamic expansion, automatic load balancing,
automatic failover, and more.

#Doradus Components
Doradus consists of following components:

- **doradus-server**: This is the Doradus server, normally compiled as
  doradus.jar. It reads config/doradus.yaml for configuration options. The
  server provides a REST API, which supports XML and JSON messages, and a JMX
  API for monitoring and administrative commands.
  
- **doradus-common**: This module consists of common classes used by both the
  client and server modules. It is normally compiled as doradus-common.jar.
  
- **doradus-client**: This is an optional module that allows Java clients to
  access a Doradus server using plain old Java objects (POJOs). It hides the
  REST API and provides basic features for connecting/reconnecting, connection
  pools, message compression, and exception handling. Requests and results are
  passed as simple Java classes.

- **regression-tests**: This folder tree contains regression tests. The tests
  are processed by the test-processor. We add new tests as new features are
  implemented and when bugs are found.

- **test-processor**: This module is the regression test processor, which
  executes the tests defined in the regression-tests folder. The main config
  file is in ./config/config.xml. The test processor's main() is located in
  com.dell.doradus.testprocessor.Program.java. See the note later on running
  regression tests.
  
#Resources
The following are the primary Doradus resources:

- **Source code**: Source code can be downloaded from this Github project:

		https://github.com/dell-oss/Doradus
       
- **Documentation**: The Ant script (build.xml) creates Java docs for the
  doradus-client module, placed in the folder ./doradus-client/docs. A set of
  PDF files describing various aspects of Doradus can be found in the following
  folder:

		https://github.com/dell-oss/Doradus/docs
    
- **Issues**: Please feel free to post bug reports and feature enhancements in
  the Github Issues area:
  
		https://github.com/dell-oss/Doradus/issues
    
- **Downloads**: Source, binary, and doc download bundles can be downloaded
  from Maven Central by searching for "Doradus". Example:
  
  		http://search.maven.org/#search%7Cga%7C1%7Cdoradus

#Requirements
Doradus requires Java 1.7 or higher and Cassandra 2.x, which must be installed
separately.

#Building Doradus
The project contains both Ant and Maven build scripts, which download
required jar files and build the client, common, and server components. (See
NOTICE.txt for a list of third party libraries used.) To build Doradus using
Ant, cd to the root `Doradus` folder and simply enter `ant`. To build using
Maven, cd to the root `Doradus` folder use enter the following commands:

	mvn clean install -DskipTests=true -Dgpg.skip=true Dmaven.javadoc.skip=true
	mvn dependency:copy-dependencies

The two build approaches create slightly different directories. Under the
`doradus-client`, `doradus-common`, and `doradus-server` folders, the Ant build
creates jar files in `lib` folders whereas Maven uses `target` folders. Also,
the Maven build includes version numbers in the Doradus jar files names whereas
the Ant build does not. Choose whichever build technique you're familiar with.

#Starting the Server

The server resides in the `doradus-server` folder. Under this folder is the
`config` folder containing the `doradus.yaml` file. An easy way to start Doradus
is to create a script within `doradus-server/bin`. If you built Doradus using
Ant, the a command line such as the following:

    java -cp "../lib/*:../config/*" com.dell.doradus.core.DoradusServer

If you built Doradus using Maven, use a command line such as the following:

	java -cp "../config:../target/classes:../target/dependency/*" com.dell.doradus.core.DoradusServer

Both examples assume Linux or MacOSX. Adjust accordingly for Windows.

Append command line arguments prefixed with a "-" to override doradus.yaml file
options. For example, doradus.yaml defines `restport: 1123`, which sets the REST
API listening port number. To override this to 5711, add the command line
argument:

    java -cp "../lib/*:../config/*" com.dell.doradus.core.DoradusServer -restport 5711
    
Doradus connects to the server(s) defined in doradus.yaml `dbhost` option. If
Cassandra is not running when the server starts, it will try connecting every 5
seconds until successful. In the mean time, REST requests that require the 
database connection will receive a 503 response.

See the [Doradus Administration document](https://github.com/dell-oss/Doradus/blob/master/docs/Doradus%20Administration.pdf) for more information about configuration
and execution options.

#Accessing Doradus
A browser can be used to access most Doradus REST API commands. For example, to
list applications that have been defined:

    http://localhost:1123/_applications
    
The Doradus Client library can be used to access a Doradus server by a Java
application. The library hides the REST API, making access a little easier. See
the Java docs in the folder ./doradus-client/doc for more information.

#Running Regression Tests

To run regression tests, first modify the file `./test-processor/config/config.xml`, if
necessary, to point to the `regression-tests` base directory. Example:

	<DEFINE name="tests.root" value="C:\Doradus\workspace\regression-tests"/>

Subfolders have names such as `bugs` and `features`. Within a subfolder, a test is defined
by the following files:

* `<name>.test.xml`: This file defines the instructions to be carried-out by the test
  processor for the test called `<name>`.

* `<name>.defs.xml`: This optional file defines schemas and input data blocks that can be
  referenced by test called `<name>`.

* `<name>.result.txt`: This file contains the expected output for the test called `<name>`.

To launch the regression test processor, run the `test-processor` module's main program
with no parameters, which is:

	com.dell.doradus.testprocessor.Program

If a test fails, the `test-processor` will create two files:

* `<name>.xresult.txt`: This file contains the actual output generated by the test.

* `<name>.xdiff.txt`: This file compares the expected and actual output and flags lines
  that are added to (+) or deleted from (-) the expected results.

The `test-processor` also creates an HTML report with the file name defined in `config.xml`
as `<report>`. This provides a quick overview of the test run, including differences for
failed tests.

You can limit the test suite to specific tests by modifying `config.xml`. For example,
to run only test `bd.010.SPIDER` in the `bugs` directory only:

	<DEFINE name="tests.root" value="C:\DELL-OSS\Doradus\regression-tests\bugs"/>
	<test-suite root="${tests.root}">
    	<include>
        	<dir path=".">
            	<test name="bd.010.SPIDER"/>
			</dir>
		</include>
	</test-suite>

#Known Issues
- With Doradus OLAP, currently there is no coordination between Doradus
  instances to serialize *merge* commands to the same shard. For now, user
  applications should send all *merge* requests to the same Doradus instance.

 
#License
Doradus is available under the Apache License Version 2.0. See LICENSE.txt or
[http://www.apache.org/licenses/](http://www.apache.org/licenses/) for a copy of this license.
