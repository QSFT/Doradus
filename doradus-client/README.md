Doradus "dory" Client
========================
  
A lightweight generic command based Java Client that allows any java client to access Doradus server using Command Builder.

### Features

* Requires no update on the Client as the time it connects to the server which can be updated with new REST APIs or DB services.
* Validates the requirements of the command such as name or parameters
* Inquires the command description that contains the required parameters that would be helpful for building the command correctly
* Connects to as many as Doradus server as you want in any non-tenant/multi-tenant modes and opens a session for a specific app to run sequential commands


[More Usage Samples in Unit Tests]
(https://github.com/dell-oss/Doradus/blob/master/doradus-client/src/test/java/com/dell/doradus/dory/DoradusClientTest.java)

For examples,

- Inquire
   
```java
    JsonObject jsonResult = client.describeCommand("SpiderService", "Add"); 
```   
In turn, it returns the description of the command. This will help give the client the idea to to build the Add command correctly with application params such as application, table and batch object
```json
    {
    "summary": "Adds a batch of updates for a specific application and table. The batch can contain new and updated objects.",
    "uri": "/{application}/{table}",
    "methods": "POST",
    "input-entity": "batch",
    "output-entity": "batch-result"
    }
```

- Build and run the command

```java
	DBObject dbObject1 = DBObject.builder().add("Subject", "Hello").add("Body", "Hello there!").build();       
    DBObject dbObject2 = DBObject.builder().add("Subject", "Bye").add("Body", "Good bye!").build();
    DBObjectBatch dbObjectBatch =  DBObjectBatch.builder().add(dbObject1).add(dbObject2).build();
    RESTResponse response = client.runCommand(Command.builder().withName("Add").withParam("table","Messages").withParam("batch", dbObjectBatch).build());
```
