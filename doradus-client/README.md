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

- Schema commands

```java
    //create client instance with server connection info
    Credentials credentials = new Credentials(`TENANT_NAME`, `USER_NAME`, `USER_PASSWORD`);         
    DoradusClient client = new DoradusClient(HOST, PORT, credentials);
    
    //create Spider application
    ApplicationDefinition appDef1 = new ApplicationDefinition();
    appDef1.setAppName("Stuff");
    client.runCommand(Command.builder().withName("DefineApp").withParam("ApplicationDefinition", appDef1).build());
    
    //create OLAP application
    ApplicationDefinition appDef = new ApplicationDefinition();
    appDef.parse(UNode.parseJSON(`JSON_SCHEMA`));
    client.runCommand(Command.builder().withName("DefineApp").withParam("ApplicationDefinition", appDef).build());
```   

- Data commands

```java
   //open client session for subsequent commands on the same Spider application
   DoradusClient client = DoradusClient.open(HOST, PORT, credentials, "Stuff");
   
   //add data operation
   DBObject dbObject1 = DBObject.builder().add("Subject", "Hello").add("Body", "Hello there!").build();        
   DBObject dbObject2 = DBObject.builder().add("Subject", "Bye").add("Body", "Good bye!").build();
            
   DBObjectBatch dbObjectBatch =  DBObjectBatch.builder().add(dbObject1).add(dbObject2).build();
   client.runCommand(Command.builder().withName("Add").withParam("table","Messages").withParam("batch", dbObjectBatch).build());
         
   //retrieve data operation
   Command command = Command.builder().withName("Query")
                                        .withParam("table", "Messages")             
                                        .withParam("query", "*")
                                        .withParam("fields", "Body")
                                        .withParam("size", "10")
                                        .build();
   client.runCommand(command2);
```     