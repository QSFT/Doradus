/*
 * DELL PROPRIETARY INFORMATION
 * 
 * This software is confidential.  Dell Inc., or one of its subsidiaries, has
 * supplied this software to you under the terms of a license agreement,
 * nondisclosure agreement or both.  You may not copy, disclose, or use this 
 * software except in accordance with those terms.
 * 
 * Copyright 2014 Dell Inc.  
 * ALL RIGHTS RESERVED.
 * 
 * DELL INC. MAKES NO REPRESENTATIONS OR WARRANTIES
 * ABOUT THE SUITABILITY OF THE SOFTWARE, EITHER EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE, OR NON-INFRINGEMENT. DELL SHALL
 * NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE
 * AS A RESULT OF USING, MODIFYING OR DISTRIBUTING
 * THIS SOFTWARE OR ITS DERIVATIVES.
 */

package com.dell.doradus.dory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import javax.json.JsonObject;

import org.junit.Ignore;
import org.junit.Test;

import com.dell.doradus.client.Credentials;
import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.DBObject;
import com.dell.doradus.common.DBObjectBatch;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.TenantDefinition;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.UserDefinition;
import com.dell.doradus.dory.command.Command;


/**
 * Doradus 'dory' client unit tests as examples of how to use the client APIs for all CRUDs operations on Doradus.
 * 
 * NOTES: Make sure to modify the HOST/PORT below to the running Doradus server instance 
 *
 */
@Ignore
public class DoradusClientTest {
	
    private static final String HOST = "localhost";
    private static final int PORT = 1111;
	private static final String OLAP_SCHEMA_FILE = "OLAPSchema.json";

    @Test
    public void testRequiredCommandName() throws Exception {
    	DoradusClient client = new DoradusClient(HOST, PORT);
    	client.setCredentials("HelloKitty", "Katniss", "Everdeen");

    	//no commandName    	
    	try {
    		client.runCommand(Command.builder().withParam("table", "Messages").build());
    		fail("should throw exception");
    	}
    	catch (Exception e) {
    		assertTrue(e.getMessage().equals("missing command name"));
    	}   	
    	
    	//unknown commandName
    	try {
    		client.runCommand(Command.builder().withName("foo").withParam("table", "Messages").build());
    		fail("should throw exception");
    	}
    	catch (Exception e) {
    		assertTrue(e.getMessage().equals("unsupported command name: foo"));
    	}   
    	client.close();      	
    }
    
    @Test
    public void testRequiredParams() throws Exception {
    	Credentials credentials = new Credentials("HelloKitty", "Katniss", "Everdeen");
    	DoradusClient client = new DoradusClient(HOST, PORT, credentials);
    	
    	//application required for "Define"
    	try {
    		client.runCommand(Command.builder().withName("DefineApp").build());
    		fail("should throw exception");
    	}
    	catch (Exception e) {
    		assertTrue(e.getMessage().equals("missing param: ApplicationDefinition"));
    	}   	
    	client.close();
      	
      	//open session with unknown app
    	try {
         	client = DoradusClient.open(HOST, PORT, credentials, "Foo");
      		client.runCommand(Command.builder().withName("Add").build());
      		fail("should throw exception");
    	}
    	catch (Exception e) {
    		assertTrue(e.getMessage().equals("Unknown application: Foo"));
    	}   
    	
    	createSpiderApp("MyApp");
    	
      	//open session with existing app
    	client = DoradusClient.open(HOST, PORT, credentials, "MyApp");
    	
    	//"batch" param required for "Add" 
    	try {
      		client.runCommand(Command.builder().withName("Add").build());
      		fail("should throw exception");
    	}
    	catch (Exception e) {
    		assertTrue(e.getMessage().equals("missing param: batch"));
    	}  
    	
    	//"table" param required for "Add" command
    	try {        
    	   	DBObject dbObject1 = DBObject.builder().add("Subject", "Hello").add("Body", "Hello there!").build();      	
    	  	DBObject dbObject2 = DBObject.builder().add("Subject", "Bye").add("Body", "Good bye!").build();   			
    	 	DBObjectBatch dbObjectBatch =  DBObjectBatch.builder().add(dbObject1).add(dbObject2).build();
    	 	
    	 	client.runCommand(Command.builder().withName("Add").withParam("batch", dbObjectBatch).build());
    		fail("should throw exception");
    	}
    	catch (Exception e) {
    		assertTrue(e.getMessage().equals("missing param: table"));
    	} 
    	
    	//'query' param required for "Query" command
    	try {     
			client.runCommand(Command.builder().withName("Query")
					.withParam("table", "Messages")				
					.withParam("fields", "Body")
					.withParam("size", "10").build());
			fail("should throw exception");
    	}
    	catch (Exception e) {
    		assertTrue(e.getMessage().equals("missing param: query"));
    	} 		
    	client.close();
    }   
    
    @Test
    public void testLogsSystemCommand() throws Exception {
       
      	Credentials superUserCredentials = new Credentials(null, "cassandra", "cassandra");          	
    	DoradusClient client = new DoradusClient(HOST, PORT, superUserCredentials);   	
    	
    	//test retrieve the map of commands by service name 
    	Map<String, List<String>> commands = client.listCommands();
    	assertTrue(commands.get("_system").contains("Logs"));
    	
    	//test retrieve Logs
    	RESTResponse response = client.runCommand(Command.builder().withName("Logs").withParam("level", "INFO").build());
		assertTrue(response.getCode().getCode() == 200);
    	client.close();  	
    	
    }

    @Test
    public void testTenantManagmentCommands() throws Exception {
    	Credentials credentials = new Credentials(null, "cassandra", "cassandra");    	
    	DoradusClient client = new DoradusClient(HOST, PORT, credentials);
    	
    	//test ListTenant
    	RESTResponse response = client.runCommand(Command.builder().withName("ListTenant").withParam("tenant", "HelloKitty").build());
    	if (response.getCode().getCode() == 200) {
        	//test DeleteTenant
        	response = client.runCommand(Command.builder().withName("DeleteTenant").withParam("tenant", "HelloKitty").build());   		
    	}
   		//test DefineTenant
		TenantDefinition tenantDef = new TenantDefinition();
		tenantDef.setName("HelloKitty");
		UserDefinition userDef = new UserDefinition("Katniss");
		userDef.setPassword("Everdeen");
		tenantDef.addUser(userDef);
		response = client.runCommand(Command.builder().withName("DefineTenant").withParam("TenantDefinition", tenantDef).build());
		assertTrue(response.getCode().getCode() == 200); 
		assertTrue(response.getBody().contains("HelloKitty"));
	
    	client.close();
    }
    
    @Test
    public void testSpiderSchemaSystemCommands() throws Exception {
    	Credentials credentials = new Credentials("HelloKitty", "Katniss", "Everdeen");      	
    	DoradusClient client = new DoradusClient(HOST, PORT, credentials);
    	
    	//test retrieve the map of commands by service name 
    	Map<String, List<String>> commands = client.listCommands();
    	assertTrue(commands.get("_system").contains("ListApps"));
    	
    	//test get description of the command before building. This will help give the idea that client needs to build the DeleteApp command with application param, for ex
    	JsonObject jsonResult = client.describeCommand("_system", "DeleteApp"); 
    	assertTrue(jsonResult.getString("uri").equals("/_applications/{application}"));
    	
    	//test find and DeleteApp
    	RESTResponse response = client.runCommand(Command.builder().withName("ListApps").build());
    	if (response.getBody().contains("Stuff1")) {   		
    		RESTResponse response1 = client.runCommand(Command.builder().withName("DeleteApp").withParam("application", "Stuff1").build());
    		assertTrue(response1.getCode().getCode() == 200);    
    	}
    	if (response.getBody().contains("Stuff2")) {   		
    		RESTResponse response2 = client.runCommand(Command.builder().withName("DeleteApp").withParam("application", "Stuff2").build());
    		assertTrue(response2.getCode().getCode() == 200);    
    	}  
		response = client.runCommand(Command.builder().withName("ListApps").build());
		assertFalse(response.getBody().contains("Stuff1")); 
		assertFalse(response.getBody().contains("Stuff2")); 	
		
		//test create default Spider app  
		ApplicationDefinition appDef1 = new ApplicationDefinition();
		appDef1.setAppName("Stuff1");
		response = client.runCommand(Command.builder().withName("DefineApp").withParam("ApplicationDefinition", appDef1).build());
		assertTrue(response.getCode().getCode() == 200);
		
		//test create another OPLAP application
	   	ApplicationDefinition appDef2 = new ApplicationDefinition();
	   	appDef2.setAppName("Stuff2");
	   	appDef2.setOption("StorageService", "OLAPService");
	   	response = client.runCommand(Command.builder().withName("DefineApp").withParam("ApplicationDefinition", appDef2).build());	
		assertTrue(response.getCode().getCode() == 200);
		
		//test list apps
		response = client.runCommand(Command.builder().withName("ListApps").build());
		assertTrue(response.getCode().getCode() == 200);
		assertTrue(response.getBody().contains("Stuff1")); 
		assertTrue(response.getBody().contains("Stuff2")); 		
		client.close();
    }
    
    @Test
    public void testSpiderDataServiceCommands() throws Exception { 
    	
     	Credentials credentials = createSpiderApp("HelloByeApp");

      	//open session with existing app
     	DoradusClient client = DoradusClient.open(HOST, PORT, credentials, "HelloByeApp");
    
     	//test retrieve the map of commands by service name 
    	Map<String, List<String>> commands = client.listCommands();
    	assertTrue(commands.get("SpiderService").contains("Add"));
    	
    	//test get description of the command before building. This will help give the idea that client needs to build the Add command with application params, for ex
    	JsonObject jsonResult = client.describeCommand("SpiderService", "Add"); 
    	assertTrue(jsonResult.getString("uri").equals("/{application}/{table}"));
    	assertTrue(jsonResult.getString("input-entity").equals("batch"));

      	//test add data 
	   	DBObject dbObject1 = DBObject.builder().add("Subject", "Hello").add("Body", "Hello there!").build();      	
	  	DBObject dbObject2 = DBObject.builder().add("Subject", "Bye").add("Body", "Good bye!").build();
			
	 	DBObjectBatch dbObjectBatch =  DBObjectBatch.builder().add(dbObject1).add(dbObject2).build();
	 	RESTResponse response = client.runCommand(Command.builder().withName("Add").withParam("table","Messages").withParam("batch", dbObjectBatch).build());
   	 	
		assertTrue(response.getCode().getCode() == 201);
   	 	
		//to query data on the same application: 
		Command command2 = Command.builder().withName("Query")
								.withParam("table", "Messages")				
								.withParam("query", "*")
								.withParam("fields", "Body")
								.withParam("size", "10")
								.build();
		RESTResponse response2 = client.runCommand(command2);
		assertTrue(response2.getCode().getCode() == 200);
    	assertTrue(response2.getBody().contains("{\"results\":{\"docs\":[{\"doc\":{\"Body\":\"Hello there!"));
    	
    	client.close();
    }

    
    @Test
    public void testOLAPSchemaSystemCommands() throws Exception {
    	Credentials credentials = new Credentials("HelloKitty", "Katniss", "Everdeen");      	
    	DoradusClient client = new DoradusClient(HOST, PORT, credentials);
    	
    	//test find and DeleteApp
    	RESTResponse response = client.runCommand(Command.builder().withName("ListApps").build());
    	if (response.getBody().contains("Email")) {   		
    		RESTResponse response1 = client.runCommand(Command.builder().withName("DeleteApp").withParam("application", "Email").build());
    		assertTrue(response1.getCode().getCode() == 200);    
    	}  	
    	//test create OLAP app 
     	ApplicationDefinition appDef = new ApplicationDefinition();
     	appDef.parse(UNode.parseJSON(getOLAPSchemaJson()));
     	client.runCommand(Command.builder().withName("DefineApp").withParam("ApplicationDefinition", appDef).build());
     	
    	//test list app
		response = client.runCommand(Command.builder().withName("ListApp").withParam("application", "Email").build());
		assertTrue(response.getCode().getCode() == 200);
		client.close();   
    }	
 
    @Test
    public void testOLAPDataServiceCommands() throws Exception { 
    	
    	Credentials credentials = new Credentials("HelloKitty", "Katniss", "Everdeen");      	
		createOLAPApp(credentials);
     	
      	//open session with OLAP app
		DoradusClient client = DoradusClient.open(HOST, PORT, credentials, "Email");
 
    	//test get description of the command before building. This will help give the idea that client needs to build the Add command with application params, for ex
    	JsonObject jsonResult = client.describeCommand("OLAPService", "Update"); 
    	assertTrue(jsonResult.getString("uri").equals("/{application}/{shard}?{params}"));
    	assertTrue(jsonResult.getString("input-entity").equals("batch"));

      	//test add data 
	   	DBObject dbObject1 = DBObject.builder().add("SendDate", "2010-07-17 15:21:12").add("Size", "1254").add("Subject", "Today message").add("Tags", "AfterHours").build();      	
	   	dbObject1.setTableName("Message");
	   	
	 	DBObjectBatch dbObjectBatch =  DBObjectBatch.builder().add(dbObject1).build();
	 	RESTResponse response = client.runCommand(Command.builder().withName("Update").withParam("shard","s1").withParam("overwrite", "true").withParam("batch", dbObjectBatch).build());
   	 	
	 	//test merge shard
	 	response = client.runCommand(Command.builder().withName("Merge").withParam("shard","s1").withParam("force-merge", "true").build());	 	
		assertTrue(response.getCode().getCode() == 200);  
		
		//test query
		Command command2 = Command.builder().withName("Query")
				.withParam("table","Message")
				.withParam("shards", "s1")				
				.withParam("query", "*")
				.withParam("fields", "SendDate,Subject")
				.withParam("size", "10")
				.build();
		RESTResponse response2 = client.runCommand(command2);
		assertTrue(response2.getCode().getCode() == 200);
		assertTrue(response2.getBody().contains("Today message"));
    }

	private void createOLAPApp(Credentials credentials) throws IOException,
			URISyntaxException, Exception {
		DoradusClient client = new DoradusClient(HOST, PORT, credentials);
    	
		//delete "Email" app if it does exist
    	RESTResponse response = client.runCommand(Command.builder().withName("ListApps").build());
    	if (response.getBody().contains("Email")) {   		
    		RESTResponse response1 = client.runCommand(Command.builder().withName("DeleteApp").withParam("application", "Email").build());
    		assertTrue(response1.getCode().getCode() == 200);    
    	}    	
    	//create OLAP app 
     	ApplicationDefinition appDef = new ApplicationDefinition();
     	appDef.parse(UNode.parseJSON(getOLAPSchemaJson()));
     	client.runCommand(Command.builder().withName("DefineApp").withParam("ApplicationDefinition", appDef).build());
     	client.close();
	}
    
	private Credentials createSpiderApp(String app) throws IOException, Exception {
		Credentials credentials = new Credentials("HelloKitty", "Katniss", "Everdeen");
     	DoradusClient client = new DoradusClient(HOST, PORT, credentials);
     	ApplicationDefinition appDef = new ApplicationDefinition();
     	appDef.setAppName(app);
     	client.runCommand(Command.builder().withName("DefineApp").withParam("ApplicationDefinition", appDef).build());
     	client.close();
		return credentials;
	}
    
    private static String getOLAPSchemaJson() throws IOException, URISyntaxException {
    	return new String(Files.readAllBytes(Paths.get(DoradusClientTest.class.getResource("/"+OLAP_SCHEMA_FILE).toURI())));
    }
}   // class DoradusClientTest
