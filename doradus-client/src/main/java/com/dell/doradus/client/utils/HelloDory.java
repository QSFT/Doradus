/*
 * Copyright (C) 2014 Dell, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dell.doradus.client.utils;

import com.dell.doradus.common.AggregateResult;
import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.DBObject;
import com.dell.doradus.common.DBObjectBatch;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.FieldType;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.UNode;
import com.dell.doradus.dory.DoradusClient;
import com.dell.doradus.dory.command.Command;

/**
 * Demonstrates a Java client application accessing a Doradus Spider database. This
 * example uses the "Dory" client, which creates objects using the builder pattern.
 * <p>
 * This example also uses a different schema than HelloSpider, separating actors into
 * their own table which are connected to movies via links. See {@link #usage()} for
 * parameter details. This application does the following:
 * <p>
 * <ol>
 * <li>Creates a {@link DoradusClient} connection to a database database.</li>
 * <li>Creates a Spider application called "HelloSpider".</li>
 * <li>Inserts some sample database into a table called "Movies" about Spider-Man movies.</li>
 * <li>Inserts actor objects referenced by movies into a table called "Actors".</li>
 * <li>Computes the average budget of Spider-Man movies in which Kirsten Dunst acted.</li>
 * </ol>
 * <p>
 * Additional queries can be performed on the HelloSpider database via a browser. For
 * example:
 * <p>
 * <pre>
 *      // Determine the number of movies each lead actor was in:
 *      http://localhost:1123/HelloSpider/Movies/_aggregate?q=*&m=COUNT(*)&f=Leads.LastName
 *      
 *      // Find all movies released in or after the year 2014
 *      http://localhost:1123/HelloSpider/Movies/_query?q=ReleaseDate>2014
 *      
 *      // Find all movies with a lead with last name "Grace" and a director named "Sam"
 *      http://localhost:1123/HelloSpider/Movies/_query?q=Leads.LastName:grace+AND+Director:sam&f=_all
 * </pre>
 * Note that Dory is a "generic" client, designed to work with any application and any
 * storage service. As a result, it uses the REST "describe" command to discover what REST
 * commands are currently available. Each command has an owner ("_system" or a storage
 * service) and a unique name (e.g., "Add", "Aggregate"). The Dory client requires that
 * you provide the name of each command in order to execute it. You can see the current
 * commands and their names by using the following URI:
 * <pre>
 *      GET /_commands
 * </pre>
 */
public class HelloDory {
    /**
     * Run the HelloSpider application. The given args must have two values: the host name
     * and port number of the Doradus database to access. Pass "-?" to display a short
     * description of this application.
     * 
     * @param args  Host name and port number of Doradus database.
     */
    public static void main(String[] args) {
        HelloDory app = new HelloDory();
        app.run(args);
    }
    
    // Create a Dory client connection and execute the example commands.
    private void run(String[] args) {
        if (args.length != 2) {
            usage();
        }
        
        System.out.println("Opening Doradus server: " + args[0] + ":" + args[1]);
        try (DoradusClient client = new DoradusClient(args[0], Integer.parseInt(args[1]))) {
            deleteApplication(client);
            createApplication(client);
            addData(client);
            queryData(client);
        }
    }

    // Delete the existing HelloSpider application if present.
    private void deleteApplication(DoradusClient client) {
        Command command = Command.builder()
            .withName("DeleteAppWithKey")
            .withParam("application", "HelloSpider")
            .withParam("key", "Arachnid")
            .build();
        client.runCommand(command); // Ignore response
    }
    
    // Create the HelloSpider application definition.
    private void createApplication(DoradusClient client) {
        ApplicationDefinition appDef = ApplicationDefinition.builder()
            .withName("HelloSpider")
            .withKey("Arachnid")
            .withOption("StorageService", "SpiderService")
            .withTable(TableDefinition.builder()
                .withName("Movies")
                .withField(FieldDefinition.builder().withName("Name").withType(FieldType.TEXT).build())
                .withField(FieldDefinition.builder().withName("ReleaseDate").withType(FieldType.TIMESTAMP).build())
                .withField(FieldDefinition.builder().withName("Cancelled").withType(FieldType.BOOLEAN).build())
                .withField(FieldDefinition.builder().withName("Director").withType(FieldType.TEXT).build())
                .withField(FieldDefinition.builder().withName("Leads").withType(FieldType.LINK).withExtent("Actors").withInverse("ActedIn").build())
                .withField(FieldDefinition.builder().withName("Budget").withType(FieldType.INTEGER).build())
                .build()
            )
            .withTable(TableDefinition.builder()
                .withName("Actors")
                .withField(FieldDefinition.builder().withName("FirstName").withType(FieldType.TEXT).build())
                .withField(FieldDefinition.builder().withName("LastName").withType(FieldType.TEXT).build())
                .withField(FieldDefinition.builder().withName("ActedIn").withType(FieldType.LINK).withExtent("Movies").withInverse("Leads").build())
                .build()
            ).build();
        Command command = Command.builder()
            .withName("DefineApp")
            .withParam("ApplicationDefinition", appDef)
            .build();
        RESTResponse response = client.runCommand(command);
        if (response.isFailed()) {
            throw new RuntimeException("DefineApp failed: " + response);
        }
    }

    // Add data to Movies and Actors tables.
    private void addData(DoradusClient client) {
        // Now that the application is created, set the application name in the session so
        // we don't have to call .withParam("application", "HelloSpider") for each command.
        client.setApplication("HelloSpider");
        
        // Add a batch of Movies, including links to Actors not yet inserted
        DBObjectBatch dbObjBatch = DBObjectBatch.builder()
            .withObject(DBObject.builder()
                .withID("Spidy1")
                .withValue("Name", "Spider-Man")
                .withValue("ReleaseDate", "2002-05-03")
                .withValue("Cancelled", false)
                .withValue("Director", "Sam Raimi")
                .withValues("Leads", "TMaguire", "KDunst", "WDafoe")
                .withValue("Budget", 240000000)
                .build()
            )
            .withObject(DBObject.builder()
                .withID("Spidy2")
                .withValue("Name", "Spider-Man 2")
                .withValue("ReleaseDate", "2004-06-04")
                .withValue("Cancelled", false)
                .withValue("Director", "Sam Raimi")
                .withValues("Leads", "TMaguire", "KDunst", "AMolina")
                .withValue("Budget", 200000000)
                .build()
            )
            .withObject(DBObject.builder()
                .withID("Spidy3")
                .withValue("Name", "Spider-Man 3")
                .withValue("ReleaseDate", "2007-04-30")
                .withValue("Cancelled", false)
                .withValue("Director", "Sam Raimi")
                .withValues("Leads", "TMaguire", "KDunst", "TGrace")
                .withValue("Budget", 258000000)
                .build()
            )
            .withObject(DBObject.builder()
                .withID("Spidy4")
                .withValue("Name", "Spider-Man 4")
                .withValue("Cancelled", true)
                .withValue("Director", "Sam Raimi")
                .withValues("Leads", "TMaguire", "KDunst")
                .build()
            )
            .withObject(DBObject.builder()
                .withID("Spidy5")
                .withValue("Name", "The Amazing Spider Man")
                .withValue("ReleaseDate", "2012-06-30")
                .withValue("Cancelled", false)
                .withValue("Director", "Marc Webb")
                .withValues("Leads", "AGarfield", "EStone", "RIfans")
                .withValue("Budget", 230000000)
                .build()
            )
            .withObject(DBObject.builder()
                .withID("Spidy6")
                .withValue("Name", "The Amazing Spider Man 2")
                .withValue("ReleaseDate", "2014-05-14")
                .withValue("Cancelled", false)
                .withValue("Director", "Marc Webb")
                .withValues("Leads", "AGarfield", "EStone", "JFoxx")
                .withValue("Budget", 230000000)
                .build()
            )
            .build();
        Command command = Command.builder()
            .withName("Add")
            .withParam("table", "Movies")
            .withParam("batch", dbObjBatch)
            .build();
        RESTResponse response = client.runCommand(command);
        if (response.isFailed()) {
            throw new RuntimeException("Add batch failed: " + response.getBody());
        }

        // Add a batch of Actors. Note that we don't have to set link inverses
        // because they are automatically inferred from the Movies batch.
        dbObjBatch = DBObjectBatch.builder()
            .withObject(DBObject.builder().withID("TMaguire").withValue("FirstName", "Tobey").withValue("LastName", "Maguire").build())
            .withObject(DBObject.builder().withID("KDunst").withValue("FirstName", "Kirsten").withValue("LastName", "Dunst").build())
            .withObject(DBObject.builder().withID("WDafoe").withValue("FirstName", "Willem").withValue("LastName", "Dafoe").build())
            .withObject(DBObject.builder().withID("AMolina").withValue("FirstName", "Alfred").withValue("LastName", "Molina").build())
            .withObject(DBObject.builder().withID("TGrace").withValue("FirstName", "Topher").withValue("LastName", "Grace").build())
            .withObject(DBObject.builder().withID("AGarfield").withValue("FirstName", "Andrew").withValue("LastName", "Garfield").build())
            .withObject(DBObject.builder().withID("EStone").withValue("FirstName", "Emma").withValue("LastName", "Stone").build())
            .withObject(DBObject.builder().withID("RIfans").withValue("FirstName", "Rhys").withValue("LastName", "Ifans").build())
            .withObject(DBObject.builder().withID("JFoxx").withValue("FirstName", "Jamie").withValue("LastName", "Foxx").build())
            .build();
        command = Command.builder()
            .withName("Add")
            .withParam("table", "Actors")
            .withParam("batch", dbObjBatch)
            .build();
        response = client.runCommand(command);
        if (response.isFailed()) {
            throw new RuntimeException("Add batch failed: " + response.getBody());
        }
    }
    
    // Compute the average budget for movies in which last name "Dunst" was a lead.
    // The "Aggregate" command uses different parameter names that the URI-based
    // command. The equivalent URI query is:
    //      GET /HelloSpider/Movies/_aggregate?m=AVERAGE(Budget)&q=Leads.LastName=Dunst
    private void queryData(DoradusClient client) {
        Command command = Command.builder()
            .withName("Aggregate")
            .withParam("table", "Movies")
            .withParam("metric", "AVERAGE(Budget)")
            .withParam("query", "Leads.LastName=Dunst")
            .build();
        RESTResponse response = client.runCommand(command);
        if (response.isFailed()) {
            throw new RuntimeException("Aggregate query failed: " + response.getBody());
        }
        AggregateResult aggResult = new AggregateResult();
        aggResult.parse(UNode.parseJSON(response.getBody()));
        System.out.println(String.format("Average budget of movies with Kirsten Dunst: $%,.2f",
                                         Double.parseDouble(aggResult.getGlobalValue())));
    }

    // Display usage information and exit.
    private void usage() {
        System.out.println("Usage: HelloDory host port");
        System.out.println("A sample Spider database application called 'HelloSpider' is created by");
        System.out.println("connecting to the Doradus database with the given host name and port number.");
        System.out.println("Some sample objects are inserted into tables called 'Movies' and 'Actors'");
        System.out.println("and an example aggregate query is performed.");
        System.exit(1);
    }
    
}   // class HelloDory
