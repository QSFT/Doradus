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

import java.util.HashMap;
import java.util.Map;

import com.dell.doradus.client.Client;
import com.dell.doradus.client.SpiderSession;
import com.dell.doradus.common.AggregateResult;
import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.ContentType;
import com.dell.doradus.common.DBObjectBatch;
import com.dell.doradus.common.UNode;

/**
 * Demonstrates a Java client application accessing a Doradus Spider database. This
 * example shows application creation and data loading by parsing JSON into UNode
 * objects that are sent in REST commands. See {@link #usage()} for parameter details.
 * This application does the following:
 * <p>
 * <ol>
 * <li>Creates a {@link Client} connection to a database database.</li>
 * <li>Creates a Spider application called "HelloSpider".</li>
 * <li>Opens a {@link SpiderSession} to the "HelloSpider" application.</li>
 * <li>Inserts some sample database into a table called "Movies" about Spider-Man movies.</li>
 * <li>Computes the average budget of Spider-Man movies for which a budget is known.</li>
 * </ol>
 * <p>
 * Additional queries can be performed on the HelloSpider database via a browser. For
 * example:
 * <p>
 * <pre>
 *      // Determine the number of movies each lead actor was in:
 *      http://localhost:1123/HelloSpider/Movies/_aggregate?q=*&m=COUNT(*)&f=Leads
 *      
 *      // Find all movies scheduled to be released in the future:
 *      http://localhost:1123/HelloSpider/Movies/_query?q=ReleaseDate>NOW()
 *      
 *      // Find all movies with a lead named "Grace" and a director named "Sam"
 *      http://localhost:1123/HelloSpider/Movies/_query?q=Leads:grace+AND+Director:sam
 * </pre>
 */
public class HelloSpider {
    private static final String SCHEMA =
        "<application name='HelloSpider'>" +
            "<key>Arachnid</key>" +
            "<options>" +
                "<option name='StorageService'>SpiderService</option>" +
            "</options>" +
            "<tables>" +
                "<table name='Movies'>" +
                    "<fields>" +
                        "<field name='Name' type='text'/>" +
                        "<field name='ReleaseDate' type='timestamp'/>" +
                        "<field name='Cancelled' type='boolean'/>" +
                        "<field name='Director' type='text'/>" +
                        "<field name='Leads' type='text' collection='true'/>" +
                        "<field name='Budget' type='integer'/>" +
                    "</fields>" +
                "</table>" +
            "</tables>" +
        "</application>";

    private static final String DATA = (
        "{'batch': {" +
            "'docs': [" +
                "{'doc': {" +
                    "'_ID': 'Spidy1'," +
                    "'Name': 'Spider-Man'," +
                    "'ReleaseDate': '2002-05-03'," +
                    "'Cancelled': false," +
                    "'Director': 'Sam Raimi'," +
                    "'Leads': {'add': ['Tobey Maguire', 'Kirsten Dunst', 'Willem Dafoe']}," +
                    "'Budget': 240000000" +
                "}}," +
                "{'doc': {" +
                    "'_ID': 'Spidy2'," +
                    "'Name': 'Spider-Man 2'," +
                    "'ReleaseDate': '2004-06-04'," +
                    "'Cancelled': false," +
                    "'Director': 'Sam Raimi'," +
                    "'Leads': {'add': ['Tobey Maguire', 'Kirsten Dunst', 'Alfred Molina']}," +
                    "'Budget': 200000000" +
                "}}," +
                "{'doc': {" +
                    "'_ID': 'Spidy3'," +
                    "'Name': 'Spider-Man 3'," +
                    "'ReleaseDate': '2007-04-30'," +
                    "'Cancelled': false," +
                    "'Director': 'Sam Raimi'," +
                    "'Leads': {'add': ['Tobey Maguire', 'Kirsten Dunst', 'Topher Grace']}," +
                    "'Budget': 258000000" +
                "}}," +
                "{'doc': {" +
                    "'_ID': 'Spidy4'," +
                    "'Name': 'Spider-Man 4'," +
                    "'Cancelled': true," +
                    "'Director': 'Sam Raimi'," +
                    "'Leads': {'add': ['Tobey Maguire', 'Kirsten Dunst']}" +
                "}}," +
                "{'doc': {" +
                    "'_ID': 'Spidy5'," +
                    "'Name': 'The Amazing Spider Man'," +
                    "'ReleaseDate': '2012-06-30'," +
                    "'Cancelled': false," +
                    "'Director': 'Marc Webb'," +
                    "'Leads': {'add': ['Andrew Garfield', 'Emma Stone', 'Rhys Ifans']}," +
                    "'Budget': 230000000" +
                "}}," +
                "{'doc': {" +
                    "'_ID': 'Spidy6'," +
                    "'Name': 'The Amazing Spider Man 2'," +
                    "'ReleaseDate': '2014-05-14'," +
                    "'Cancelled': false," +
                    "'Director': 'Marc Webb'," +
                    "'Leads': {'add': ['Andrew Garfield', 'Emma Stone', 'Jamie Foxx']}" +
                "}}" +
            "]" +
        "}}").replace('\'', '"');
    
    /**
     * Run the HelloSpider application. The given args must have two values: the host name
     * and port number of the Doradus database to access. Pass "-?" to display a short
     * description of this application.
     * 
     * @param args  Host name and port number of Doradus database.
     */
    public static void main(String[] args) {
        HelloSpider app = new HelloSpider();
        app.run(args);
    }

    private void run(String[] args) {
        if (args.length != 2) {
            usage();
        }
        
        // Connect to the Doradus database, creating a client connection.
        System.out.println("Opening Doradus server: " + args[0] + ":" + args[1]);
        Client client = new Client(args[0], Integer.parseInt(args[1]));
        
        // Create the HelloSpider application definition
        ApplicationDefinition appDef = new ApplicationDefinition();
        appDef.parse(UNode.parseXML(SCHEMA));
        System.out.println("Creating application: " + appDef.getAppName());
        client.createApplication(appDef);
        
        // Create a client session for the HelloSpider application.
        SpiderSession session = (SpiderSession)client.openApplication("HelloSpider");
        client.close();
        
        // Add a batch of data to the Movies table.
        DBObjectBatch batch = new DBObjectBatch();
        batch.parse(UNode.parse(DATA, ContentType.APPLICATION_JSON));
        System.out.println("Loading batch of " + batch.getObjectCount() + " objects to 'Movies'");
        session.addBatch("Movies", batch);
        
        // Compute the average budget for movies whose budget is known.
        Map<String, String> params = new HashMap<>();
        params.put("m", "AVERAGE(Budget)");
        AggregateResult aggResult = session.aggregateQuery("Movies", params);
        System.out.println(String.format("Average movie budget: $%,d", Integer.parseInt(aggResult.getGlobalValue())));
        session.close();
    }

    private void usage() {
        System.out.println("Usage: HelloSpider host port");
        System.out.println("A sample Spider database application called 'HelloSpider' is created by");
        System.out.println("connecting to the Doradus database with the given host name and port number.");
        System.out.println("Some sample objects are inserted into a table called 'Movies'.");
        System.exit(1);
    }
    
}   // class HelloSpider
