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
package com.dell.doradus.dory;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import com.dell.doradus.client.Credentials;
import com.dell.doradus.client.RESTClient;
import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.ContentType;
import com.dell.doradus.common.HttpMethod;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.Utils;
import com.dell.doradus.dory.command.Command;

public class DoradusClient implements AutoCloseable {
        
    //constants
	private static final String _DESCRIBE_URI = "/_commands";
    
	//members
	private final RESTClient restClient;
    private String applicationName;
    private String storageService;
    private JsonObject restMetadataJson = null;
    
    /**
     * Create a new DoradusClient that will communicate with the Doradus server using the given
     * host and port. 
     * 
     * @param host  Doradus Server host name or IP address.
     * @param port  Doradus Server port number.
     */
    public DoradusClient(String host, int port) {
        this(host, port, "", null, null);   
    }
    
    /**
     * Create a new DoradusClient that will communicate with the Doradus server using the given
     * host and port and tenant credentials
     * 
     * @param host  Doradus Server host name or IP address.
     * @param port  Doradus Server port number.
     * @param credentials  Credentials for use with a Doradus application.
     */   
    public DoradusClient(String host, int port, Credentials credentials) {
        this(host, port, "", credentials, null);            
    }
    
    /**
     * Static factory method to open a 'dory' client session
     * @param host Doradus Server host name or IP address.
     * @param port Doradus Server port number.
     * @param applicationName
     * @return the instance of the DoradusClient session
     */
    public static DoradusClient open(String host, int port, Credentials credentials, String applicationName) {  
        DoradusClient doradusClient = new DoradusClient(host, port, null, credentials, applicationName);    
        doradusClient.setCredentials(credentials);
        String storageService = lookupStorageServiceByApp(doradusClient.getRestClient(), applicationName);
        doradusClient.setStorageService(storageService);
        return doradusClient;
    }
    
    /**
     * Set the given application name as context for all future commands. This method can
     * be used when an application name was not given in a constructor.
     * 
     * @param appName   Application name that is used for application-specific commands
     *                  called from this point forward. 
     */
    public void setApplication(String appName) {
        applicationName = appName;
        String storageService = lookupStorageServiceByApp(getRestClient(), applicationName);
        setStorageService(storageService);
    }
    
    /**
     * Set credentials for use with a Doradus application.
     * @param credentials
     */
    public void setCredentials(Credentials credentials) {
        restClient.setCredentials(credentials);
    }   
    
    /**
     * Set credentials such as tenant, username, password for use with a Doradus application.
     * @param tenant tenant name
     * @param username user name use when accessing applications within the specified tenant.
     * @param userpassword user password
     */
    public void setCredentials(String tenant, String username, String userpassword) {
        Credentials credentials = new Credentials(tenant, username, userpassword);
        restClient.setCredentials(credentials);
    }
    
    /**
     * set JSON Object that contains all REST commands descriptions
     * @param restMetadataJson
     */
    public void setRestMetadataJson(JsonObject restMetadataJson) {
        this.restMetadataJson = restMetadataJson;
    }
    
    /**
     * Retrieve the map of commands keyed by service name
     * @return map of commands 
     */
    public Map<String, List<String>> listCommands() {
        Map<String, List<String>> result = new HashMap<String, List<String>>();
        for (String cat : restMetadataJson.keySet())  {
            JsonObject commands = restMetadataJson.getJsonObject(cat);
            List<String> names = new ArrayList<String>();
            for (String commandName: commands.keySet()) {
                names.add(commandName);
            }       
            result.put(cat, names);
        }
        return result;
    }   
    
    
    /**
     * Describe command that helps give the idea what client needs to build the command
     * @param service service name such as 'SpiderService' for application commands or '_systems' for system commands
     * @param command command name
     * @return JsonObject result in JSON that contains the description of the command
     */
    public JsonObject describeCommand(String service, String command) {
        return Command.matchCommand(restMetadataJson, command, service);
        
    }
    
    /**
     * Execute any client command after it gets built properly with all required param and their values
     * @param command the command to execute
     * @return RESTResponse result object
     * 
     */
    public RESTResponse runCommand(Command command)  {
        if (this.applicationName != null) {
            command.setApplicationName(this.applicationName);
        }
        if (this.storageService != null) {
            command.setStorageService(this.storageService);
        }       
        if (this.restMetadataJson != null) {
            command.setRestMetadataJson(this.restMetadataJson);
        }
        command.validate(restClient);
        return command.call(restClient);        
    }
       

    /**
     * Close the client
     */
    @Override
    public void close() {
        if (this.restClient != null) {
            this.restClient.close();
        }
    }   
    
    /**
     * private constructor
     * @param host
     * @param port
     * @param applicationName
     */
    private DoradusClient(String host, int port, String apiPrefix, Credentials credentials, String applicationName) {
        this.restClient = new RESTClient(host, port, "");
        this.restClient.setCredentials(credentials);
        this.applicationName = applicationName;
        this.storageService = null;
        loadRESTRulesIfNotExist(restClient);
    }


    /**
     * Convenient method to lookup storageService of the application
     * @param restClient
     * @param applicationName
     * @return storage service name
     */
    private static String lookupStorageServiceByApp(RESTClient restClient, String applicationName) {
        Utils.require(applicationName != null, "Missing application name");
        if (applicationName != null) {
            ApplicationDefinition appDef =  Command.getAppDef(restClient, applicationName);
            Utils.require(appDef != null, "Unknown application: %s", applicationName);
            return appDef.getStorageService();
        }
        return null;
    }
    
    /**
     * set storage service
     * @param storageService
     */
    private void setStorageService(String storageService) {
        this.storageService = storageService;
        
    }

    /**
     * Load RESTRules once
     * @param restClient
     */
    private synchronized void loadRESTRulesIfNotExist(RESTClient restClient)  {     
        if (restMetadataJson == null || restMetadataJson.isEmpty()) {
            restMetadataJson = loadRESTCommandsFromServer(restClient);
        }       
    }
    
 
    /**
     * Load REST commands by calling describe command
     * @param restClient
     * @return JsonObject that contains all the descriptions of all REST commands
     */
    private JsonObject loadRESTCommandsFromServer(RESTClient restClient) {
        RESTResponse response = null;
        try {
            response = restClient.sendRequest(HttpMethod.GET, _DESCRIBE_URI, ContentType.APPLICATION_JSON, null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (!response.getCode().isError()) {
            JsonReader jsonReader = Json.createReader(new StringReader(response.getBody()));            
            JsonObject result = jsonReader.readObject().getJsonObject("commands");
            jsonReader.close();
            return result;
        }
        else {
            throw new RuntimeException("Describe command error: " + response.getBody());
        }
    }
    
    /**
     * Returns the RESTClient object
     * @return
     */
    private RESTClient getRestClient() {
        return restClient;
    }    
    
}
