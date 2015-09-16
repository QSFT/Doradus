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
package com.dell.doradus.dory.command;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.client.RESTClient;
import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.ContentType;
import com.dell.doradus.common.HttpCode;
import com.dell.doradus.common.HttpMethod;
import com.dell.doradus.common.JSONable;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;

public class Command {
        
	//constants
    private static final String STORAGE_SERVICE = "storageservice";
	private static final String APPLICATION = "application";
	private static final String PARAMS = "params";
	private static final String METHODS = "methods";
	private static final String URI = "uri";
	private static final String PARAMETERS = "parameters";
	private static final String INPUT_ENTITY = "input-entity";
	private static final String _SYSTEM = "_system";
	private static final String _REQUIRED = "_required";
	
	//members
	private String commandName;
    private Map<String, Object> commandParams = new HashMap<>();    
    private JsonObject metadataJson;
    private boolean compound = false;   
    protected JsonObject restMetadataJson = null;
    
    protected static final Logger logger = LoggerFactory.getLogger(Command.class.getSimpleName());
    
    //setters
    public void setName(String name) {
        this.commandName = name;        
    }
    public void setRestMetadataJson(JsonObject restMetadataJson) {
        this.restMetadataJson = restMetadataJson;
    }
    
    public void addParam(String name, Object value) {
        commandParams.put(name, value); 
    }   
    
    public void setMetadataJson(JsonObject metadataJson) {
        this.metadataJson = metadataJson;       
    }
    
    public void setApplicationName(String applicationName) {
        commandParams.put(APPLICATION, applicationName);      
    }
    
    public void setStorageService(String storageService) {
        commandParams.put(STORAGE_SERVICE, storageService);            
    }
    
    public void setCompound(boolean compound) {
        this.compound = compound;
        
    }
    //getters
    public Map<String, Object> getParams() {
        return commandParams;
    }
    
    public String getCommandName() {
        return commandName;
    }

    public boolean isCompound() {
        return this.compound;
    }
    
    /**
     * Call method implementation
     * @param restClient
     * @return RESTResponse result object
     */
    public RESTResponse call(RESTClient restClient){        
        
        String methodName = getMethod();
        String uri = getURI();
        StringBuilder uriBuilder = new StringBuilder(Utils.isEmpty(restClient.getApiPrefix()) ? "" : "/" + restClient.getApiPrefix());
        uriBuilder.append(substitute(uri));     
        uri = uriBuilder.toString();
        try {           
            byte[] body = getBody(methodName);                                  
            RESTResponse response = sendRequest(restClient, methodName, uri, body);
            return response;            
        } catch (Exception e) {
             throw new RuntimeException(e);
        }            
    }


    /**
     * Validate the command
     * @param restClient
     */
    public void validate(RESTClient restClient) {
        Utils.require(this.commandName != null, "missing command name");
        
        //validate command name 
        this.metadataJson = matchCommand(restMetadataJson, this.commandName, commandParams.containsKey(STORAGE_SERVICE) ? (String)commandParams.get(STORAGE_SERVICE): _SYSTEM);
        if (this.metadataJson == null) {
        	//application command not found, look for system command
        	if (commandParams.containsKey(STORAGE_SERVICE)) {
        		this.metadataJson = matchCommand(restMetadataJson, this.commandName, _SYSTEM);
        	}
        }
      	if  (this.metadataJson == null) {      	
    		throw new RuntimeException("unsupported command name: " + this.commandName);
    	}        
        //validate required params
        validateRequiredParams();
    }


    /**
     * Validate Required Params
     */
    private void validateRequiredParams() {
        String[] uriStrings = getURI().split("\\?");       
        if (uriStrings.length > 0) {
	        List<String> requiredParms = extractRequiredParamsInURI(uriStrings[0]);
	        for (String param: requiredParms) {
	        	Utils.require(commandParams.containsKey(param), "missing param: " + param); 
	        }
        }
        if (metadataJson.containsKey(INPUT_ENTITY)) {
            String paramValue = metadataJson.getString(INPUT_ENTITY);
            if (paramValue != null) {
                JsonObject parametersNode = metadataJson.getJsonObject(PARAMETERS);
                if (parametersNode != null) {
                    if (parametersNode.keySet().size() == 1) {//parent node usually "params" 
                        JsonObject params = parametersNode.getJsonObject(parametersNode.keySet().iterator().next());
                        for (String param: params.keySet()) {
                            JsonObject paramNode = params.getJsonObject(param);                     
                            if (paramNode.keySet().contains(_REQUIRED)) {                                     
                                setCompound(true);
                                Utils.require(commandParams.containsKey(param), "missing param: " + param); 
                            }               
                        }       
                    }
                }
                else {
                    Utils.require(commandParams.containsKey(paramValue), "missing param: " + paramValue);       
                }
            }
        }       
 
    }
    
    /**
     * Extract Required Params In URI
     * @param uri
     * @return the list of required params
     */
    private List<String> extractRequiredParamsInURI(String uri) {
	    List<String> matchList = new ArrayList<String>();
	    Pattern regex = Pattern.compile("\\{(.*?)\\}");
	    Matcher regexMatcher = regex.matcher(uri);
        while (regexMatcher.find()) {
          matchList.add(regexMatcher.group(1));
        }
        return matchList;
    }
    
 
    /**
     * Substitute the params with values submittted via command builder
     * @param uri the uri string
     * @return the uri string after substitution 
     */
    private String substitute(String uri) {
    	List<String> params = extractRequiredParamsInURI(uri);
    	for (String param: params) {
    		if (param.equals(PARAMS)) {
    			uri = uri.replace("{" + PARAMS +"}", Utils.urlEncode(getParamsURI()));
    		}
    		else {
    			uri = uri.replace("{" + param + "}", (String)commandParams.get(param));
    		}
    	}
        return uri;
    }


    /**
     * Build the body for request
     * @param method
     * @return
     */
    private byte[] getBody(String method) {
        byte[] body = null;
    	String methodsWithInputEntity = HttpMethod.POST.name() + "|" + HttpMethod.PUT.name() + "|" + HttpMethod.DELETE.name();
    	if (method.matches(methodsWithInputEntity)) {
            if (isCompound()) {
                body = Utils.toBytes(getQueryInputEntity());
            }
            else {
                if (metadataJson.containsKey(INPUT_ENTITY)) {
                    JSONable data = (JSONable)commandParams.get(metadataJson.getString(INPUT_ENTITY));
                    if (data != null) {
                        body = Utils.toBytes(data.toJSON());
                    }
                }
            }
        }
        return body;
    }

    
    /**
     * Make RESTful calls to Doradus server
     * @param restClient
     * @param methodName
     * @param uri
     * @param body
     * @return
     * @throws IOException
     */
    private RESTResponse sendRequest(RESTClient restClient, String methodName,
            String uri, byte[] body) throws IOException {
        RESTResponse response = restClient.sendRequest(HttpMethod.methodFromString(methodName), 
                                                       uri, ContentType.APPLICATION_JSON, body);        
        logger.debug("response: {}", response.toString());
        return response;
    }
 
    /**
     * Finds out if a command is supported by Doradus
     * @param commandsJson
     * @param commandName
     * @param storageService
     * @return
     */
    public static JsonObject matchCommand(JsonObject commandsJson, String commandName, String storageService) {
        for (String key : commandsJson.keySet())  {
            if (key.equals(storageService)) {
                JsonObject commandCats = commandsJson.getJsonObject(key);
                if (commandCats.containsKey(commandName)) {
                    return commandCats.getJsonObject(commandName);
                }                
            }
        }
        return null;
    }    
    
    /**
     * Get the {@link ApplicationDefinition} for the given application name. If the
     * connected Doradus server has no such application defined, null is returned.
     * 
     * @param appName   Application name.
     * @return          Application's {@link ApplicationDefinition}, if it exists,
     *                  otherwise null.
     */
    public static ApplicationDefinition getAppDef(RESTClient restClient, String appName)  {
        
        // GET /_applications/{application}
        Utils.require(!restClient.isClosed(), "Client has been closed");
        Utils.require(appName != null && appName.length() > 0, "appName");

        try {           
            StringBuilder uri = new StringBuilder(Utils.isEmpty(restClient.getApiPrefix()) ? "" : "/" + restClient.getApiPrefix());                     
            uri.append("/_applications/");
            uri.append(Utils.urlEncode(appName));
            RESTResponse response = restClient.sendRequest(HttpMethod.GET, uri.toString());
            logger.debug("listApplication() response: {}", response.toString());
            if (response.getCode() == HttpCode.NOT_FOUND) {
                return null;
            }
            ApplicationDefinition appDef = new ApplicationDefinition();
            appDef.parse(getUNodeResult(response));
            return appDef;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }   // getAppDef
    
    /**
     * getUNodeResult
     * @param response
     * @return UNode object
     */
    protected static UNode getUNodeResult(RESTResponse response) {
        return UNode.parse(response.getBody(), response.getContentType());
    }   // getUNodeResult    

    //convenient methods
    private String getMethod() {
        String methods = this.metadataJson.getString(METHODS);
        if (methods.contains(HttpMethod.POST.name())) { 
            return HttpMethod.POST.name();
        }
        else if (methods.contains(HttpMethod.PUT.name())) {
            return HttpMethod.PUT.name();
        }
        else {
            return methods;
        }
    }

    private String getURI() {
        return this.metadataJson.getString(URI); 
    }       
    
    private String getParamsURI() {
        StringBuilder uriBuilder = new StringBuilder();
        JsonObject parametersNode = metadataJson.getJsonObject(PARAMETERS);
        if (parametersNode != null) {
            JsonObject paramDetail = parametersNode.getJsonObject(PARAMS);
            for (String param: paramDetail.keySet()) {
                if (commandParams.containsKey(param)) {
                    uriBuilder.append(param).append("=").append(commandParams.get(param));
                }
            }
        }
        return uriBuilder.toString();
    }
        
    private String getQueryInputEntity() {
        JsonObjectBuilder jsonQuery = Json.createObjectBuilder();
        String inputEntityParam = metadataJson.getString(INPUT_ENTITY);
        JsonObject parametersNode = metadataJson.getJsonObject(PARAMETERS);
        if (parametersNode != null) {
            JsonObject paramDetail = parametersNode.getJsonObject(inputEntityParam);
            for (String param: paramDetail.keySet()) {
                Object value = commandParams.get(param);
                if (value!=null) {
                    jsonQuery.add(param, value.toString());
                }
            }
            return Json.createObjectBuilder().add(inputEntityParam, jsonQuery).build().toString();
        }
        return null;
    }
    
    /**
     * Creates a new {@link Command.Builder} instance.
     * <p>
     * This is a convenience method for {@code new Command.Builder()}.
     *
     * @return the new Command builder.
     */
    public static Command.Builder builder() {
        return new Command.Builder();
    }
    
    /**
     * Helper class to build {@link Command} instances.
     */
    public static class Builder {
        private Command command = new Command();
        
        /**
         * Provides command with name
         * @param name
         * @return
         */
        public Builder withName(String name) {
            command.setName(name);
            return this;
        }
 
        /**
         * Provides command with JSON object
         * @param name
         * @param value JSONable object
         * @return
         */
        public Builder withParam(String name, JSONable value) {
            if (!Utils.isEmpty(name)) {
                command.addParam(name, value);
            }
            return this;
        }
    
        /**
         * Provides command with param key and String value
         * @param name
         * @param value
         * @return
         */
        public Builder withParam(String name, String value) {
            if (!Utils.isEmpty(name)) {
                command.addParam(name, value);
            }
            return this;
        }
        
        /**
         * Provides command with param key and int value
         * @param name
         * @param value
         * @return
         */
        public Builder withParam(String name, int value) {
            if (!Utils.isEmpty(name)) {
                command.addParam(name, new Integer(value));
            }
            return this;
        }
            
        /**
         * Builds the Command 
         *
         * @return the newly built Command instance.
         */
        public Command build() {
            return this.command;
        }

    }   
}
