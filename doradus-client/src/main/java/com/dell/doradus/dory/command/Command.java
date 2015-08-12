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
import java.util.HashMap;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import com.dell.doradus.client.RESTClient;
import com.dell.doradus.common.HttpMethod;
import com.dell.doradus.common.JSONable;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.Utils;

public class Command extends BaseCommand<RESTResponse> {
		
	private String commandName;
	private Map<String, Object> commandParams = new HashMap<>();	
	private JsonObject metadataJson;
	private boolean compound = false;
	
	//setters
	public void setName(String name) {
		this.commandName = name;		
	}
	
	public void addParam(String name, Object value) {
		commandParams.put(name, value);	
	}	
	
	public void setMetadataJson(JsonObject metadataJson) {
		this.metadataJson = metadataJson;		
	}
	
	public void setApplicationName(String applicationName) {
		commandParams.put("application", applicationName);		
	}
	
	public void setStorageService(String storageService) {
		commandParams.put("storageservice", storageService);			
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
	 * command call method implementation
	 * @param restClient 
	 */
	@Override
	public RESTResponse call(RESTClient restClient){		
		
		String methodName = getMethod();
		String uri = getURI();
		StringBuilder uriBuilder = new StringBuilder(Utils.isEmpty(restClient.getApiPrefix()) ? "" : "/" + restClient.getApiPrefix());
		uriBuilder.append(substitute(uri, restClient.getCredentials().getTenant()));		
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
     * Validate
     * @param restClient 
     * @param command
     * @return command in json
     * @throws IOException 
     */
	public void validate(RESTClient restClient) {
		Utils.require(this.commandName != null, "missing command name");
		
		//validate command name	
		this.metadataJson = matchCommand(restMetadataJson, this.commandName, commandParams.containsKey("storageservice") ? (String)commandParams.get("storageservice"): "_system");
		if (this.metadataJson == null) {
			throw new RuntimeException("unsupported command name: " + this.commandName);
		}
		
		//validate required params
		validateRequiredParams();
	}

	/**
	 * validate Required Params
	 */
	private void validateRequiredParams() {
		if (metadataJson.containsKey("input-entity")) {
			String paramValue = metadataJson.getString("input-entity");
			if (paramValue != null) {
				JsonObject parametersNode = metadataJson.getJsonObject("parameters");
				if (parametersNode != null) {
					if (parametersNode.keySet().size() == 1) {//parent node usually "params" 
						JsonObject params = parametersNode.getJsonObject(parametersNode.keySet().iterator().next());
						for (String param: params.keySet()) {
							JsonObject paramNode = params.getJsonObject(param);						
							if (paramNode.keySet().contains("_required")) {										
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
		String uri = metadataJson.getString("uri");	
		if (uri.contains("{application}")) {
			Utils.require(commandParams.containsKey("application"), "missing param: application");		
		}		
		if (uri.contains("{table}")) {
			Utils.require(commandParams.containsKey("table"), "missing param: table");		
		}
		if (uri.contains("{shard}")) {
			Utils.require(commandParams.containsKey("shard"), "missing param: shard");		
		}
		if (uri.contains("{key}")) {
			Utils.require(commandParams.containsKey("key"), "missing param: key");		
		}
	}
	

	
	//convenient methods
    private String getMethod() {
    	String methods = this.metadataJson.getString("methods");
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
    	return this.metadataJson.getString("uri"); 
    }	    
    

	private String substitute(String uri, String tenant) {
		if (uri.contains("{tenant}")) {			
			uri = uri.replace("{tenant}", tenant != null ? tenant : getTenant());
		}
		if (uri.contains("{application}")) {
			uri = uri.replace("{application}", Utils.urlEncode(getApplicationName()));
		}
		if (uri.contains("{table}")) {
			uri = uri.replace("{table}", Utils.urlEncode(getTableName()));
		}
		if (uri.contains("{key}")) {
			uri = uri.replace("{key}", Utils.urlEncode(getKey()));
		}		
		if (uri.contains("{shard}")) {
			uri = uri.replace("{shard}", Utils.urlEncode(getShardName()));
		}
		if (uri.contains("{params}")) {
			uri = uri.replace("{params}", Utils.urlEncode(getParamsURI()));
		}
		return uri;
	}



	/**
	 * getQueryInputEntity
	 * @return
	 */
	private String getQueryInputEntity() {
		JsonObjectBuilder jsonQuery = Json.createObjectBuilder();
		String inputEntityParam = metadataJson.getString("input-entity");
		JsonObject parametersNode = metadataJson.getJsonObject("parameters");
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
	 * Build the body for request
	 * @param method
	 * @return
	 */
    private byte[] getBody(String method) {
    	byte[] body = null;
    	if (method.contains(HttpMethod.POST.name()) || method.contains(HttpMethod.PUT.name())) {
    		if (isCompound()) {
    			body = Utils.toBytes(getQueryInputEntity());
    		}
    		else {
    			if (metadataJson.containsKey("input-entity")) {
		    		JSONable data = (JSONable)commandParams.get(metadataJson.getString("input-entity"));
		    		if (data != null) {
		    			body = Utils.toBytes(data.toJSON());
		    		}
    			}
    		}
    	}
    	return body;
	}

	private String getApplicationName() {
		return (String)commandParams.get("application");
	}
	
	private String getTableName() {
		return (String)commandParams.get("table");
	}

	private String getShardName() {
		return (String)commandParams.get("shard");
	}
	private String getKey() {
		return (String)commandParams.get("key");
	}
		
	private String getParamsURI() {
		StringBuilder uriBuilder = new StringBuilder();
		JsonObject parametersNode = metadataJson.getJsonObject("parameters");
		if (parametersNode != null) {
			JsonObject paramDetail = parametersNode.getJsonObject("params");
			for (String param: paramDetail.keySet()) {
				if (commandParams.containsKey(param)) {
					uriBuilder.append(param).append("=").append(commandParams.get(param));
				}
			}
		}
		return uriBuilder.toString();
	}
	
	private String getTenant() {
		return (String)commandParams.get("tenant");
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
