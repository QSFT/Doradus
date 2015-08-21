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

import javax.json.JsonObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.client.RESTClient;
import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.ContentType;
import com.dell.doradus.common.HttpCode;
import com.dell.doradus.common.HttpMethod;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;

public abstract class BaseCommand<T>  {
    
    protected static final Logger logger = LoggerFactory.getLogger(BaseCommand.class.getSimpleName());
       
    protected JsonObject restMetadataJson = null;

    /**
     * Executes the command
     *
     * @return T a result. Each command has its own return type
     */ 
    public abstract RESTResponse call(RESTClient restClient);
    

    public BaseCommand() {

    }
    /**
     * set RestMetadataJson
     * @param restMetadataJson
     */
    public void setRestMetadataJson(JsonObject restMetadataJson) {
        this.restMetadataJson = restMetadataJson;
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
    protected RESTResponse sendRequest(RESTClient restClient, String methodName,
            String uri, byte[] body) throws IOException {
        RESTResponse response = restClient.sendRequest(HttpMethod.methodFromString(methodName), 
                                                       uri, ContentType.APPLICATION_JSON, body);        
        logger.debug("response: {}", response.toString());
        return response;
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
}
