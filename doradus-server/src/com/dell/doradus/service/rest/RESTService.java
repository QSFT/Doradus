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

package com.dell.doradus.service.rest;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.SortedSet;

import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.service.Service;

/**
 * The REST Service for Doradus. This singleton class creates and runs an
 * instance of the Web server. It maintains a mapping from REST requests to callbacks
 * that handle each command via {@link #registerRESTCommands(Iterable)}. The servlet
 * that examines each request calls {@link #matchCommand(String, String, String, Map)} to
 * find the appropriate callback for each request, if one exists.
 */
public class RESTService extends Service {
    /**
     * Request callback interface when someone wants to be notified about request actions.
     */
    public static interface RequestCallback {
        void onConnectionOpened();
        void onConnectionClosed();
        void onNewRequest();
        void onRequestSucceeded(long startTimeNanos);
        void onRequestRejected(String reason);
        void onRequestFailed(Throwable e);
    }   // interface RequestCallback
	
    private static final RESTService INSTANCE = new RESTService();
    
    private WebServer				m_webservice;
 
    
    private final RESTCommandSet    m_commandSet = new RESTCommandSet();

    /**
     * Get the singleton instance of this service. The service may or may not have been
     * initialized yet.
     * 
     * @return  The singleton instance of this service.
     */
    public static RESTService instance() {
        return INSTANCE;
    }   // instance

    //----- Inherited Service methods
    
    // Initialize WebService so it's ready to run.
    @Override
    public void initService() {
    	boolean loadWebServer = ServerConfig.getInstance().load_webserver;
    	if (loadWebServer) {
	    	try {
			    Class<?> serviceClass = Class.forName(ServerConfig.getInstance().webserver_class);
		        Method instanceMethod = serviceClass.getMethod("instance", (Class<?>[])null);
		        m_webservice = (WebServer)instanceMethod.invoke(null, (Object[])null);
		        m_webservice.init(RESTServlet.class.getName());
		    } catch (Exception e) {
		        throw new RuntimeException("Error initializing WebServer: " + ServerConfig.getInstance().webserver_class, e);
		    }
    	}
    }   // initService

    // Begin servicing REST requests.
    @Override
    public void startService() {
        m_commandSet.freezeCommandSet(true);
        displayCommandSet();
        try {
           	if (m_webservice != null) {
           		m_webservice.start();
           	}
        } catch (Exception e) {
            throw new RuntimeException("Failed to start WebService", e);
        }
    }   // startService

    // Shutdown the REST Service
    @Override
    public void stopService() {
        try {
           	if (m_webservice != null) {
           		m_webservice.stop();
           	}
        } catch (Exception e) {
            m_logger.warn("WebService stop failed", e);
        }
        m_commandSet.clear();

    }   // stopService

    //----- RESTService public methods
    
    /**
     * Register the given callback, which will be notified each time client request
     * activity occurs. 
     * 
     * @param callback  {@link RequestCallback} object.
     */
    public void registerRequestCallback(RequestCallback callback) {
       	if (m_webservice != null) {
       		m_webservice.registerRequestCallback(callback);
       	}
    }   // registerRequestCallback
    
    /**
     * Register the given {@link RESTCommand}s. An exception is thrown if any of the
     * commands are considered a duplicate of another registered REST command.
     * 
     * @param restCommands  {@link RESTCommand}s to be registered. Any collection type
     *                      can be passed, for example.
     */
    public void registerRESTCommands(Iterable<RESTCommand> restCommands) {
        for (RESTCommand cmd : restCommands) {
            m_commandSet.addCommand(cmd);
        }
    }   // registerRESTCommands
    
    /**
     * See if the given REST request matches any registered commands. If it does, return
     * corresponding {@link RESTCommand} and update the given variable map with decoded
     * URI variables. For example, if the matching command is defined as:
     * <pre>
     *      GET /{application}/{table}/_query?{query}
     * </pre>
     * And the actual request passed is:
     * <pre>
     *      GET /Magellan/Stars/_query?q=Tarantula+Nebula%2A
     * <pre>
     * The variable map will be returned with the follow key/value pairs:
     * <pre>
     *      application=Magellan
     *      table=Stars
     *      query=Tarantula+Nebula%2A
     * </pre>
     * 
     * @param method        HTTP method (case-insensitive: GET, PUT).
     * @param uri           Request URI (case-sensitive: "/Magellan/Stars/_query")
     * @param query         Optional query parameter (case-sensitive: "q=Tarantula+Nebula%2A").
     * @param variableMap   Variable parameters defined in the RESTCommand substituted with
     *                      the actual values passed, not decoded (see above).
     * @return              The {@link RESTCommand} if a match was found, otherwise null.
     */
    public RESTCommand matchCommand(String method, String uri, String query,
                                    Map<String, String> variableMap) {
        return m_commandSet.findMatch(method, uri, query, variableMap);
    }   // matchCommand
    
    //----- Package-private methods (used by RESTServlet)

    void onNewrequest() {
    	if (m_webservice != null) {
    		m_webservice.notifyNewRequest(); 
    	}
    } 
    
    void onRequestSuccess(long startTimeNanos) {
       	if (m_webservice != null) {
       		m_webservice.notifyRequestSuccess(startTimeNanos);
       	}
    }   
    
    void onRequestRejected(String reason) {
       	if (m_webservice != null) {
       		m_webservice.notifyRequestRejected(reason);
       	}
    }   
    
    void onRequestFailed(Throwable e) {
       	if (m_webservice != null) {
       		m_webservice.notifyRequestFailed(e);
       	}
    } 
    
    //----- Private methods
    
    // Singleton construction only
    private RESTService() {}
   

    // If DEBUG logging is enabled, log all REST commands in sorted order.
    private void displayCommandSet() {
        if (m_logger.isDebugEnabled()) {
            m_logger.debug("Registered REST Commands:");
            Map<String, SortedSet<RESTCommand>> commandMap = m_commandSet.getCommands();
            for (String method : commandMap.keySet()) {
                for (RESTCommand cmd : commandMap.get(method)) {
                    m_logger.debug(cmd.toString());
                }
            }
        }
    }   // displayCommandSet
    
}   // class RESTService
