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
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.HttpMethod;
import com.dell.doradus.common.rest.RESTCatalog;
import com.dell.doradus.common.Utils;
import com.dell.doradus.service.Service;
import com.dell.doradus.service.StorageService;
import com.dell.doradus.service.rest.annotation.Description;
import com.dell.doradus.service.schema.SchemaService;

/**
 * The REST Service for Doradus. This singleton class creates and runs an instance of the
 * {@link WebServer}. It maintains a set of registered REST commands and matches requests
 * to the appropriate command.
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
    private WebServer m_webservice;
    private final RESTRegistry m_cmdRegistry = new RESTRegistry();

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
        m_webservice = loadWebServer(); // could be null
    	registerCommands(Arrays.asList(DescribeCmd.class));
    }   // initService

    // Begin servicing REST requests.
    @Override
    public void startService() {
        m_cmdRegistry.freezeCommandSet(true);
        displayCommandSet();
        if (m_webservice != null) {
            try {
                m_webservice.start();
            } catch (Exception e) {
                throw new RuntimeException("Failed to start WebService", e);
            }
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
    }   // stopService

    //----- RESTService public methods
    
    /**
     * Return the current set of visible, registered commands as a {@link RESTCatalog}.
     * This object can be serialized, returned via the REST API, and deserialized.
     *  
     * @return  A {@link RESTCatalog} of all visible, registered commands.
     */
    public RESTCatalog describeCommands() {
        return m_cmdRegistry.describeCommands();
    }
    
    /**
     * Register the given activity callback, which will be notified each time client
     * request activity occurs. 
     * 
     * @param callback  {@link RequestCallback} object.
     */
    public void registerRequestCallback(RequestCallback callback) {
       	if (m_webservice != null) {
       		m_webservice.registerRequestCallback(callback);
       	}
    }   // registerRequestCallback
    
    /**
     * Register a set of REST commands as global (system) commands. The commands are
     * defined via a set of {@link RESTCallback} objects, which must use the
     * {@link Description} annotation to provide metadata about the commands.
     * 
     * @param callbackClasses   Iterable collection of {@link RESTCallback} classes
     *                          that define the REST commands to be registered.
     */
    public void registerCommands(Iterable<Class<? extends RESTCallback>> callbackClasses) {
        m_cmdRegistry.registerCallbacks(null, callbackClasses);
    }
    
    /**
     * Register a set of application REST commands as belonging to the given storage
     * service owner. The commands are defined via a set of {@link RESTCallback} objects,
     * which must use the {@link Description} annotation to provide metadata about the
     * commands.
     * 
     * @param callbackClasses   Iterable collection of {@link RESTCallback} classes
     *                          that define application-specific REST commands to be
     *                          registered.
     * @param service           {@link StorageService} that owns the commands. 
     */
    public void registerCommands(Iterable<Class<? extends RESTCallback>> cmdClasses,
                                 StorageService service) {
        m_cmdRegistry.registerCallbacks(service, cmdClasses);
    }
    
    /**
     * Register a set of application REST commands as belonging to the given storage
     * service owner, which extends another storage service. The commands are defined
     * via a set of {@link RESTCallback} objects, which must use the {@link Description}
     * annotation to provide metadata about the commands.
     * 
     * @param callbackClasses   Iterable collection of {@link RESTCallback} classes
     *                          that define application-specific REST commands to be
     *                          registered.
     * @param service           {@link StorageService} that owns the commands.
     * @param parentService     {@link StorageService} whose commands are extended and/or
     *                          overridden by the given service's new commands.
     */
    public void registerCommands(Iterable<Class<? extends RESTCallback>> cmdClasses,
                                 StorageService service,
                                 StorageService parentService) {
        m_cmdRegistry.setParent(service.getClass().getSimpleName(), parentService.getClass().getSimpleName());
        m_cmdRegistry.registerCallbacks(service, cmdClasses);
    }
    
    /**
     * Attempt to match the given REST request to a registered command. If it does, return
     * the corresponding {@link RegisteredCommand} and update the given variable map with
     * decoded URI variables. For example, if the matching command is defined as:
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
     * Note that URI-encoded parameter values remain encoded in the variable map.
     * 
     * @param appDef        {@link ApplicationDefinition} of application that provides
     *                      context for command, if any. Null for system commands.
     * @param method        {@link HttpMethod} of the request.
     * @param uri           Request URI (case-sensitive: "/Magellan/Stars/_query")
     * @param query         Optional query parameter (case-sensitive: "q=Tarantula+Nebula%2A").
     * @param variableMap   Variable parameters defined in the REST command substituted with
     *                      the actual values passed, not decoded (see above).
     * @return              The {@link RegisteredCommand} if a match was found, otherwise null.
     */
    public RegisteredCommand findCommand(ApplicationDefinition appDef, HttpMethod method, String uri,
                                         String query, Map<String, String> variableMap) {
        String cmdOwner = null;
        if (appDef != null) {
            StorageService ss = SchemaService.instance().getStorageService(appDef);
            cmdOwner = ss.getClass().getSimpleName();
        }
        return m_cmdRegistry.findCommand(cmdOwner, method, uri, query, variableMap);
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
   
    // Attempt to load the WebServer instance defined by webserver_class.
    private WebServer loadWebServer() {
        WebServer webServer = null;
        if (!Utils.isEmpty(getParamString("webserver_class"))) {
            try {
                Class<?> serviceClass = Class.forName(getParamString("webserver_class"));
                Method instanceMethod = serviceClass.getMethod("instance", (Class<?>[])null);
                webServer = (WebServer)instanceMethod.invoke(null, (Object[])null);
                webServer.init(RESTServlet.class.getName());
            } catch (Exception e) {
                throw new RuntimeException("Error initializing WebServer: " + getParamString("webserver_class"), e);
            }
        }
        return webServer;
    }

    // If DEBUG logging is enabled, log all REST commands in sorted order.
    private void displayCommandSet() {
        if (m_logger.isDebugEnabled()) {
            m_logger.debug("Registered REST Commands:");
            Collection<String> commands = m_cmdRegistry.getCommands();
            for (String command : commands) {
                m_logger.debug(command);
            }
        }
    }   // displayCommandSet

}   // class RESTService
