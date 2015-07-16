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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.HttpMethod;
import com.dell.doradus.common.rest.CommandSet;
import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.service.Service;
import com.dell.doradus.service.StorageService;
import com.dell.doradus.service.schema.SchemaService;

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
    private WebServer m_webservice;
    private final RESTCommandSet m_commandSet = new RESTCommandSet();

    // REST commands supported by the the Spider service:
    private static final List<RESTCommand> REST_RULES = Arrays.asList(new RESTCommand[] {
        new RESTCommand("GET /_commands com.dell.doradus.service.rest.DescribeCmd"),
    });
    
    private static final List<Class<? extends RESTCallback>> CMD_CLASSES = new ArrayList<>();
    static {
        CMD_CLASSES.add(DescribeCmd.class);
    }
    
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
    	registerGlobalCommands(REST_RULES);
    	registerCommands(null, CMD_CLASSES);
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
    
    public CommandSet describeCommands() {
        return m_commandSet.describeCommands();
    }
    
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
    
    // TODO: Experimental
    public void registerCommands(StorageService cmdOwner,
                                 Iterable<Class<? extends RESTCallback>> cmdClasses) {
        m_commandSet.addCommands(cmdOwner, cmdClasses);
    }
    
    /**
     * Register the given {@link RESTCommand}s as global commands. All such commands are
     * registered and recognized at global level. This method throws an exception if any
     * command is a duplicate of another global RESTCommand.
     *
     * @param restCommands  {@link RESTCommand}s to be registered. Any collection type
     *                      can be passed, for example.
     */
    public void registerGlobalCommands(Iterable<RESTCommand> restCommands) {
        m_commandSet.addCommands(null, restCommands);
    }
    
    /**
     * Register the given {@link RESTCommand}s as application commands belonging to the
     * given storage service. Only commands whose URI begins with "/{application}/" are
     * actually registered as application commands; all others are registered as system
     * (global) commands. Application commands are recognized and routed to the storage
     * service for the actual application name passed for each invocation.
     * <p>
     * This method throws an exception if any command is a duplicate of another
     * RESTCommand registered for the same scope.
     *
     * @param restCommands  {@link RESTCommand}s to be registered. Any collection type
     *                      can be passed, for example.
     * @param service       {@link StorageService} that owns the given commands.
     */
    public void registerApplicationCommands(Iterable<RESTCommand> restCommands, StorageService service) {
        assert service != null;
        m_commandSet.addCommands(service.getClass().getSimpleName(), restCommands);
    }
    
    /**
     * Register the given {@link RESTCommand}s as application commands belonging to the
     * given storage service, which extends the given parent storage service. Only
     * commands whose URI begins with "/{application}/" are registered as application
     * commands; all others are registered as system (global) commands.
     * <p>
     * This method allows a storage service to extend the commands of another storage
     * service. An application command is first routed to the storage service declared as
     * the owner of that application. However, if no matching command is registered for
     * that storage service, the parent service is then searched. This happens recursively
     * so that a hierarchy of storage services can be defined.
     * <p>
     * This method throws an exception if any command is a duplicate of another
     * RESTCommand registered for the same owner.
     *
     * @param restCommands  {@link RESTCommand}s to be registered. Any collection type
     *                      can be passed, for example.
     * @param service       {@link StorageService} that owns the given commands.
     * @param parentService Parent {@link StorageService} that the given service extends
     *                      with new commands.
     */
    public void registerApplicationCommands(Iterable<RESTCommand> restCommands,
                                            StorageService service,
                                            StorageService parentService) {
        assert service != null;
        m_commandSet.setParent(service.getClass().getSimpleName(), parentService.getClass().getSimpleName());
        m_commandSet.addCommands(service.getClass().getSimpleName(), restCommands);
    }
    
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
     * @param appDef        {@link ApplicationDefinition} of applicaiton that provides
     *                      context for command, if any. Null for system commands.
     * @param method        HTTP method (case-insensitive: GET, PUT).
     * @param uri           Request URI (case-sensitive: "/Magellan/Stars/_query")
     * @param query         Optional query parameter (case-sensitive: "q=Tarantula+Nebula%2A").
     * @param variableMap   Variable parameters defined in the RESTCommand substituted with
     *                      the actual values passed, not decoded (see above).
     * @return              The {@link RESTCommand} if a match was found, otherwise null.
     */
    public Xyzzy findCommand(ApplicationDefinition appDef, HttpMethod method, String uri,
                                    String query, Map<String, String> variableMap) {
        String cmdOwner = null;
        if (appDef != null) {
            StorageService ss = SchemaService.instance().getStorageService(appDef);
            cmdOwner = ss.getClass().getSimpleName();
        }
        return m_commandSet.findCommand(cmdOwner, method, uri, query, variableMap);
    }   // matchCommand
    
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
     * @param appDef        {@link ApplicationDefinition} of applicaiton that provides
     *                      context for command, if any. Null for system commands.
     * @param method        HTTP method (case-insensitive: GET, PUT).
     * @param uri           Request URI (case-sensitive: "/Magellan/Stars/_query")
     * @param query         Optional query parameter (case-sensitive: "q=Tarantula+Nebula%2A").
     * @param variableMap   Variable parameters defined in the RESTCommand substituted with
     *                      the actual values passed, not decoded (see above).
     * @return              The {@link RESTCommand} if a match was found, otherwise null.
     */
    public RESTCommand matchCommand(ApplicationDefinition appDef, String method, String uri,
                                    String query, Map<String, String> variableMap) {
        String cmdOwner = null;
        if (appDef != null) {
            StorageService ss = SchemaService.instance().getStorageService(appDef);
            cmdOwner = ss.getClass().getSimpleName();
        }
        return m_commandSet.findMatch(cmdOwner, method, uri, query, variableMap);
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
            Collection<String> commands = m_commandSet.getCommands();
            for (String command : commands) {
                m_logger.debug(command);
            }
        }
    }   // displayCommandSet

}   // class RESTService
