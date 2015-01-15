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

import java.util.Map;
import java.util.Queue;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.eclipse.jetty.io.Connection;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.ThreadPool;

import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.service.Service;

/**
 * The Jetty-based REST Service for Doradus. This singleton class creates and runs an
 * instance of the Jetty server. It maintains a mapping from REST requests to callbacks
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
    
    private static final int SOCKET_TIMEOUT_MILLIS = 60 * 1000 * 5;  // 5 minutes
    private static final RESTService INSTANCE = new RESTService();
    
    private Server                  m_jettyServer;
    private final RESTCommandSet    m_commandSet = new RESTCommandSet();

    // Although it is unlike new RequestCallbacks will be added after initialization, we use
    // a ConcurrentLinkedQueue so we don't have to serialize onXxx requests.
    private final Queue<RequestCallback> m_requestCallbacks = new ConcurrentLinkedQueue<>();
    
    // Private Connection.Listener used to invoke connection opens and closes. Registered
    // as an MBean with Jetty's Connector.
    private class ConnListener implements Connection.Listener {
        @Override
        public void onOpened(Connection arg0) {
            for (RequestCallback callback : m_requestCallbacks) {
                callback.onConnectionOpened();
            }
        }
        
        @Override
        public void onClosed(Connection arg0) {
            for (RequestCallback callback : m_requestCallbacks) {
                callback.onConnectionClosed();
            }
        }   // onClosed
    }   // class ConnListener
    
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
    
    // Initialize the Jetty server so it's ready to run.
    @Override
    public void initService() {
        // Server
        m_jettyServer = configureJettyServer();
        
        // Connector
        ServerConnector connector = configureConnector();
        m_jettyServer.addConnector(connector);
        
        // Handler
        ServletHandler handler = configureHandler();
        m_jettyServer.setHandler(handler);
    }   // initService

    // Begin servicing REST requests.
    @Override
    public void startService() {
        displayCommandSet();
        try {
            m_jettyServer.start();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start Jetty", e);
        }
    }   // startService

    // Shutdown the REST Service
    @Override
    public void stopService() {
        try {
            m_jettyServer.stop();
            m_jettyServer.join();
        } catch (InterruptedException e) {
            // Ignore
        } catch (Exception e) {
            m_logger.warn("Jetty stop failed", e);
        }
        m_commandSet.clear();
        m_requestCallbacks.clear();
    }   // stopService

    //----- RESTService public methods
    
    /**
     * Register the given callback, which will be notified each time client request
     * activity occurs. 
     * 
     * @param callback  {@link RequestCallback} object.
     */
    public void registerRequestCallback(RequestCallback callback) {
        m_requestCallbacks.add(callback);
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
        for (RequestCallback callback : m_requestCallbacks) {
            callback.onNewRequest();
        }
    }   // onNewRequest
    
    void onRequestSuccess(long startTimeNanos) {
        for (RequestCallback callback : m_requestCallbacks) {
            callback.onRequestSucceeded(startTimeNanos);
        }
    }   // onRequestSuccess
    
    void onRequestRejected(String reason) {
        for (RequestCallback callback : m_requestCallbacks) {
            callback.onRequestRejected(reason);
        }
    }   // onRequestRejected
    
    void onRequestFailed(Throwable e) {
        for (RequestCallback callback : m_requestCallbacks) {
            callback.onRequestFailed(e);
        }
    }   // onRequestFailed
    
    //----- Private methods
    
    // Singleton construction only
    private RESTService() {}
    
    // Create, configure, and return the Jetty Server object.
    private Server configureJettyServer() {
        ServerConfig config = ServerConfig.getInstance();
        Server server = new Server();
        ThreadPool threadPool = server.getThreadPool();
        if (threadPool instanceof QueuedThreadPool) {
            ((QueuedThreadPool)threadPool).setMaxThreads(config.maxconns);
        }
        server.setStopAtShutdown(true);
        return server;
    }   // configureJettyServer
    
    // Create, configure, and return the ServerConnector object.
    private ServerConnector configureConnector() {
        ServerConfig config = ServerConfig.getInstance();
        ServerConnector connector = null;
        if (config.tls) {
            connector = createSSLConnector();
        } else {
            // Unsecured connector
            connector = new ServerConnector(m_jettyServer);
        }
        if (config.restaddr != null) {
            connector.setHost(config.restaddr);
        }
        connector.setPort(config.restport);
        connector.setIdleTimeout(SOCKET_TIMEOUT_MILLIS);
        connector.addBean(new ConnListener());  // invokes registered callbacks, if any
        return connector;
    }   // configureConnector
    
    // Create, configure, and return the ServletHandler object.
    private ServletHandler configureHandler() {
        ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(RESTServlet.class, "/*");
        return handler;
    }   // configureHandler

    // Create a Jetty ServerConnector configured to use TLS/SSL.
    private ServerConnector createSSLConnector() {
        ServerConfig config = ServerConfig.getInstance();
        
        SslContextFactory sslContextFactory = new SslContextFactory();
        sslContextFactory.setKeyStorePath(config.keystore);
        sslContextFactory.setKeyStorePassword(config.keystorepassword);
        sslContextFactory.setTrustStorePath(config.truststore);
        sslContextFactory.setTrustStorePassword(config.truststorepassword);
        sslContextFactory.setNeedClientAuth(config.clientauthentication);
        sslContextFactory.setIncludeCipherSuites(config.tls_cipher_suites.toArray(new String[]{}));

        HttpConfiguration http_config = new HttpConfiguration();
        http_config.setSecureScheme("https");
        HttpConfiguration https_config = new HttpConfiguration(http_config);
        https_config.addCustomizer(new SecureRequestCustomizer());
        SslConnectionFactory sslConnFactory = new SslConnectionFactory(sslContextFactory, "http/1.1");
        HttpConnectionFactory httpConnFactory = new HttpConnectionFactory(https_config);
        ServerConnector sslConnector = new ServerConnector(m_jettyServer, sslConnFactory, httpConnFactory);
        return sslConnector;
    }   // createSSLConnector

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
