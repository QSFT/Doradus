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
package com.dell.doradus.server;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

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

import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.service.rest.RESTService.RequestCallback;
import com.dell.doradus.service.rest.WebServer;

/**
 * JettyWebServer works a a WebServer plugin for Doradus. It can be run-time loaded and initialized during start-up time of Doradus server
 * See the settings (load_webserver and webserver_class) configured in doradus.yaml.
 * 
 */
public class JettyWebServer extends WebServer{
	
    private Server  m_jettyServer;
    private static final int SOCKET_TIMEOUT_MILLIS = 60 * 1000 * 5;  // 5 minutes
    
	private static final JettyWebServer INSTANCE = new JettyWebServer();
	   
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
    public static JettyWebServer instance() {
        return INSTANCE;
    }   // instance
    
    @Override
    public void init(String servletClassName) {
        // Server
        m_jettyServer = configureJettyServer();
        
        // Connector
        ServerConnector connector = configureConnector();
        m_jettyServer.addConnector(connector);
        
        // Handler
        ServletHandler handler = configureHandler(servletClassName);
        m_jettyServer.setHandler(handler);
    }   

    @Override
    public void start() {
        try {
            m_jettyServer.start();
            m_jettyServer.join();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start Jetty", e);
        }
    }   // startService

 
    @Override
    public void stop() {
        try {
            m_jettyServer.stop();
            m_jettyServer.join();
        } catch (InterruptedException e) {
            // Ignore
        } catch (Exception e) {
            m_logger.warn("Jetty stop failed", e);
        }
        m_requestCallbacks.clear(); 
    }   // stopService

    // Create, configure, and return the Jetty Server object.
    private Server configureJettyServer() {
        ServerConfig config = ServerConfig.getInstance();
        LinkedBlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<Runnable>(config.maxTaskQueue); 
        QueuedThreadPool threadPool = new QueuedThreadPool(config.maxconns, config.defaultMinThreads, config.defaultIdleTimeout, 
        		taskQueue);
        Server server = new Server(threadPool);
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
    private ServletHandler configureHandler(String servletClassName) {
        ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(servletClassName, "/*");
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
    

	@Override
	public void registerRequestCallback(RequestCallback callback) {
		m_requestCallbacks.add(callback);		
	}

	@Override
	public void notifyNewRequest() {
        for (RequestCallback callback : m_requestCallbacks) {
            callback.onNewRequest();
        }		
	}

	@Override
	public void notifyRequestSuccess(long startTimeNanos) {
      for (RequestCallback callback : m_requestCallbacks) {
            callback.onRequestSucceeded(startTimeNanos);
        }		
	}

	@Override
	public void notifyRequestRejected(String reason) {
        for (RequestCallback callback : m_requestCallbacks) {
            callback.onRequestRejected(reason);
        }	
	}

	@Override
	public void notifyRequestFailed(Throwable e) {
        for (RequestCallback callback : m_requestCallbacks) {
            callback.onRequestFailed(e);
        }		
	}
}
