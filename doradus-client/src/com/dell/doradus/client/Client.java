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

package com.dell.doradus.client;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.ContentType;
import com.dell.doradus.common.HttpCode;
import com.dell.doradus.common.HttpMethod;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;

/**
 * Creates a REST API connection to a Doradus server and provides methods to access
 * specific datababase tenants, which are called "applications". Requests and responses
 * use plain old Java objects (POJOs) and hide the details of the REST API. There are two
 * basic ways to use the Client class:
 * <p>
 * <ol>
 * <li>Global session: Each Client object opens a connection to a Doradus server and
 *     provides methods to get the schema for new applications, create new applications,
 *     and update and delete applications. A global session also allows the creation of
 *     application-specific sessions via the method {@link #openApplication(String)}. </li>
 * <p>
 * <li>Application session: If application schema management is not needed, an
 *     application session can be created via the static method
 *     {@link #openApplication(String, String, int, SSLTransportParameters)}.</li> 
 * </ol>
 * <p>
 * Global and application sessions use a single underlying {@link RESTClient} connection
 * for all REST commands, hence access via a session must be single-threaded. Create
 * multiple Client and/or ApplicationSession objects for multi-threaded access.
 * <p>
 * The Doradus client uses Simple Logging Facade for Java (SLF4J) to log debug messages,
 * which is typically used with Apache log4j. You can control logging options via a
 * log4j.properties file, which is defined as a JVM run time parameter. For example,
 * include this argument:
 * <pre>
 *   -Dlog4j.configuration=file:/somefolder/config/log4j.properties
 * </pre>
 * 
 * @see com.dell.doradus.client.RESTConnector
 */
public class Client implements AutoCloseable {
    // Members:
    private final String                    m_host;
    private final int                       m_port;
    private final SSLTransportParameters    m_sslParams;
    private final RESTClient                m_restClient;

    private final Logger m_logger = LoggerFactory.getLogger(getClass().getSimpleName());

    //----- Client session methods
    
    /**
     * Create a new Client that will communicate with the Doradus server using the given
     * REST API host and port. With this constructor, an unsedure (HTTP) connection is
     * created. An exception is thrown if the server is not available at the given host
     * and port or if it doesn't allow unsecured connection.
     * 
     * @param host  Doradus REST API host name or IP address.
     * @param port  Doradus REST API port number.
     */
    public Client(String host, int port) {
        Utils.require(!Utils.isEmpty(host), "host");
        
        m_sslParams = null;
        m_host = host;
        m_port = port;
        m_restClient = new RESTClient(m_sslParams, m_host, m_port);
    }   // constructor

    /**
     * Create a new Client that will communicate with the Doradus server using the given
     * REST API host, port, and TLS/SSL parameters. If the sslParams parameter is null,
     * this constructor acts exactly like the constructor {@link #Client(String, int)}.
     * Otherwise, it attempts to create a secured (HTTPS) connection to the Doradus
     * server. An exception is thrown if the server is not available at the given host
     * and port or if it doesn't allow SSL/TLS connections with the given parameters.
     * 
     * @param host          Doradus REST API host name or IP address.
     * @param port          Doradus REST API port number.
     * @param sslParams	    Optional {@link SSLTransportParameters} object containing
     *                      TLS/SSL connection parameters. If null, creates an unsecured
     *                      (HTTP) connection. 
     */
    public Client(String host, int port, SSLTransportParameters sslParams) {
        Utils.require(!Utils.isEmpty(host), "host");
        
        m_host = host;
        m_port = port;
        m_sslParams = sslParams;
        m_restClient = new RESTClient(m_sslParams, m_host, m_port);
    }   // constructor
    
    /**
     * Create a new Client that will communicate with a Doradus server with an existing
     * {@link RESTClient} object. This allows the RESTClient to be created independently.
     * 
     * @param  restClient   {@link RESTClient} object with an established connection to a
     *                      Doradus server.
     */
    public Client(RESTClient restClient) {
        Utils.require(restClient != null, "restClient");
        Utils.require(!restClient.isClosed(), "RESTClient connection must be open");
        
    	m_restClient = restClient;
    	m_sslParams = restClient.getSSLParams();
    	m_host = restClient.getHost();
    	m_port = restClient.getPort();
    }	// constructor
    
    /**
     * Close the client connection if it is open. After calling this method, any calls
     * that require a connection will receive an exception.
     */
    @Override
    public void close() {
        if (m_restClient != null) {
            m_restClient.close();
        }
    }   // close
    
    /**
     * Set the compression option for this client. When true, this option causes entities
     * in all input and output REST messages to be compressed. This option is also
     * inherited by all subsequent {@link ApplicationSession}s created by via this Client.
     * 
     * @param compression   True to compress entities (message payloads) in all REST
     *                      messages.
     */
    public void setCompression(boolean compression) {
        Utils.require(!m_restClient.isClosed(), "Client has been closed");
        m_restClient.setCompression(compression);
    }   // setCompression
    
    //----- Application session methods
    
    /**
     * Create an {@link ApplicationSession} for the given application name on the Doradus
     * server, creating a new connection the given REST API host, port, and TLS/SSL
     * parameters. This static method allows an application session to be created without
     * creating a Client object and then calling {@link #openApplication(String)}. If the
     * given sslParams is null, an unsecured (HTTP) connectio is opened. Otherwise, a
     * TLS/SSL connection is created using the given credentials. An exception is thrown
     * if the given application is unknown or cannot be accessed or if a connection cannot
     * be established. If successful, the session object will be specific to the type of
     * application opened, and it will have its own REST session to the Doradus server.
     * 
     * @param appName   Name of an application to open.
     * @param host      REST API host name or IP address.
     * @param port      REST API port number.
     * @param sslParams {@link SSLTransportParameters} object containing TLS/SSL
     *                  parameters to use, or null to open an HTTP connection. 
     * @return          {@link ApplicationSession} through which the application can be
     *                  accessed.
     * @see   com.dell.doradus.client.OLAPSession
     * @see   com.dell.doradus.client.SpiderSession
     */
    public static ApplicationSession openApplication(String appName, String host, int port,
                                                     SSLTransportParameters sslParams) {
        Utils.require(!Utils.isEmpty(appName), "appName");
        Utils.require(!Utils.isEmpty(host), "host");

        try (Client client = new Client(host, port, sslParams)) {
            ApplicationDefinition appDef = client.getAppDef(appName);
            Utils.require(appDef != null, "Unknown application: %s", appName);
            RESTClient restClient = new RESTClient(client.m_restClient);
            return openApplication(appDef, restClient);
        }
    }   // openApplication
    
    /**
     * Open the application with the given name, returning an {@link ApplicationSession}
     * through which the application can be accessed. An exception is thrown if the given
     * application is unknown or cannot be accessed. If successful, the session object
     * will be specific to the type of application opened, and it will have its own
     * REST session to the Doradus server.
     * 
     * @param appName   Name of an application in the connected Doradus database.
     * @return          {@link ApplicationSession} through which the application can be
     *                  accessed.
     * @see   com.dell.doradus.client.OLAPSession
     * @see   com.dell.doradus.client.SpiderSession
     */
    public ApplicationSession openApplication(String appName) {
        Utils.require(!Utils.isEmpty(appName), "appName");
        Utils.require(!m_restClient.isClosed(), "Client has been closed");
        
        ApplicationDefinition appDef = getAppDef(appName);
        Utils.require(appDef != null, "Unknown application: %s", appName);
        RESTClient newRestClient = new RESTClient(m_restClient);
        return Client.openApplication(appDef, newRestClient);
    }   // openApplication
    
    //----- Application Management
    
    // NOTE: We could move these to a "system session" that must be exlicitly opened,
    // thereby requiring credentials that verify the user can call these.
    
    /**
     * Create a new application in the connected Doradus server as defined in the given
     * {@link ApplicationDefinition} object. If the given application already exists, its
     * schema is replaced with the given definition. The {@link ApplicationDefinition} for
     * the same application is returned, updated with any system-assigned defaults. An
     * exception is thrown if an error occurs.
     * 
     * @param appDef    {@link ApplicationDefinition} of application to create or update.
     * @return          {@link ApplicationDefinition} of same application, updated with
     *                  any system-assigned defaults.
     */
    public ApplicationDefinition createApplication(ApplicationDefinition appDef) {
        Utils.require(!m_restClient.isClosed(), "Client has been closed");
        Utils.require(appDef != null, "appDef");
        Utils.require(appDef.getAppName() != null && appDef.getAppName().length() > 0,
                      "Application must have a 'name' defined");
        Utils.require(appDef.getKey() != null && appDef.getKey().length() > 0,
                      "Application must have a 'key' defined");
        
        try {
            // Serialize as a JSON message and convert to bytes.
            byte[] body = null;
            body = Utils.toBytes(appDef.toDoc().toJSON());
            
            // Send a POST request to the "/_applications".
            RESTResponse response =
                m_restClient.sendRequest(HttpMethod.POST, "/_applications", ContentType.APPLICATION_JSON, body);
            m_logger.debug("defineApplication() response: {}", response.toString());
            throwIfErrorResponse(response);
            return getAppDef(appDef.getAppName());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }   // createApplication
    
    /**
     * Create a new application in the connected Doradus server as defined by the given
     * schema text. The text must be formatted in JSON or XML as defined by the content
     * type. If the given application already exists, its schema is replaced with the
     * given definition. The {@link ApplicationDefinition} for the same application is
     * returned, updated with any system-assigned defaults. An exception is thrown if an
     * error occurs.
     * 
     * @param text          Text definition of the application to create or update.
     * @param contentType   {@link ContentType} in which text is formatted. Only
     *                      {@link ContentType#APPLICATION_JSON} and
     *                      {@link ContentType#TEXT_XML} are supported.
     * @return              {@link ApplicationDefinition} of same application, updated
     *                      with any system-assigned defaults.
     */
    public ApplicationDefinition createApplication(String text, ContentType contentType) {
        Utils.require(!m_restClient.isClosed(), "Client has been closed");
        Utils.require(text != null && text.length() > 0, "text");
        Utils.require(contentType != null, "contentType");
        
        ApplicationDefinition appDef = new ApplicationDefinition();
        appDef.parse(UNode.parse(text, contentType));
        return createApplication(appDef);
    }   // createdApplication
    
    /**
     * Get the {@link ApplicationDefinition} for all applications defined in the connected
     * Doradus database.
     * 
     * @return  A collection of all applications defined in the database, possible empty.
     */
    public Collection<ApplicationDefinition> getAllAppDefs() {
        Utils.require(!m_restClient.isClosed(), "Client has been closed");

        try {
            // Send a GET request to "/_applications" to list all applications.
            RESTResponse response = m_restClient.sendRequest(HttpMethod.GET, "/_applications");
            m_logger.debug("listAllApplications() response: {}", response.toString());
            throwIfErrorResponse(response);
            
            UNode rootNode = UNode.parse(response.getBody(), response.getContentType());
            List<ApplicationDefinition> appDefList = new ArrayList<ApplicationDefinition>();
            for (UNode appNode : rootNode.getMemberList()) {
                ApplicationDefinition appDef = new ApplicationDefinition();
                appDef.parse(appNode);
                appDefList.add(appDef);
            }
            return appDefList;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }   // getAllAppDefs
    
    /**
     * Get the {@link ApplicationDefinition} for the given application name. If the
     * connected Doradus server has no such application defined, null is returned.
     * 
     * @param appName   Application name.
     * @return          Application's {@link ApplicationDefinition}, if it exists,
     *                  otherwise null.
     */
    public ApplicationDefinition getAppDef(String appName)  {
        Utils.require(!m_restClient.isClosed(), "Client has been closed");
        Utils.require(appName != null && appName.length() > 0, "appName");

        try {
            // Send a GET request to "/_applications/{application}
            String uri = "/_applications/" + Utils.urlEncode(appName);
            RESTResponse response = m_restClient.sendRequest(HttpMethod.GET, uri);
            m_logger.debug("listApplication() response: {}", response.toString());
            if (response.getCode() == HttpCode.NOT_FOUND) {
                return null;
            }
            throwIfErrorResponse(response);
            
            ApplicationDefinition appDef = new ApplicationDefinition();
            appDef.parse(UNode.parse(response.getBody(), response.getContentType()));
            return appDef;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }   // getAppDef
    
    /**
     * Delete an existing application from the connected Doradus server, including all of
     * its tables and data. Because updates are idempotent, deleting an already-deleted
     * application is acceptable. Hence, if no error is thrown, the result is always true.
     * An exception is thrown if an error occurs.
     * 
     * @param appName   Name of existing application to delete.
     * @param key       Name of key of application to delete.
     * @return          True if the application was deleted or already deleted.
     */
    public boolean deleteApplication(String appName, String key) {
        Utils.require(!m_restClient.isClosed(), "Client has been closed");
        Utils.require(appName != null && appName.length() > 0, "appName");
        Utils.require(key != null && key.length() > 0, "key");
        
        try {
            // Send a DELETE request to "/_applications/{application}/{key}".
            String uri = "/_applications/" + Utils.urlEncode(appName) + "/" + Utils.urlEncode(key);
            RESTResponse response = m_restClient.sendRequest(HttpMethod.DELETE, uri);
            m_logger.debug("deleteApplication() response: {}", response.toString());
            if (response.getCode() != HttpCode.NOT_FOUND) {
                // Notfound is acceptable
                throwIfErrorResponse(response);
            }
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }   // deleteApplication
    
    //----- Private methods
    
    // Create an ApplicationSession using the given appDef. The session takes ownership of
    // the given restClient. If the open fails, the restClient is closed.
    private static ApplicationSession openApplication(ApplicationDefinition appDef, RESTClient restClient) {
        String storageService = appDef.getStorageService();
        if (storageService == null ||
                storageService.length() <= "Service".length() ||
                !storageService.endsWith("Service")) {
            throw new RuntimeException("Unknown storage-service for application '" + appDef.getAppName() + "': " + storageService);
        }
        
        // Using the application's StorageService, attempt to create an instance of the
        // ApplicationSession subclass XxxSession where Xxx is the storage prefix.
        try {
            String prefix = storageService.substring(0, storageService.length() - "Service".length());
            String className = Client.class.getPackage().getName() + "." + prefix + "Session";
            @SuppressWarnings("unchecked")
            Class<ApplicationSession> appClass = (Class<ApplicationSession>) Class.forName(className);
            Constructor<ApplicationSession> constructor = appClass.getConstructor(ApplicationDefinition.class, RESTClient.class);
            ApplicationSession appSession = constructor.newInstance(appDef, restClient);
            return appSession;
        } catch (Exception e) {
            restClient.close();
            throw new RuntimeException("Unable to load session class", e);
        }
    }   // openApplication
    
    // If the given response shows an error, throw a RuntimeException using its text.
    private void throwIfErrorResponse(RESTResponse response) {
        if (response.getCode().isError()) {
            String errMsg = response.getBody();
            if (Utils.isEmpty(errMsg)) {
                errMsg = "Unknown error; response code: " + response.getCode();
            }
            throw new RuntimeException(errMsg);
        }
    }   // throwIfErrorResponse
    
}   // class Client
