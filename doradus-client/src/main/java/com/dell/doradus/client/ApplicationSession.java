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

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.common.AggregateResult;
import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.BatchResult;
import com.dell.doradus.common.ContentType;
import com.dell.doradus.common.DBObjectBatch;
import com.dell.doradus.common.HttpMethod;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;

/**
 * Represents a client session for a specific Doradus application. Each session holds its
 * own REST connection. ApplicationSession provides methods commong to all Doradus
 * applications. Subclasses provide additional methods specific to the type of application 
 * being accessed.
 */
abstract public class ApplicationSession implements AutoCloseable {
    protected final ApplicationDefinition m_appDef;
    protected final RESTClient            m_restClient;
    
    protected final Logger m_logger = LoggerFactory.getLogger(getClass().getSimpleName());

    //----- Session methods
    
    /**
     * Close this client session. No further calls can be made with this session object
     * after this method is called.
     */
    @Override
    public void close() {
        m_restClient.close();
    }   // close
    
    /**
     * Set the compression option for this session. When true, this option causes entities
     * in all input and output REST messages to be compressed. 
     * 
     * @param compression   True to compress entities (message payloads) in all REST
     *                      messages.
     */
    public void setCompression(boolean compression) {
        Utils.require(!m_restClient.isClosed(), "Client has been closed");
        m_restClient.setCompression(compression);
    }   // setCompression
    
    /**
     * Create an {@link ApplicationSession} for the given application, using the given
     * {@link RESTClient} for all requests. The given {@link ApplicationDefinition} is
     * cached but can be refreshed with the latest schema changes via
     * {@link #refreshSchema()}.
     * 
     * @param appDef        {@link ApplicationDefinition} of the Doradus application to
     *                      access in this session. 
     * @param restClient    {@link RESTClient} that will be used for all access via this
     *                      session.
     */
    ApplicationSession(ApplicationDefinition appDef, RESTClient restClient) {
        m_appDef = appDef;
        m_restClient = restClient;
    }   // constructor

    //----- Schema methods
    
    /**
     * Get the {@link ApplicationDefinition} for the this session's application. The
     * definition is cached, so it could be out of date. See {@link #refreshSchema()} 
     * 
     * @return  This session's application schema as a {@link ApplicationDefinition}.
     * @see     #refreshSchema()
     */
    public ApplicationDefinition getAppDef() {
        return m_appDef;
    }   // getAppDef
    
    /**
     * Refresh this session's application schema from the database. Since the
     * application's {@link ApplicationDefinition} is cached, it could be out of date
     * if the schema has been modified. This method fetches the latest version and
     * returns it. An exception is thrown if the application has been deleted or any
     * other error occurs.
     * 
     * @return  Latest version of this session's application as an
     *          {@link ApplicationDefinition}, which is also cahced.
     * @see #getAppDef()
     */
    public ApplicationDefinition refreshSchema()  {
        try {
            // Send a GET request to "/_applications/{application}
            StringBuilder uri = new StringBuilder("/_applications/");
            uri.append(Utils.urlEncode(m_appDef.getAppName()));
            RESTResponse response = m_restClient.sendRequest(HttpMethod.GET, uri.toString());
            m_logger.debug("listApplication() response: {}", response.toString());
            throwIfErrorResponse(response);
            m_appDef.parse(getUNodeResult(response));
            return m_appDef;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }   // refreshSchema
    
    /**
     * Update the schema for this session's application with the given definition. The
     * text must be formatted in XML or JSON, as defined by the given content type. True
     * is returned if the update was successful. An exception is thrown if an error
     * occurred.
     * 
     * @param text          Text of updated schema definition.
     * @param contentType   Format of text. Must be {@link ContentType#APPLICATION_JSON} or
     *                      {@link ContentType#TEXT_XML}.
     * @return              True if the schema update was successful.
     */
    public boolean updateSchema(String text, ContentType contentType) {
        Utils.require(text != null && text.length() > 0, "text");
        Utils.require(contentType != null, "contentType");
        
        try {
            // Send a PUT request to "/_applications/{application}".
            byte[] body = Utils.toBytes(text);
            StringBuilder uri = new StringBuilder("/_applications/");
            uri.append(Utils.urlEncode(m_appDef.getAppName()));
            RESTResponse response =
                m_restClient.sendRequest(HttpMethod.PUT, uri.toString(), ContentType.APPLICATION_JSON, body);
            m_logger.debug("updateSchema() response: {}", response.toString());
            throwIfErrorResponse(response);
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }   // updateSchema
    
    //----- Update methods
    
    /**
     * Add the given batch to the given application store. The store name is either a
     * table name (Spider application) or shard (OLAP application). Objects are added,
     * updated, and/or deleted depending on the batch. The result of the update is
     * returned as a {@link BatchResult}.
     * 
     * @param store         Name of table (Spider) or shard (OLAP) to update.
     * @param dbObjBatch    {@link DBObjectBatch} of objects to update.
     * @return              A {@link BatchResult} object containing the results of the
     *                      update request.
     */
    public abstract BatchResult addBatch(String store, DBObjectBatch dbObjBatch);
    
    /**
     * Delete the objects specified in the given batch from the given application store.
     * The store name is either a table name (Spider application) or shard (OLAP
     * application). Minimally, the object ID of each object must be specified within
     * the batch. The result of the update is returned as a {@link BatchResult}.
     * 
     * @param store         Name of table (Spider) or shard (OLAP) from which to delete
     *                      objects.
     * @param dbObjBatch    {@link DBObjectBatch} of objects to delete.
     * @return              A {@link BatchResult} object containing the results of the
     *                      delete request.
     */
    public abstract BatchResult deleteBatch(String store, DBObjectBatch dbObjBatch);
    
    //----- Query methods
    
    /**
     * Perform an aggregate query for the given table and query parameters. The table must
     * belong to this session's application. Query parameters are specific to the storage
     * service managing the application. Common parameters are:
     * 
     * <table>
     * <tr><td>Name</td><td>Value</td><td>Description</td></tr>
     * <tr><td>m</td>
     *     <td>metric list</td>
     *     <td>Required. Comma-separated list of metric expression(s) to compute.</td>
     * </tr>
     * <tr><td>q</td>
     *     <td>query expression</td>
     *     <td>Optional. Query expression that selects objects in the table. Default is
     *         "*", which selects all objects.</td>
     * </tr>
     * <tr><td>f</td>
     *     <td>grouping parameter</td>
     *     <td>Optional. Comma-separated list of grouping field expressions. When present,
     *         creates a grouped aggregate query instead of a global aggregate query.</td>
     * </tr>
     * </table>
     * Results are turned in the given {@link AggregateResult} object. An exception is thrown
     * if a parameter is invalid or a database error occurs.
     * 
     * @param tableName Name of table to query.
     * @param params    Names and values of aggregate query parameters. 
     * @return          {@link AggregateResult} containing results.
     */
    public abstract AggregateResult aggregateQuery(String tableName, Map<String, String> params);
    
    /**
     * Perform an object query for the given table and query parameters. The table must
     * belong to this session's application. Query parameters are specific to the storage
     * service managing the application. Common parameters are:
     * 
     * <table>
     * <tr><td>Name</td><td>Value</td><td>Description</td></tr>
     * <tr><td>q</td>
     *     <td>query expression</td>
     *     <td>Required. Query expression that selects objects in the table. "*" selects
     *         all objects.</td>
     * </tr>
     * <tr><td>f</td>
     *     <td>field list</td>
     *     <td>Comma-separated list of fields to return in the query.</td>
     * </tr>
     * <tr><td>s</td>
     *     <td>integer</td>
     *     <td>Number of objects to return in the page. Defaults to the value configured
     *         for the server. 0 means "all objects".</td>
     * </tr>
     * <tr><td>o</td>
     *     <td>field [ASC|DESC]</td>
     *     <td>Order the results by the given scalar field name, optionally followed by
     *         ASC (ascending, the default) or DESC (descending).</td>
     * </tr>
     * <tr><td>k</td>
     *     <td>integer</td>
     *     <td>Skip the given number of objects, thereby returning a secondary page of
     *         results.</td>
     * </tr>
     * </table>
     * Results are turned in the given {@link QueryResult} object. An exception is thrown
     * if a parameter is invalid or a database error occurs.
     * 
     * @param tableName Name of table to query. 
     * @param params    Names and values of query parameters. 
     * @return          {@link QueryResult} containing results.
     */
    public abstract QueryResult objectQuery(String tableName, Map<String, String> params);
    
    //----- Protected methods
    
    // Parse the entity in given RESTResponse into a UNode tree, returning the root node.
    protected UNode getUNodeResult(RESTResponse response) {
        return UNode.parse(response.getBody(), response.getContentType());
    }   // getUNodeResult

    // If the given response shows an error, throw a RuntimeException using its text.
    protected void throwIfErrorResponse(RESTResponse response) {
        if (response.getCode().isError()) {
            String errMsg = response.getBody();
            if (Utils.isEmpty(errMsg)) {
                errMsg = "Unknown error; response code: " + response.getCode();
            }
            throw new RuntimeException(errMsg);
        }
    }   // throwIfErrorResponse
    
}   // class ApplicationSession
