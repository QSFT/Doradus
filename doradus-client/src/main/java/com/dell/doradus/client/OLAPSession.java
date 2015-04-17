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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.dell.doradus.common.AggregateResult;
import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.BatchResult;
import com.dell.doradus.common.ContentType;
import com.dell.doradus.common.DBObjectBatch;
import com.dell.doradus.common.HttpCode;
import com.dell.doradus.common.HttpMethod;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;

/**
 * Represents a session to a Doradus application whose StorageService option is the
 * OLAPService. Provides methods specific to OLAPService applications.
 */
public class OLAPSession extends ApplicationSession {

    /**
     * Create an OLAPSession that will access the given OLAP application, sending commands
     * over the given RESTClient.
     * 
     * @param appDef        {@link ApplicationDefinition} of an OLAP application.
     * @param restClient    {@link RESTClient} to use for commands.
     */
    public OLAPSession(ApplicationDefinition appDef, RESTClient restClient) {
        super(appDef, restClient);
        verifyApplication();
    }   // constructor

    //----- Updates
    
    /**
     * Add the given batch of objects defined by the given {@link DBObjectBatch} to the
     * given OLAP shard. Objects can belong to different tables, but all tables must
     * belong to this session's application. If the update was successful, the GUID of
     * the segment is returned within the given {@link BatchResult}. An exception is
     * thrown if an error occurs.
     * 
     * @param shard         Name of shard to load.
     * @param dbObjBatch    {@link DBObjectBatch} of objects to add.
     * @return              {@link BatchResult} containing results of the update.
     */
    public BatchResult addBatch(String shard, DBObjectBatch dbObjBatch) {
        Utils.require(!Utils.isEmpty(shard), "shard");
        Utils.require(dbObjBatch != null && dbObjBatch.getObjectCount() > 0,
                      "Object batch must have at least 1 object");
        
        try {
            // Send a POST request to "/{application}/{shard}".
            byte[] body = null;
            body = dbObjBatch.toDoc().toCompressedJSON();
            StringBuilder uri = new StringBuilder(Utils.isEmpty(m_restClient.getApiPrefix()) ? "" : "/" + m_restClient.getApiPrefix());          			            
            uri.append("/");
            uri.append(Utils.urlEncode(m_appDef.getAppName()));
            uri.append("/");
            uri.append(Utils.urlEncode(shard));
            RESTResponse response =
                m_restClient.sendRequestCompressed(HttpMethod.POST, uri.toString(), ContentType.APPLICATION_JSON, body);
            m_logger.debug("addBatch() response: {}", response.toString());
            return createBatchResult(response);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }   // addBatch

    /**
     * Delete the objects in the given batch from the given shard. The result of the
     * update is returned as a {@link BatchResult}.
     * 
     * @param shard         Name of the shard from which to delete objects.
     * @param dbObjBatch    {@link DBObjectBatch} of objects to delete.
     * @return              A {@link BatchResult} object containing the results of the
     *                      update request.
     */
    @Override
    public BatchResult deleteBatch(String shard, DBObjectBatch dbObjBatch) {
        Utils.require(!Utils.isEmpty(shard), "shard");
        Utils.require(dbObjBatch != null && dbObjBatch.getObjectCount() > 0,
                      "Object batch must have at least 1 object");
        
        try {
            // Send a DELETE request to "/{application}/{shard}".
            byte[] body = Utils.toBytes(dbObjBatch.toDoc().toJSON());
            StringBuilder uri = new StringBuilder(Utils.isEmpty(m_restClient.getApiPrefix()) ? "" : "/" + m_restClient.getApiPrefix());          			            
            uri.append("/");
            uri.append(Utils.urlEncode(m_appDef.getAppName()));
            uri.append("/");
            uri.append(Utils.urlEncode(shard));
            RESTResponse response = 
                m_restClient.sendRequest(HttpMethod.DELETE, uri.toString(), ContentType.APPLICATION_JSON, body);
            m_logger.debug("deleteBatch() response: {}", response.toString());
            return createBatchResult(response);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }   // deleteBatch
    
    //----- Shard requests
    
    /**
     * Delete the OLAP shard with the given name belonging to this session's application.
     * True is returned if the delete was successful. An exception is thrown if an error
     * occurred.
     * 
     * @param shard     Shard name.
     * @return          True if the shard was successfully deleted.
     */
    public boolean deleteShard(String shard) {
        Utils.require(!Utils.isEmpty(shard), "shard");
        try {
            // Send a DELETE request to "/{application}/_shards/{shard}"
            StringBuilder uri = new StringBuilder(Utils.isEmpty(m_restClient.getApiPrefix()) ? "" : "/" + m_restClient.getApiPrefix());          			            
            uri.append("/");
            uri.append(Utils.urlEncode(m_appDef.getAppName()));
            uri.append("/_shards/");
            uri.append(Utils.urlEncode(shard));
            RESTResponse response = m_restClient.sendRequest(HttpMethod.DELETE, uri.toString());
            m_logger.debug("deleteShard() response: {}", response.toString());
            throwIfErrorResponse(response);
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }   // deleteShard
    
    /**
     * Get a list of all shard names owned by this application. If there are no shards
     * with data yet, an empty collection is returned.
     * 
     * @return  Collection of shard names, possibly empty.
     */
    public Collection<String> getShardNames() {
        List<String> shardNames = new ArrayList<>();
        try {
            // Send a GET request to "/{application}/_shards"
            StringBuilder uri = new StringBuilder(Utils.isEmpty(m_restClient.getApiPrefix()) ? "" : "/" + m_restClient.getApiPrefix());          			            
            uri.append("/");
            uri.append(Utils.urlEncode(m_appDef.getAppName()));
            uri.append("/_shards");
            RESTResponse response = m_restClient.sendRequest(HttpMethod.GET, uri.toString());
            m_logger.debug("mergeShard() response: {}", response.toString());
            throwIfErrorResponse(response);
            
            // Response should be UNode tree in the form /Results/{application}/shards
            // where "shards" is an array of shard names.
            UNode rootNode = getUNodeResult(response);
            UNode appNode = rootNode.getMember(m_appDef.getAppName());
            UNode shardsNode = appNode.getMember("shards");
            for (UNode shardNode : shardsNode.getMemberList()) {
                shardNames.add(shardNode.getValue());
            }
            return shardNames;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }   // getShardNames
    
    /**
     * Get statistics for the given shard name. Since the response to this command is
     * subject to change, the command result is parsed into a UNode, and the root object
     * is returned. An exception is thrown if the given shard name does not exist or does
     * not have any data.
     *  
     * @param shardName Name of a shard to query.
     * @return          Root {@link UNode} of the parsed result.
     */
    public UNode getShardStats(String shardName) {
        try {
            // Send a GET request to "/{application}/_shards/{shard}"
            StringBuilder uri = new StringBuilder(Utils.isEmpty(m_restClient.getApiPrefix()) ? "" : "/" + m_restClient.getApiPrefix());          			            
            uri.append("/");
            uri.append(Utils.urlEncode(m_appDef.getAppName()));
            uri.append("/_shards/");
            uri.append(shardName);
            RESTResponse response = m_restClient.sendRequest(HttpMethod.GET, uri.toString());
            m_logger.debug("mergeShard() response: {}", response.toString());
            throwIfErrorResponse(response);
            return getUNodeResult(response);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }   // getShardStats
    
    /**
     * Request a merge of the OLAP shard with the given name belonging to this session's
     * application. Optionally set the shard's expire-date to the given value. True is
     * returned if the merge was successful. An exception is thrown if an error occurred.
     * 
     * @param shard         Shard name.
     * @param expireDate    Optional value for shard's new expire-date. Leave null if the
     *                      shard should have no expiration date.
     * @return              True if the merge was successful.
     */
    public boolean mergeShard(String shard, Date expireDate) {
        Utils.require(!Utils.isEmpty(shard), "shard");
        try {
            // Send a POST request to "/{application}/_shards/{shard}[?expire-date=<date>]"
            StringBuilder uri = new StringBuilder(Utils.isEmpty(m_restClient.getApiPrefix()) ? "" : "/" + m_restClient.getApiPrefix());          			            
            uri.append("/");
            uri.append(Utils.urlEncode(m_appDef.getAppName()));
            uri.append("/_shards/");
            uri.append(Utils.urlEncode(shard));
            if (expireDate != null) {
                uri.append("?expire-date=");
                uri.append(Utils.urlEncode(Utils.formatDateUTC(expireDate)));
            }
            RESTResponse response = m_restClient.sendRequest(HttpMethod.POST, uri.toString());
            m_logger.debug("mergeShard() response: {}", response.toString());
            throwIfErrorResponse(response);
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }   // mergeShard

    //----- Queries
    
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
     * <tr><td>pair</td>
     *     <td>pair list</td>
     *     <td>Optiona. A comma-separated of pair of link paths that define a special
     *         <i>dual role</i> query. See OLAP documentation about the operation of this
     *         special query type.</td>
     * </tr>
     * <tr><td>shards</td>
     *     <td>shard list</td>
     *     <td>Comma-separated list of shards to query. Either this or the "range"
     *         parameter must be provided, but not both.</td>
     * </tr>
     * <tr><td>range</td>
     *     <td><i>shard-from</i>[,<i>shard-to</i>]</td>
     *     <td>Defines a range of shards to query beginning with shards whose name is
     *         greater than or equal to the <i>shard-from</i> name. If a <i>shard-to</i>
     *         name is given, shard names must also be less than or equal to that name.
     *         Either this or the "shards" parameter must be provided, but not both.</td>
     * </tr>
     * <tr><td>xshards</td>
     *     <td>shard list</td>
     *     <td>Comma-separated list of shards that define the search scope of xlinks. This
     *         parameter is only meaningful when the aggregate query uses xlinks. Either
     *         this or the "xrange" parameter can be provided, but not both. If neither
     *         "xshards" nor "xrange" is specified, the search scope of xlinks is defined
     *         by the "shards" or "range" parameter.</td>
     * </tr>
     * <tr><td>xrange</td>
     *     <td><i>shard-from</i>[,<i>shard-to</i>]</td>
     *     <td>Defines a range of shards to query for xlink values beginning with shards
     *         whose name is greater than or equal to the <i>shard-from</i> name. If a
     *         <i>shard-to</i> name is given, shard names must also be less than or equal
     *         to that name. Either this or the "xshards" parameter can be provided, but
     *         not both. If neither "xshards" nor "xrange" is specified, the search scope
     *         of xlinks is defined by the "shards" or "range" parameter.</td>
     * </tr>
     * </table>
     * Results are turned in the given {@link AggregateResult} object. An exception is thrown
     * if a parameter is invalid or a database error occurs.
     * 
     * @param tableName Name of table to query.
     * @param params    Names and values of aggregate query parameters. 
     * @return          {@link AggregateResult} containing results.
     */
    @Override
    public AggregateResult aggregateQuery(String tableName, Map<String, String> params) {
        // Prerequisites:
        Utils.require(!Utils.isEmpty(tableName), "tableName");
        Utils.require(params != null && params.size() > 0, "params");
        TableDefinition tableDef = m_appDef.getTableDef(tableName);
        Utils.require(tableDef != null,
                      "Table is not defined for application '%s': %s", m_appDef.getAppName(), tableName);
        
        // Form the URI, which has the general form: GET /{application}/{table}/_aggregate?{params}
        StringBuilder uri = new StringBuilder(Utils.isEmpty(m_restClient.getApiPrefix()) ? "" : "/" + m_restClient.getApiPrefix());          			            
        uri.append("/");
        uri.append(Utils.urlEncode(m_appDef.getAppName()));
        uri.append("/");
        uri.append(Utils.urlEncode(tableDef.getTableName()));
        uri.append("/_aggregate?");
        
        Utils.require(params.containsKey("m"), "Metric ('m') parameter is required");
        uri.append("m=");
        uri.append(Utils.urlEncode(params.get("m")));
        
        for (String name : params.keySet()) {
            String value = params.get(name);
            switch (name) {
            case "q":
            case "f":
            case "pair":
            case "shards":
            case "range":
            case "xshards":
            case "xrange":
                uri.append("&");
                uri.append(name);
                uri.append("=");
                uri.append(Utils.urlEncode(value));
                break;
            
            case "m":
                // already added above
                break;

            default:
                Utils.require(false, "Unknown parameter name: %s", name);
            }
        }
        
        // Send the query and capture the response.
        try {
            RESTResponse response = m_restClient.sendRequest(HttpMethod.GET, uri.toString());
            m_logger.debug("aggregateQuery() response: {}", response.toString());
            
            // If the response is not "OK", create a failed-query response and return.
            AggregateResult result = new AggregateResult();
            if (response.getCode() != HttpCode.OK) {
                result.setErrorMessage(response.getBody());
            } else {
                result.parse(UNode.parse(response.getBody(), response.getContentType()));
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }   // aggregateQuery
    
    /**
     * Perform an object query for the given table and query parameters. The table must
     * belong to this session's OLAP application. Recognized query parameters are:
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
     * <tr><td>shards</td>
     *     <td>shard list</td>
     *     <td>Comma-separated list of shards to query. Either this or the "range"
     *         parameter must be provided, but not both.</td>
     * </tr>
     * <tr><td>range</td>
     *     <td><i>shard-from</i>[,<i>shard-to</i>]</td>
     *     <td>Defines a range of shards to query beginning with shards whose name is
     *         greater than or equal to the <i>shard-from</i> name. If a <i>shard-to</i>
     *         name is given, shard names must also be less than or equal to that name.
     *         Either this or the "shards" parameter must be provided, but not both.</td>
     * </tr>
     * </table>
     * If the "k" is not given, the first page of the query is returned. Results are
     * returned in a {@link QueryResult} object. An exception is thrown if a parameter is
     * invalid or a database error occurs.
     * 
     * @param tableName Name of table to query. 
     * @param params    Names and values of query parameters. 
     * @return          {@link QueryResult} containing results.
     */
    @Override
    public QueryResult objectQuery(String tableName, Map<String, String> params){
        Utils.require(tableName != null, "tableName");
        Utils.require(params != null, "params");
        TableDefinition tableDef = m_appDef.getTableDef(tableName);
        Utils.require(tableDef != null, "Unknown table: %s", tableName);
        
        // Form the URI, which has the general form: GET /{application}/{table}/_query?{params}
        StringBuilder uri = new StringBuilder(Utils.isEmpty(m_restClient.getApiPrefix()) ? "" : "/" + m_restClient.getApiPrefix());          			            
        uri.append("/");
        uri.append(Utils.urlEncode(tableDef.getAppDef().getAppName()));
        uri.append("/");
        uri.append(Utils.urlEncode(tableDef.getTableName()));
        uri.append("/_query?");

        Utils.require(params.containsKey("q"), "'q' parameter is required");
        uri.append("q=");
        uri.append(Utils.urlEncode(params.get("q")));
        
        for (String name : params.keySet()) {
            String value = params.get(name);
            switch (name) {
            case "f":
            case "k":
            case "s":
            case "shards":
            case "range":
            case "o":
                uri.append("&");
                uri.append(name);
                uri.append("=");
                uri.append(Utils.urlEncode(value));
                break;
            
            case "q":
                // already added above
                break;

            default:
                Utils.require(false, "Unknown parameter name: %s", name);
            }
        }
        
        // Send the query and capture the response.
        try {
            RESTResponse response = m_restClient.sendRequest(HttpMethod.GET, uri.toString());
            m_logger.debug("objectQuery() response: {}", response.toString());
            
            // If the response is not "OK", create a failed-query response.
            QueryResult result = null;
            if (response.getCode() != HttpCode.OK) {
                result = new QueryResult(tableDef, response.getBody());
            } else {
                result = new QueryResult(tableDef, getUNodeResult(response));
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }   // objectQuery
    
    //----- Private methods

    // Extract the BatchResult from the given RESTResponse. Could be an error.
    private BatchResult createBatchResult(RESTResponse response) {
        // See what kind of message payload, if any, we received.
        BatchResult result = null;
        if (response.getCode().isError()) {
            String errMsg = response.getBody();
            if (errMsg.length() == 0) {
                errMsg = "Unknown error; response code=" + response.getCode();
            }
            result = BatchResult.newErrorResult(errMsg);
        } else {
            result = new BatchResult(getUNodeResult(response));
        }
        return result;
    }   // createBatchResult

    // Throw if this session's AppDef is not for an OLAP app.
    private void verifyApplication() {
        String ss = m_appDef.getStorageService();
        if (Utils.isEmpty(ss) || !ss.startsWith("OLAP")) {
            throw new RuntimeException("Application '" + m_appDef.getAppName() +
                                       "' is not an OLAP application");
        }
    }   // verifyApplication
    
}   // class OLAPSession
