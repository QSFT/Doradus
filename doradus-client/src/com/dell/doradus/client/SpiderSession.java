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

import java.util.HashMap;
import java.util.Map;

import com.dell.doradus.common.AggregateResult;
import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.BatchResult;
import com.dell.doradus.common.ContentType;
import com.dell.doradus.common.DBObject;
import com.dell.doradus.common.DBObjectBatch;
import com.dell.doradus.common.HttpCode;
import com.dell.doradus.common.HttpMethod;
import com.dell.doradus.common.ObjectResult;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.StatResult;
import com.dell.doradus.common.StatisticDefinition;
import com.dell.doradus.common.StatsStatus;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;

/**
 * Represents a session to a Doradus application whose StorageService option is the
 * SpiderService. Provides methods specific to SpiderService applications.
 */
public class SpiderSession extends ApplicationSession {

    /**
     * Create an SpiderSession that will access the given Spider application, sending
     * commands over the given RESTClient.
     * 
     * @param appDef        {@link ApplicationDefinition} of a Spider application.
     * @param restClient    {@link RESTClient} to use for commands.
     */
    public SpiderSession(ApplicationDefinition appDef, RESTClient restClient) {
        super(appDef, restClient);
        verifyApplication();
    }   // constructor

    //----- Updates
    
    /**
     * Add the given batch of objects to the given table, which must belong to this
     * session's application. Objects are added if they do not currently exist or do not
     * have an object ID assigned. Otherwise, existing objects are updated. The result of
     * the update is returned as a {@link BatchResult}.
     * 
     * @param tableName     Name of table to update.
     * @param dbObjBatch    {@link DBObjectBatch} of objects to add or update.
     * @return              A {@link BatchResult} object containing the results of the
     *                      update request.
     */
    @Override
    public BatchResult addBatch(String tableName, DBObjectBatch dbObjBatch) {
        Utils.require(!Utils.isEmpty(tableName), "tableName");
        Utils.require(dbObjBatch != null && dbObjBatch.getObjectCount() > 0,
                      "Object batch must have at least 1 object");
        
        try {
            // Send a POST request to "/{application}/{table}"
            byte[] body = Utils.toBytes(dbObjBatch.toDoc().toJSON());
            String uri = "/" + Utils.urlEncode(m_appDef.getAppName()) +
                         "/" + Utils.urlEncode(tableName);
            RESTResponse response = 
                m_restClient.sendRequest(HttpMethod.POST, uri, ContentType.APPLICATION_JSON, body);
            m_logger.debug("addBatch() response: {}", response.toString());
            return createBatchResult(response);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }   // addBatch
    
    /**
     * Add the given object to the given table, which must be defined for a table that
     * belongs to this session's application. This is a convenience method that bundles
     * the DBObject in a {@link DBObjectBatch} and calls
     * {@link #addBatch(String, DBObjectBatch)}. The {@link ObjectResult} from the batch
     * result is returned.
     * 
     * @param tableName Name of table to add object to.
     * @param dbObj     {@link DBObject} of object to add to the database.
     * @return          {@link ObjectResult} of the add request. The result can be used to
     *                  determine the ID of the object if it was added by the system.
     */
    public ObjectResult addObject(String tableName, DBObject dbObj) {
        Utils.require(!Utils.isEmpty(tableName), "tableName");
        Utils.require(dbObj != null, "dbObj");
        TableDefinition tableDef = m_appDef.getTableDef(tableName);
        Utils.require(tableDef != null,
                      "Unknown table for application '%s': %s", m_appDef.getAppName(), tableName);
        
        try {
            // Send single-object batch to "POST /{application}/{table}"
            DBObjectBatch dbObjBatch = new DBObjectBatch();
            dbObjBatch.addObject(dbObj);
            byte[] body = Utils.toBytes(dbObjBatch.toDoc().toJSON());
            String uri = "/" + Utils.urlEncode(m_appDef.getAppName()) +
                         "/" + Utils.urlEncode(tableName);
            RESTResponse response = 
                m_restClient.sendRequest(HttpMethod.POST, uri, ContentType.APPLICATION_JSON, body);
            m_logger.debug("addBatch() response: {}", response.toString());
            BatchResult batchResult = createBatchResult(response);
            
            ObjectResult objResult = null;
            if (batchResult.isFailed()) {
                objResult = ObjectResult.newErrorResult(batchResult.getErrorMessage(), dbObj.getObjectID());
            } else {
                // Object may have been assigned an ID, so iterate to first ObjectResult.
                for (String objID : batchResult.getResultObjectIDs()) {
                    objResult = batchResult.getObjectResult(objID);
                    break;
                }
            }
            return objResult;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }   // addObject
    
    /**
     * Delete the objects in the given batch from the given table, which must belong to
     * this session's Spider application. All objects in the batch must have IDs.
     * The result of the update is returned as a {@link BatchResult}.
     * 
     * @param tableName     Name of table from which to delete objects.
     * @param dbObjBatch    {@link DBObjectBatch} of objects to delete. The ID must be set
     *                      for each contained {@link DBObject}.
     * @return              A {@link BatchResult} object containing the results of the
     *                      delete request.
     */
    @Override
    public BatchResult deleteBatch(String tableName, DBObjectBatch dbObjBatch) {
        Utils.require(!Utils.isEmpty(tableName), "tableName");
        Utils.require(dbObjBatch != null && dbObjBatch.getObjectCount() > 0,
                      "Object batch must have at least 1 object");
        TableDefinition tableDef = m_appDef.getTableDef(tableName);
        Utils.require(tableDef != null,
                      "Unknown table for application '%s': %s", m_appDef.getAppName(), tableName);
        
        try {
            // Send a DELETE request to "/{application}/{table}"
            byte[] body = Utils.toBytes(dbObjBatch.toDoc().toJSON());
            String uri = "/" + Utils.urlEncode(m_appDef.getAppName()) +
                         "/" + Utils.urlEncode(tableName);
            RESTResponse response = 
                m_restClient.sendRequest(HttpMethod.DELETE, uri, ContentType.APPLICATION_JSON, body);
            m_logger.debug("deleteBatch() response: {}", response.toString());
            return createBatchResult(response);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }   // deleteBatch
    
    /**
     * Delete the object with the given ID from the given table. This is a convenience
     * method that creates a {@link DBObjectBatch} consisting of a single {@link DBObject}
     * with the given ID and then calls {@link #deleteBatch(String, DBObjectBatch)}. The
     * {@link ObjectResult} for the object ID is then returned. It is not an error to
     * delete an object that has already been deleted.
     * 
     * @param tableName Name of table from which to delete object. It must belong to this
     *                  session's application.
     * @param objID     ID of object to delete.
     * @return          {@link ObjectResult} of deletion request.
     */
    public ObjectResult deleteObject(String tableName, String objID) {
        Utils.require(!Utils.isEmpty(tableName), "tableName");
        Utils.require(!Utils.isEmpty(objID), "objID");
        TableDefinition tableDef = m_appDef.getTableDef(tableName);
        Utils.require(tableDef != null,
                      "Unknown table for application '%s': %s", m_appDef.getAppName(), tableName);
        DBObjectBatch dbObjBatch = new DBObjectBatch();
        dbObjBatch.addObject(objID, tableName);
        BatchResult batchResult = deleteBatch(tableName, dbObjBatch);
        return batchResult.getObjectResult(objID);
    }   // deleteObject
    
    //----- Queries
    
    /**
     * Perform an aggregate query with the given parameters. This is a convenience method
     * that packages parameters into a Map<String,String> and calls
     * {@link #aggregateQuery(String, Map)}. Optional parameters can be null or empty.
     * 
     * @param tableName         Name of table to query. It must belong to this session's
     *                          application.
     * @param metric            Required metric expression (e.g., "COUNT(*)").
     * @param queryText         Optional query expression.
     * @param groupingFields    Optional list of grouping field expressions.
     * @param bComposite        True if multi-level composite grouping is desired.
     * @return                  Query result as an {@link AggregateResult}.
     */
    public AggregateResult aggregateQuery(String  tableName,
                                          String  metric,
                                          String  queryText,
                                          String  groupingFields,
                                          boolean bComposite) {
        Map<String, String> params = new HashMap<>();
        if (!Utils.isEmpty(metric)) {
            params.put("m", metric);
        }
        if (!Utils.isEmpty(queryText)) {
            params.put("q", queryText);
        }
        if (!Utils.isEmpty(groupingFields)) {
            // Apply URL encoding in case of Unicode characters.
            if (bComposite) {
                params.put("cf", groupingFields);
            } else {
                params.put("f", groupingFields);
            }
        }
        return aggregateQuery(tableName, params);
    }   // aggregateQuery
    
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
     *         creates a grouped aggregate query instead of a global aggregate query.
     *         Either this or the 'cf' paramter can be provided, but not both.</td>
     * </tr>
     * <tr><td>cf</td>
     *     <td>composite grouping parameter</td>
     *     <td>Optional. Comma-separated list of grouping field expressions. When present,
     *         creates a grouped aggregate query instead of a global aggregate query. The
     *         'cf' parameter causes a <i>composite</i> group to be generated for secondary
     *         groups. If single-level aggregate queries, 'cf' is the same as 'f'. Either
     *         this or the 'f' parameter can be specified, but not both.</td>
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
        StringBuilder uri = new StringBuilder();
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
            case "cf":
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
                result.parse(getUNodeResult(response));
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }   // aggregateQuery
    
    /**
     * Get the object with the given ID from the given table. Null is returned if there is
     * no such object.
     *  
     * @param tableName     Name of table to get object from. It must belong to this
     *                      session's application.
     * @param objectID      Object's ID.
     * @return              {@link DBObject} containing all of the object's scalar and link
     *                      field values, or null if the object does not exist.
     */
    public DBObject getObject(String tableName, String objectID) {
        Utils.require(!Utils.isEmpty(tableName), "tableName");
        Utils.require(!Utils.isEmpty(objectID), "objectID");
        TableDefinition tableDef = m_appDef.getTableDef(tableName);
        Utils.require(tableDef != null,
                      "Unknown table for application '%s': %s", m_appDef.getAppName(), tableName);
        
        try {
            // Send a GET request to "/{application}/{table}/{object ID}"
            String uri = "/" + Utils.urlEncode(m_appDef.getAppName()) +
                         "/" + Utils.urlEncode(tableName) +
                         "/" + Utils.urlEncode(objectID);
            RESTResponse response = m_restClient.sendRequest(HttpMethod.GET, uri);
            m_logger.debug("getObject() response: {}", response.toString());
            
            // If the response is not "OK", return null.
            if (response.getCode() != HttpCode.OK) {
                return null;
            }
            return new DBObject().parse(getUNodeResult(response));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }   // getObject
    
    /**
     * Perform an object query for the given table and query parameters. The table must
     * belong to this session's Spider application. Recognized query parameters are:
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
     * <tr><td>g</td>
     *     <td>text</td>
     *     <td>Continuation token from a previous page. Objects are returned *after* the
     *         object represented by this token.</td>
     * </tr>
     * <tr><td>e</td>
     *     <td>text</td>
     *     <td>Continuation token from a previous page. Objects are returned *at* the
     *         object represented by this token. That is, the same object (if it still
     *         exists) is returned as the first result of the new page.</td>
     * </tr>
     * </table>
     * Either "e" or "g" can be provided, but not both. If none of "e", "g", or "k" is
     * given, the first page of the query is returned. Results are returned in a
     * {@link QueryResult} object. An exception is thrown if a parameter is invalid or a
     * database error occurs.
     * 
     * @param tableName Name of table to query. 
     * @param params    Names and values of query parameters. 
     * @return          {@link QueryResult} containing results.
     */
    @Override
    public QueryResult objectQuery(String tableName, Map<String, String> params) {
        Utils.require(tableName != null, "tableName");
        Utils.require(params != null, "params");
        TableDefinition tableDef = m_appDef.getTableDef(tableName);
        Utils.require(tableDef != null, "Unknown table: %s", tableName);
        
        // Form the URI, which has the general form: GET /{application}/{table}/_query?{params}
        StringBuilder uri = new StringBuilder();
        uri.append("/");
        uri.append(Utils.urlEncode(tableDef.getAppDef().getAppName()));
        uri.append("/");
        uri.append(Utils.urlEncode(tableDef.getTableName()));
        uri.append("/_query?");

        Utils.require(params.containsKey("q"), "Query ('q') parameter is required");
        uri.append("q=");
        uri.append(Utils.urlEncode(params.get("q")));
        
        for (String name : params.keySet()) {
            String value = params.get(name);
            switch (name) {
            case "e":
            case "f":
            case "k":
            case "g":
            case "s":
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
    
    /**
     * Perform an object query for the given table and query parameters. This is a
     * convenience method for Spider applications that bundles the given parameters into
     * a Map<String,String> and calls {@link #objectQuery(String, Map)}. String parameters
     * should *not* be URL-encoded. Optional, unused parameters can be null or empty (for
     * strings) or -1 (for integers).
     * 
     * @param tableName     Name of table to query. Must belong to this session's
     *                      application.
     * @param queryText     Query expression ('q') parameter. Required. 
     * @param fieldNames    Comma-separated field names to retrieve. Optional.
     * @param pageSize      Page size ('s'). Optional.
     * @param afterObjID    Continue-after continuation token ('g'). Optional.
     * @param sortOrder     Sort order parameter ('o'). Optional.
     * @return              Query results as a {@link QueryResult} object.
     */
    public QueryResult objectQuery(String tableName,
                                   String queryText,
                                   String fieldNames,
                                   int    pageSize,
                                   String afterObjID,
                                   String sortOrder) {
        Map<String, String> params = new HashMap<>();
        if (!Utils.isEmpty(queryText)) {
            params.put("q", queryText);
        }
        if (!Utils.isEmpty(fieldNames)) {
            params.put("f", fieldNames);
        }
        if (pageSize >= 0) {
            params.put("s", Integer.toString(pageSize));
        }
        if (!Utils.isEmpty(afterObjID)) {
            params.put("g", afterObjID);
        }
        if (!Utils.isEmpty(sortOrder)) {
            params.put("o", sortOrder);
        }
        return objectQuery(tableName, params);
    }   // QueryResult
    
    //----- Statistics methods

    /**
     * Refresh all statistics owned by the given table, which must belong to this session's
     * Spider application. An error is thrown if the table is unknown. For each
     * statistic owned by the table, a task is queued to recalculate the statistic. This
     * method returns once all tasks are queued, though the tasks may not start right away
     * and may take a while to run depending on Task Manager configuration. The status of 
     * statistic refresh requests can be checked with
     * {@link #getStatisticRefreshStatus(String)}. 
     * 
     * @param tableName Name of table to refresh statistics for.
     */
    public void refreshStatistics(String tableName) {
        Utils.require(tableName != null, "tableName");
        TableDefinition tableDef = m_appDef.getTableDef(tableName);
        Utils.require(tableDef != null, "Unknown table: %s", tableName);
        
        // Form the URI: PUT /{application}/{table}/_statistics/_refresh
        StringBuilder uri = new StringBuilder();
        uri.append("/");
        uri.append(Utils.urlEncode(tableDef.getAppDef().getAppName()));
        uri.append("/");
        uri.append(Utils.urlEncode(tableName));
        uri.append("/_statistics/_refresh");
        
        // Send the query and capture the response.
        try {
            RESTResponse response = m_restClient.sendRequest(HttpMethod.PUT, uri.toString());
            m_logger.debug("refreshStatistics() response: {}", response.toString());
            throwIfErrorResponse(response);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }   // refreshStatistics
    
    /**
     * Refresh a single statistic. The statistic with the given name must belong to the
     * given table, which must belong to this session's Spider application. This method
     * returns when a task is queued to refresh the statistic, but the task may not start
     * right away and it might take a while to perform depending on Task Manager
     * configuration. The status of  statistic refresh requests can be checked with
     * {@link #getStatisticRefreshStatus(String)}.
     *  
     * @param tableName Name of table that owns statistic.
     * @param statName  Name of statistic to refresh.
     */
    public void refreshStatistic(String tableName, String statName) {
        Utils.require(tableName != null, "tableName");
        TableDefinition tableDef = m_appDef.getTableDef(tableName);
        Utils.require(tableDef != null, "Unknown table: %s", tableName);
        StatisticDefinition statDef = tableDef.getStatDef(statName);
        Utils.require(statDef != null, "Unknown statistic: %s", statName);
        
        // Form the URI: PUT /{application}/{table}/_statistics/{stat}/_refresh
        StringBuilder uri = new StringBuilder();
        uri.append("/");
        uri.append(Utils.urlEncode(tableDef.getAppDef().getAppName()));
        uri.append("/");
        uri.append(Utils.urlEncode(tableName));
        uri.append("/_statistics/");
        uri.append(Utils.urlEncode(statName));
        uri.append("/_refresh");
        
        // Send the query and capture the response.
        try {
            RESTResponse response = m_restClient.sendRequest(HttpMethod.PUT, uri.toString());
            m_logger.debug("refreshStatistic() response: {}", response.toString());
            throwIfErrorResponse(response);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }   // refreshStatistic
    
    /**
     * Get the refresh status of all statistics belonging to the given table, which must
     * belong to this session's Spider application. The status of all known statistics is
     * returned as a {@link StatsStatus} object. It is not an error to request the
     * statistics refresh status of a table that has no statistics defined.
     * 
     * @param tableName Name of table to get statistics refresh status for.
     * @return          Statistics refresh status as a {@link StatsStatus} object.
     */
    public StatsStatus getStatisticRefreshStatus(String tableName) {
        Utils.require(tableName != null, "tableName");
        TableDefinition tableDef = m_appDef.getTableDef(tableName);
        Utils.require(tableDef != null, "Unknown table: %s", tableName);
        
        // Form the URI: GET /{application}/{table}/_statistics/_status
        StringBuilder uri = new StringBuilder();
        uri.append("/");
        uri.append(Utils.urlEncode(tableDef.getAppDef().getAppName()));
        uri.append("/");
        uri.append(Utils.urlEncode(tableName));
        uri.append("/_statistics/_status");
        
        // Send the query and capture the response.
        try {
            RESTResponse response = m_restClient.sendRequest(HttpMethod.GET, uri.toString());
            m_logger.debug("refreshStatistics() response: {}", response.toString());
            throwIfErrorResponse(response);
            StatsStatus status = new StatsStatus();
            status.parse(getUNodeResult(response));
            return status;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }   // getStatisticRefreshStatus
    
    /**
     * Retrieve the metric value(s) of the given statistic, optionally filtering the
     * groups returned. The given statistic name must belong to the given table, which
     * must belong to this session's Spider application. A statistic query is performed,
     * which retrieves the latest values computed for the statistic and returns them in a
     * {@link StatResult} object.
     * <p>
     * If the statistic is defined without a grouping parameter, it has a single, global
     * metric value, in which case the filter parameter cannot be used. If the statistic
     * is defined with a grouping parameter, it may have one or more group levels, and a
     * metric value will be computed for each leaf and intermediate group. If the filter
     * parameter is null or empty, the metric values of all groups are fetched. The filter
     * parameter can be used to limit which groups are returned at each grouping level. It
     * must use the following general form:
     * 
     * <pre>
     *      GROUP(1)=expression1,GROUP(2)=expression2,...,GROUP(n)=expressionN
     * </pre>
     * 
     * One GROUP clause is allowed per grouping level; multiple GROUP clauses must be
     * separated by commas. The parameter to the GROUP clause must be a valid grouping
     * level for the statistic. The expression is a Boolean query expression that selects
     * values for the corresponding grouping level. For example, suppose a statistic is
     * defined with a single grouping field called "Tags". The metric expression for a
     * single Tags value can be retrieved with the following filter parameter:
     * 
     * <pre>
     * GROUP(1) = Confidential
     * </pre>
     * 
     * That is, only the statistics group corresponding to the Tags value "Confidential",
     * if found, is returned. Secondary groups are filtered by defining appropriate
     * selection expressions for the corresponding group levels. For all field types,
     * equality and range clauses are allowed. For text fields, term clauses are also
     * allowed. For example, suppose the statistic was defined with a secondary text field
     * called "Subject". Secondary groups can be limited to those with the keyword
     * "Priority" using the following filter:
     * 
     * <pre>
     *      GROUP(2):Priority
     * </pre>
     * 
     * Each grouping level can have a separate GROUP clause. For example:
     * 
     * <pre>
     *      GROUP(1)=Confidential,GROUP(2):Priority
     * </pre>
     * 
     * Only groups that match the given filter parameter are returned.
     * 
     * @param tableName     Name of table that owns statistic.
     * @param statName      Name of statistic to query.
     * @param filterParam   Optional filter parameter to limit the groups returned for a
     *                      grouped statistic (see above).
     * @return {@link StatResult} containing statistic query results.
     */
    public StatResult queryStatistic(String tableName, String statName, String filterParam) {
        Utils.require(tableName != null, "tableName");
        TableDefinition tableDef = m_appDef.getTableDef(tableName);
        Utils.require(tableDef != null, "Unknown table: %s", tableName);
        StatisticDefinition statDef = tableDef.getStatDef(statName);
        Utils.require(statDef != null, "Unknown statistic: %s", statName);
        
        // Form the URI: GET /{application}/{table}/_statistics/{stat}[?{params}]
        StringBuilder uri = new StringBuilder();
        uri.append("/");
        uri.append(Utils.urlEncode(tableDef.getAppDef().getAppName()));
        uri.append("/");
        uri.append(Utils.urlEncode(tableName));
        uri.append("/_statistics/");
        uri.append(Utils.urlEncode(statName));
        if (!Utils.isEmpty(filterParam)) {
            uri.append("/?");
            uri.append(Utils.urlEncode(filterParam));
        }
        
        // Send the query and capture the response.
        try {
            RESTResponse response = m_restClient.sendRequest(HttpMethod.GET, uri.toString());
            m_logger.debug("queryStatistic() response: {}", response.toString());
            StatResult statResult = new StatResult(statDef);
            if (response.getCode() != HttpCode.OK) {
                statResult.setErrorMessage(response.getBody());
            } else {
                statResult.parse(getUNodeResult(response));
            }
            return statResult;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }   // queryStatistic
    
    //----- Private methods

    // Throw if this session's AppDef is not for an OLAP app.
    private void verifyApplication() {
        String ss = m_appDef.getStorageService();
        if (Utils.isEmpty(ss) || !ss.startsWith("Spider")) {
            throw new RuntimeException("Application '" + m_appDef.getAppName() +
                                       "' is not an Spider application");
        }
    }   // verifyApplication
    
}   // class SpiderSession
