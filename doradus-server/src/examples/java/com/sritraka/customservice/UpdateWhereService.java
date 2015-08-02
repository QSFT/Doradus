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

package com.sritraka.customservice;

import java.util.Arrays;
import java.util.Map;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.BatchResult;
import com.dell.doradus.common.DBObject;
import com.dell.doradus.common.DBObjectBatch;
import com.dell.doradus.common.HttpCode;
import com.dell.doradus.common.HttpMethod;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;
import com.dell.doradus.common.rest.RESTParameter;
import com.dell.doradus.search.SearchResult;
import com.dell.doradus.search.SearchResultList;
import com.dell.doradus.service.Service;
import com.dell.doradus.service.StorageService;
import com.dell.doradus.service.rest.RESTService;
import com.dell.doradus.service.rest.UNodeInCallback;
import com.dell.doradus.service.rest.annotation.Description;
import com.dell.doradus.service.rest.annotation.ParamDescription;
import com.dell.doradus.service.schema.SchemaService;
import com.dell.doradus.service.spider.SpiderService;

/**
 * This is an example plug-in service for the Doradus Server. It extends the abstract
 * {@link Service} class to create an "UpdateWhereService", which adds an "update where"
 * REST command that can be used by any Spider application. The "update where" command
 * works as follows:   
 * <ul>
 * <li>The REST command is invoked with the following URI:<p>
 * <pre>PUT /{application}/{table}/_update?q={query}</pre>
 * </li>
 * <li>{application} is the application name, which must be managed by the Spider service.
 * </li>
 * <li>{table} is a table owned by the application whose objects are to be updated.
 * </li>
 * <li>{query} is a DQL query expression that selects the objects to be updated. All
 *     selected objects are updated with the same update (described below). No other
 *     URI parameters such as &amp;s should be given.
 * </li>
 * </ul>
 * The command must be accompanied by an input message containing a single "doc" element.
 * Each field updated within the doc group is applied to all selected objects. For
 * example, suppose the input message was the following JSON:
 * <pre>
 *      {"doc": {
 *         "Tags": {"add": ["Customer"]},
 *         "ThreadID": null
 *      }}</pre>
 * This update adds the value "Customer" to the multi-valued field "Tags" and sets the
 * "ThreadID" field to null. All updates are performed in a single batch commit. For a
 * selected object, the update is a no-op if the object already possesses the indicated
 * field values.
 * <p>
 * The command returns a "batch-result" object that uses the same format as a batch POST
 * or PUT command: a "doc" element is included for each selected object, giving the
 * object's ID and indicating if the object was updated.
 * <p>
 * To use this service, add the full service package name to the "default_services" option
 * in doradus.yaml. Example:
 * <pre>
 *      default_services:
 *          - com.dell.doradus.mbeans.MBeanService
 *          ...
 *          <b>- com.sritraka.customservice.UpdateWhereService</b></pre>
 * Note that, although this sample service is functional, it is rudimentary. For example,
 * updates to large batches use too much memory (which could be fixed with some paging).
 */
public class UpdateWhereService extends Service {
    private static final UpdateWhereService INSTANCE = new UpdateWhereService();
    private UpdateWhereService() {}
    
    @Description(
        name = "UpdateWhere",
        summary = "Updates all objects in a Spider application table selected by a query " +
                  "with the same update. Specify a query with 'q' in {params} and provide " +
                  "a 'doc' object with updates in the input entity.",
        methods = HttpMethod.PUT,
        uri = "/{application}/{table}/_update?{params}",
        inputEntity = "doc",
        outputEntity = "results"
    )
    public static class UpdateWhereCmd extends UNodeInCallback {
        @ParamDescription
        public static RESTParameter describeParams() {
            return new RESTParameter("params").add("q", "text", true);
        }
        
        @Override
        public RESTResponse invokeUNodeIn(UNode inNode) {
            // Parse request parameters
            ApplicationDefinition appDef = m_request.getAppDef();
            TableDefinition tableDef = m_request.getTableDef(appDef);
            String appName = appDef.getAppName();
            String tableName = tableDef.getTableName();

            StorageService storageService = SchemaService.instance().getStorageService(appDef);
            Utils.require(storageService.getClass().getSimpleName().equals("SpiderService"),
                          "Application must be a SpiderService application: " + appName);
            
            String params = m_request.getVariable("params");    // leave encoded
            Map<String, String> paramMap = Utils.parseURIQuery(params);
            Utils.require(paramMap.containsKey("q"), "Missing URI parameter: q");
            Utils.require(paramMap.size() == 1, "Only the 'q' parameter is allowed");
            params = params + "&f=_ID&s=0";
            
            // Parse the input entity into a DBObject
            Utils.require(inNode != null, "This command requires an input entity");
            DBObject modelObj = new DBObject();
            modelObj.parse(inNode);

            // Execute the query and apply the update to each object, creating a batch. 
            SearchResultList searchSet = SpiderService.instance().objectQueryURI(tableDef, params);
            DBObjectBatch dbObjBatch = new DBObjectBatch();
            for (SearchResult searchResult : searchSet.results) {
                dbObjBatch.addObject(modelObj.makeCopy(searchResult.id()));
            }
            
            // Submit the update batch and return the batch update result.
            BatchResult batchResult = SpiderService.instance().addBatch(appDef, tableName, dbObjBatch);
            String body = batchResult.toDoc().toString(m_request.getOutputContentType());
            return new RESTResponse(HttpCode.OK, body, m_request.getOutputContentType());
        }   // invokeUNodeIn
        
    }   // UpdateWhereCmd
    
    public static UpdateWhereService instance() {return INSTANCE;}
    
    @Override
    protected void initService() {
        // Add a REST command registered for the SpiderService.
        RESTService.instance().registerCommands(Arrays.asList(UpdateWhereCmd.class), SpiderService.instance());
    }

    // Nothing extra to do for start/stop service.
    
    @Override
    protected void startService() { }

    @Override
    protected void stopService() { }

}   // class UpdateWhereService
