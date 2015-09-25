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

package com.dell.doradus.spider3;

import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.BatchResult;
import com.dell.doradus.common.DBObjectBatch;
import com.dell.doradus.common.HttpCode;
import com.dell.doradus.common.HttpMethod;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;
import com.dell.doradus.common.rest.RESTParameter;
import com.dell.doradus.logservice.LogQuery;
import com.dell.doradus.search.SearchResultList;
import com.dell.doradus.service.StorageService;
import com.dell.doradus.service.db.Tenant;
import com.dell.doradus.service.rest.RESTCallback;
import com.dell.doradus.service.rest.RESTService;
import com.dell.doradus.service.rest.ReaderCallback;
import com.dell.doradus.service.rest.UNodeOutCallback;
import com.dell.doradus.service.rest.annotation.Description;
import com.dell.doradus.service.rest.annotation.ParamDescription;
import com.dell.doradus.service.schema.SchemaService;
import com.dell.doradus.service.taskmanager.Task;

public class Spider3Service extends StorageService {
    private static final Spider3Service INSTANCE = new Spider3Service();

    private static final List<Class<? extends RESTCallback>> cmdClasses = Arrays.asList(
        UpdateCmd.class,
        QueryCmd.class,
        AggregateCmd.class
    );

    private Spider3Service() {}

    public static Spider3Service instance() {
        return INSTANCE;
    }
    
    @Override public Collection<Task> getAppTasks(ApplicationDefinition appDef) {
        checkServiceState();
        List<Task> appTasks = new ArrayList<>();
        return appTasks;
    }
    
    // Commands: POST /{application}/{table} and PUT /{application}/{table}
    @Description(
        name = "Add",
        summary = "Adds a batch of new objects to the given table.",
        methods = {HttpMethod.POST, HttpMethod.PUT},
        uri = "/{application}/{table}",
        inputEntity = "batch"
    )
    public static class UpdateCmd extends ReaderCallback {
        @Override public RESTResponse invokeStreamIn(Reader reader) {
            Utils.require(reader != null, "This command requires an input entity");
            ApplicationDefinition appDef = m_request.getAppDef();
            String table = m_request.getVariable("table");
            Tenant tenant = m_request.getTenant();
            DBObjectBatch batch = new DBObjectBatch();
            if (m_request.getInputContentType().isJSON()) {
                batch.parseJSON(reader);
            } else {
                UNode rootNode = UNode.parse(reader, m_request.getInputContentType());
                batch.parse(rootNode);
            }
            BatchResult result = Spider3.instance().addBatch(tenant, appDef, table, batch);
            return new RESTResponse(HttpCode.OK,
                                    result.toDoc().toString(m_request.getOutputContentType()),
                                    m_request.getOutputContentType());
        }
    }

    // Command: GET /{application}/{table}/_query?{params}
    @Description(
        name = "Query",
        summary = "Performs an object query on a specific application and table.",
        methods = HttpMethod.GET,
        uri = "/{application}/{table}/_query?{params}",
        outputEntity = "results"
    )
    public static class QueryCmd extends UNodeOutCallback {
        @ParamDescription
        public static RESTParameter describeParams() {
            return new RESTParameter("params")
                        .add("q", "text", true)
                        .add("s", "integer")
                        .add("k", "integer")
                        .add("f", "text")
                        .add("o", "text")
                        .add("e", "text")
                        .add("g", "text")
                        .add("skipCount", "boolean");
        }
        
        @Override public UNode invokeUNodeOut() {
            ApplicationDefinition appDef = m_request.getAppDef();
            TableDefinition tableDef = m_request.getTableDef(appDef);
            Tenant tenant = m_request.getTenant();
            String params = m_request.getVariable("params");    // leave encoded
            LogQuery logQuery = new LogQuery(params);
            SearchResultList searchResult = Spider3.instance().search(tenant, appDef, tableDef.getTableName(), logQuery);
            return searchResult.toDoc();
        }
    }

    // Command: GET /{application}/{table}/_aggregate?{params}
    @Description(
        name = "Aggregate",
        summary = "Performs an aggregate query on a specific application and table.",
        methods = HttpMethod.GET,
        uri = "/{application}/{table}/_aggregate?{params}",
        outputEntity = "results"
    )
    public static class AggregateCmd extends UNodeOutCallback {
        @ParamDescription
        public static RESTParameter describeParams() {
            return new RESTParameter("params")
                            .add("q", "text", true)
                            .add("f", "text")
                            .add("m", "text");
        }

        @Override public UNode invokeUNodeOut() {
            throw new NotImplementedException();
            //ApplicationDefinition appDef = m_request.getAppDef();
            //TableDefinition tableDef = m_request.getTableDef(appDef);
            //Tenant tenant = m_request.getTenant();
            //String params = m_request.getVariable("params");    // leave encoded
            //LogAggregate logAggregate = new LogAggregate(params);
            //AggregationResult result = Spider3Service.instance().m_logService.aggregate(tenant, appDef.getAppName(), tableDef.getTableName(), logAggregate);
            //AggregateResult aresult = AggregateResultConverter.create(result, "COUNT(*)", logAggregate.getQuery(), logAggregate.getFields());
            //return aresult.toDoc();
        }
    }

    @Override
    protected void initService() {
        RESTService.instance().registerCommands(cmdClasses, this);
    }

    @Override
    protected void startService() {
        SchemaService.instance().waitForFullService();
    }

    @Override
    protected void stopService() { }

    @Override
    public void deleteApplication(ApplicationDefinition appDef) {
        checkServiceState();
        Spider3.instance().deleteApplication(Tenant.getTenant(appDef), appDef.getAppName());
    }

    @Override
    public void initializeApplication(ApplicationDefinition oldAppDef, ApplicationDefinition appDef) {
        checkServiceState();
        Spider3.instance().createApplication(Tenant.getTenant(appDef), appDef.getAppName());
    }

    @Override
    public void validateSchema(ApplicationDefinition appDef) {
        checkServiceState();
    }

}
