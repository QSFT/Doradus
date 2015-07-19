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

package com.dell.doradus.logservice;

import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dell.doradus.common.AggregateResult;
import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.BatchResult;
import com.dell.doradus.common.CommonDefs;
import com.dell.doradus.common.HttpCode;
import com.dell.doradus.common.HttpDefs;
import com.dell.doradus.common.HttpMethod;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;
import com.dell.doradus.common.rest.CommandParameter;
import com.dell.doradus.olap.OlapBatch;
import com.dell.doradus.olap.aggregate.AggregateResultConverter;
import com.dell.doradus.olap.aggregate.AggregationResult;
import com.dell.doradus.search.SearchResultList;
import com.dell.doradus.service.StorageService;
import com.dell.doradus.service.db.Tenant;
import com.dell.doradus.service.rest.RESTCallback;
import com.dell.doradus.service.rest.RESTCommand;
import com.dell.doradus.service.rest.RESTService;
import com.dell.doradus.service.rest.ReaderCallback;
import com.dell.doradus.service.rest.UNodeOutCallback;
import com.dell.doradus.service.rest.annotation.Description;
import com.dell.doradus.service.rest.annotation.ParamDescription;
import com.dell.doradus.service.schema.SchemaService;
import com.dell.doradus.service.taskmanager.Task;

/**
 * {@link StorageService} implementation for the log service.
 */
public class LoggingService extends StorageService {
    private static final LoggingService INSTANCE = new LoggingService();

    // REST commands supported by the LoggingService:
    private static final List<RESTCommand> REST_RULES = Arrays.asList(new RESTCommand[] {
        new RESTCommand("POST /{application}/{table}                        com.dell.doradus.logservice.LoggingService$UpdateCmd"),
        new RESTCommand("PUT  /{application}/{table}                        com.dell.doradus.logservice.LoggingService$UpdateCmd"),
        new RESTCommand("GET  /{application}/{table}/_query?{params}        com.dell.doradus.logservice.LoggingService$QueryCmd"),
        new RESTCommand("GET  /{application}/{table}/_aggregate?{params}    com.dell.doradus.logservice.LoggingService$AggregateCmd"),
        
        new RESTCommand("GET  /_lsapp           com.dell.doradus.logservice.LoggingService$LogServiceAppCmd"),
        new RESTCommand("GET  /_lsapp?{params}  com.dell.doradus.logservice.LoggingService$LogServiceAppCmd"),
    });

    private LogService m_logService = new LogService();
    
    // Singleton creation only
    private LoggingService() {}

    public static LoggingService instance() {
        return INSTANCE;
    }
    
    public LogService getLogService() { return m_logService; }
    
    //----- Command callbacks
    
    @Override public Collection<Task> getAppTasks(ApplicationDefinition appDef) {
        checkServiceState();
        List<Task> appTasks = new ArrayList<>();
        appTasks.add(new LogServiceAgerTask(appDef));
        appTasks.add(new LogServiceMergerTask(appDef));
        return appTasks;
    }   // getAppTasks
    
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
            String application = m_request.getVariable("application");
            String table = m_request.getVariable("table");
            Tenant tenant = m_request.getTenant();
            
            OlapBatch batch = null;
            if (m_request.getInputContentType().isJSON()) {
                batch = OlapBatch.parseJSON(reader, "Timestamp");
            } else {
                UNode rootNode = UNode.parse(reader, m_request.getInputContentType());
                batch = OlapBatch.fromUNode(rootNode, "Timestamp");
            }
            
            LoggingService.instance().m_logService.addBatch(tenant, application, table, batch);
            return new RESTResponse(HttpCode.OK,
                                    new BatchResult().toDoc().toString(m_request.getOutputContentType()),
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
        public static CommandParameter describeParams() {
            return new CommandParameter("params")
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
            SearchResultList searchResult = LoggingService.instance().m_logService.search(tenant, appDef.getAppName(), tableDef.getTableName(), logQuery);
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
        public static CommandParameter describeParams() {
            return new CommandParameter("params")
                            .add("q", "text", true)
                            .add("f", "text")
                            .add("m", "text");
        }

        @Override public UNode invokeUNodeOut() {
            ApplicationDefinition appDef = m_request.getAppDef();
            TableDefinition tableDef = m_request.getTableDef(appDef);
            Tenant tenant = m_request.getTenant();
            String params = m_request.getVariable("params");    // leave encoded
            LogAggregate logAggregate = new LogAggregate(params);
            AggregationResult result = LoggingService.instance().m_logService.aggregate(tenant, appDef.getAppName(), tableDef.getTableName(), logAggregate);
            AggregateResult aresult = AggregateResultConverter.create(result, "COUNT(*)", logAggregate.getQuery(), logAggregate.getFields());
            return aresult.toDoc();
        }
    }

    // Command: GET /_lsapp?{params}
    @Description(
        name = "Browse",
        summary = "Provides a simple web browser interface for LoggingService applications.",
        methods = HttpMethod.GET,
        uri = "/_lsapp?{params}",
        outputEntity = "{html}"
    )
    public static class LogServiceAppCmd extends RESTCallback {
        @ParamDescription
        public static CommandParameter describeParams() {
            // {params} are optional but details don't need to be public.
            return new CommandParameter("params", null, false);
        }
        
        @Override public RESTResponse invoke() {
            String params = m_request.getVariable("params");
            Map<String, String> parameters = Utils.parseURIQuery(params);
            String html = LogServiceApp.process(m_request.getTenant(), LoggingService.instance().m_logService, parameters);
            Map<String, String> headers = new HashMap<String, String>();
            headers.put(HttpDefs.CONTENT_TYPE, "text/html");
            return new RESTResponse(HttpCode.OK, Utils.toBytes(html), headers);
        }
    }
    
    //----- Service methods
    
    @Override
    protected void initService() {
        RESTService.instance().registerApplicationCommands(REST_RULES, this);
        List<Class<? extends RESTCallback>> cmdClasses = Arrays.asList(
            UpdateCmd.class,
            QueryCmd.class,
            AggregateCmd.class
        );
        RESTService.instance().registerCommands(cmdClasses, this);
    }

    @Override
    protected void startService() {
        SchemaService.instance().waitForFullService();
    }

    @Override
    protected void stopService() { }

    //----- StorageService: Schema update methods
    
    @Override
    public void deleteApplication(ApplicationDefinition appDef) {
        checkServiceState();
        for(TableDefinition tableDef: appDef.getTableDefinitions().values()) {
            m_logService.deleteTable(Tenant.getTenant(appDef), appDef.getAppName(), tableDef.getTableName());
        }
    }

    @Override
    public void initializeApplication(ApplicationDefinition oldAppDef, ApplicationDefinition appDef) {
        checkServiceState();
        for(TableDefinition tableDef: appDef.getTableDefinitions().values()) {
            m_logService.createTable(Tenant.getTenant(appDef), appDef.getAppName(), tableDef.getTableName());
        }
    }

    @Override
    public void validateSchema(ApplicationDefinition appDef) {
        checkServiceState();
        validateApplication(appDef);
    }

    //----- Private methods
    
    private void validateApplication(ApplicationDefinition appDef) {
        for (String optName : appDef.getOptionNames()) {
            String optValue = appDef.getOption(optName);
            switch (optName) {
            case CommonDefs.OPT_STORAGE_SERVICE:
                assert optValue.equals(this.getClass().getSimpleName());
                break;
                
            case CommonDefs.OPT_TENANT:
                // Ignore
                break;
                
            default:
                throw new IllegalArgumentException("Unknown option for LoggingService application: " + optName);
            }
        }

        for(TableDefinition tableDef: appDef.getTableDefinitions().values()) {
            Utils.require(tableDef.getFieldDefinitions().size() == 0,
                      "Field definitions are not allowed with the LoggingService");
        }
    }

}   // class LoggingService
