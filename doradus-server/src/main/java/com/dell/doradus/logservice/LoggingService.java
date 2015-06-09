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
import java.util.Arrays;
import java.util.List;

import com.dell.doradus.common.AggregateResult;
import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.BatchResult;
import com.dell.doradus.common.CommonDefs;
import com.dell.doradus.common.HttpCode;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;
import com.dell.doradus.olap.OlapBatch;
import com.dell.doradus.olap.aggregate.AggregateResultConverter;
import com.dell.doradus.olap.aggregate.AggregationResult;
import com.dell.doradus.search.SearchResultList;
import com.dell.doradus.service.StorageService;
import com.dell.doradus.service.db.Tenant;
import com.dell.doradus.service.rest.RESTCommand;
import com.dell.doradus.service.rest.RESTService;
import com.dell.doradus.service.rest.ReaderCallback;
import com.dell.doradus.service.rest.UNodeOutCallback;
import com.dell.doradus.service.schema.SchemaService;

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
    });

    private LogService m_logService = new LogService();
    
    // Singleton creation only
    private LoggingService() {}

    public static LoggingService instance() {
        return INSTANCE;
    }
    
    //----- Command callbacks
    
    // Commands: POST /{application}/{table} and PUT /{application}/{table} 
    public static class UpdateCmd extends ReaderCallback {
        @Override public RESTResponse invokeStreamIn(Reader reader) {
            Utils.require(reader != null, "This command requires an input entity");
            String application = m_request.getVariable("application");
            String table = m_request.getVariable("table");
            Tenant tenant = m_request.getTenant();
            
            OlapBatch batch = null;
            if (m_request.getInputContentType().isJSON()) {
                batch = OlapBatch.parseJSON(reader);
            } else {
                UNode rootNode = UNode.parse(reader, m_request.getInputContentType());
                batch = OlapBatch.fromUNode(rootNode);
            }
            
            LoggingService.instance().m_logService.addBatch(tenant, application, table, batch);
            return new RESTResponse(HttpCode.OK,
                                    new BatchResult().toDoc().toString(m_request.getOutputContentType()),
                                    m_request.getOutputContentType());
        }
    }

    // Command: GET /{application}/_query?{params}
    public static class QueryCmd extends UNodeOutCallback {

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

    // Command: GET /{application}/_aggregate?{params}
    public static class AggregateCmd extends UNodeOutCallback {

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
    
    //----- Service methods
    
    @Override
    protected void initService() {
        RESTService.instance().registerApplicationCommands(REST_RULES, this);
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
        m_logService.deleteApplication(Tenant.getTenant(appDef), appDef.getAppName());
        for(TableDefinition tableDef: appDef.getTableDefinitions().values()) {
            m_logService.deleteTable(Tenant.getTenant(appDef), appDef.getAppName(), tableDef.getTableName());
        }
    }

    @Override
    public void initializeApplication(ApplicationDefinition oldAppDef, ApplicationDefinition appDef) {
        checkServiceState();
        m_logService.createApplication(Tenant.getTenant(appDef), appDef.getAppName());
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
