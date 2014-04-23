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

package com.dell.doradus.service.spider;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.HttpCode;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.service.StorageService;
import com.dell.doradus.service.rest.NotFoundException;
import com.dell.doradus.service.rest.RESTCallback;
import com.dell.doradus.service.schema.SchemaService;

/**
 * Implements the REST command: PUT /{application}/{table}/_statistics/_refresh. Refreshes
 * all statistics in the given application/table.
 */
public class StatsRefreshTableCmd extends RESTCallback {

    @Override
    protected RESTResponse invoke() {
        String application = m_request.getVariableDecoded("application");
        ApplicationDefinition appDef = SchemaService.instance().getApplication(application);
        if (appDef == null) {
            throw new NotFoundException("Unknown application: " + application);
        }
        
        StorageService storageService = SchemaService.instance().getStorageService(appDef);
        Utils.require(storageService.getClass().getSimpleName().equals(SpiderService.class.getSimpleName()),
                      "Only SpiderService applications support statistics: %s", application);
        
        String table = m_request.getVariableDecoded("table");
        TableDefinition tableDef = appDef.getTableDef(table);
        Utils.require(tableDef != null, "Unknown table for application '%s': %s", application, table);
        
        return SpiderService.instance().refreshTableStatistics(application, tableDef) ?
        		new RESTResponse(HttpCode.OK) :
        		new RESTResponse(HttpCode.SERVICE_UNAVAILABLE,
        				"Statistics refreshening is already started on " + table);
    }   // invoke

}   // class StatsRefreshTableCmd
