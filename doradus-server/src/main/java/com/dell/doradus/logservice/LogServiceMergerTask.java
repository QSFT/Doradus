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

import java.util.List;

import org.slf4j.Logger;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.service.db.Tenant;
import com.dell.doradus.service.taskmanager.Task;

/**
 * merges logs unless table-level option "merge" is set to false (it's true by default) 
 */
public class LogServiceMergerTask extends Task {

    public LogServiceMergerTask(ApplicationDefinition appDef) {
        super(appDef, null, "logs-merging", "1 HOUR");
    }

    @Override
    public void execute() {
        doTask(m_tenant, m_appDef, m_logger);
    }

    public static void doTask(Tenant tenant, ApplicationDefinition appDef, Logger logger) {
        String application = appDef.getAppName();
        logger.info("Merging logs for {}/{}", tenant.getName(), application);
        LogService logService = LoggingService.instance().getLogService();
        for(TableDefinition tableDef: appDef.getTableDefinitions().values()) {
            boolean mergeDisabled = "false".equals(tableDef.getOption("merge"));
            if(mergeDisabled) continue;
            List<String> partitions = logService.getPartitions(tenant, application, tableDef.getTableName());
            for(String partition: partitions) {
                logService.mergePartition(tenant, application, tableDef.getTableName(), partition);
            }
            logger.info("Processed table {}",tableDef.getTableName());
        }
    }
}
