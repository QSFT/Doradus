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

import java.util.GregorianCalendar;
import java.util.TimeZone;

import org.slf4j.Logger;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.CommonDefs;
import com.dell.doradus.common.RetentionAge;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.service.db.Tenant;
import com.dell.doradus.service.taskmanager.Task;

/**
 * Deletes old logs as specified by "retention-age" table-level option
 */
public class LogServiceAgerTask extends Task {

    public LogServiceAgerTask(ApplicationDefinition appDef) {
        super(appDef, null, "logs-aging", "1 DAY");
    }

    @Override
    public void execute() {
        LogServiceAgerTask.doTask(m_tenant, m_appDef, m_logger);
    }
    
    public static void doTask(Tenant tenant, ApplicationDefinition appDef, Logger logger) {
        String application = appDef.getAppName();
        logger.info("Checking expired logs for {}/{}", tenant.getKeyspace(), application);
        LogService logService = LoggingService.instance().getLogService();
        for(TableDefinition tableDef: appDef.getTableDefinitions().values()) {
            String retentionAgeStr = tableDef.getOption(CommonDefs.OPT_RETENTION_AGE);
            if(retentionAgeStr == null) continue;
            RetentionAge retentionAge = new RetentionAge(retentionAgeStr);
            GregorianCalendar cal = retentionAge.getExpiredDate(new GregorianCalendar(TimeZone.getTimeZone("GMT")));
            logger.info("Deleting logs before {}", Utils.formatDate(cal));
            long timestamp = cal.getTimeInMillis();
            logService.deleteOldSegments(tenant, application, tableDef.getTableName(), timestamp);
            logger.info("Processed table {}",tableDef.getTableName());
        }
    }

}
