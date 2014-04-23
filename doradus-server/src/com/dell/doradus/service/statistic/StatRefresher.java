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

package com.dell.doradus.service.statistic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.StatisticDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.service.schema.SchemaService;
import com.dell.doradus.tasks.DoradusTask;

/**
 * Background task for recalculating statistics implementation.
 * It calls {@link StatisticRunner} for the real work.
 */
public class StatRefresher extends DoradusTask {

    // Logging interface:
    private Logger m_logger = LoggerFactory.getLogger(getClass().getSimpleName());
    
	@Override
	protected void runTask() {
        String application = getAppName();
        ApplicationDefinition appDef = SchemaService.instance().getApplication(application);
        if (appDef == null) {
			m_logger.error("Application {} doesn\'t exist anymore", application);
        	return;
        }
        String table = getTableName();
        TableDefinition tableDef = appDef.getTableDef(table);
        
        String stat = getParameter();
        String message = "Statistics refreshment started: application=" + application +
        		"; table=" + table +
        		(Utils.isEmpty(stat) ? ";" : "; statistics=" + stat);

        long startTime = System.currentTimeMillis();
        m_logger.info(message);
        if (Utils.isEmpty(stat)) {
        	for (StatisticDefinition statDef : tableDef.getStatDefinitions()) {
        		startRunner(application, statDef);
        	}
        } else {
    		startRunner(application, tableDef.getStatDef(stat));
        }
        long elapsed = System.currentTimeMillis() - startTime;
        m_logger.info("Statistics refreshment finished in {} seconds", (elapsed / 1000));
	}	// runTask

	private void startRunner(String application, StatisticDefinition statDef) {
		new StatisticRunner(application, statDef).run();
	}	// startRunner
}
