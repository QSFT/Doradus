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

package com.dell.doradus.tasks;

import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.core.Defs;
import com.dell.doradus.service.db.DRow;
import com.dell.doradus.service.schema.SchemaService;
import com.dell.doradus.service.spider.SpiderService;

public class DeleteScalarFieldData extends FixDataTask {
	
	private final Logger logger = LoggerFactory.getLogger(DeleteScalarFieldData.class);
	
	@Override
	protected void runTask() {
		ApplicationDefinition appDef = SchemaService.instance().getApplication(getAppName());
		if (appDef == null) {
			logger.error("Application {} doesn't exist", getAppName());
			return;
		}
		TableDefinition tabDef = appDef.getTableDef(getTableName());
		if (tabDef == null) {
			logger.error("Table {} doesn't exist in the {} application",
					getTableName(), getAppName());
			return;
		}
		String fieldName = getParameter();
		String objectsStore = SpiderService.objectsStoreName(tabDef);
		String termsStore = SpiderService.termsStoreName(tabDef);
		
		// 1. Deleting data from the object table 
		Iterator<DRow> iRows = m_dbService.getAllRowsAllColumns(m_appName, objectsStore);
		while (iRows.hasNext()) {
			for (int count = 0; iRows.hasNext() && count < MAX_MUTATION_COUNT; ++count) {
				m_dbTran.deleteColumn(objectsStore, iRows.next().getKey(), fieldName);
				if (isInterrupted()) {
					return;
				}
			}
			m_dbService.commit(m_dbTran);
		}
		
		// 2. Delete terms data
		deleteTerms(tabDef, fieldName);
		
		m_dbTran.deleteColumn(termsStore, Defs.FIELD_REGISTRY_ROW_KEY, fieldName);
		m_dbService.commit(m_dbTran);
	}
}
