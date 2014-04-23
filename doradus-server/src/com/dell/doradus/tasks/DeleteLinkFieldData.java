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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.DRow;
import com.dell.doradus.service.schema.SchemaService;
import com.dell.doradus.service.spider.SpiderService;

public class DeleteLinkFieldData extends DoradusTask {
	public static final String TABLE_PARAM = "table";
	public static final String INVERSE_PARAM = "inverse";
	
	private final Logger logger = LoggerFactory.getLogger(DeleteLinkFieldData.class);
	
	protected DBService m_dbService = DBService.instance();
	protected DBTransaction m_dbTran = m_dbService.startTransaction();
	protected final int MAX_MUTATION_COUNT = ServerConfig.getInstance().batch_mutation_threshold;

	@Override
	protected void runTask() {
		ApplicationDefinition appDef = SchemaService.instance().getApplication(getAppName());
		if (appDef == null) {
			logger.error("Application {} doesn't exist anymore", getAppName());
			return;
		}
		TableDefinition tabDef = appDef.getTableDef(getTableName());
		if (tabDef == null) {
			logger.error("Table {} doesn't exist in the {} application",
					getTableName(), getAppName());
			return;
		}
		
		deleteLinkData(tabDef, getParameter());
		String invTable = getExtraParam(TABLE_PARAM);
		String invField = getExtraParam(INVERSE_PARAM);
		if (invTable != null && invField != null) {
			TableDefinition tabDefInverse = appDef.getTableDef(invTable);
			if (tabDefInverse == null) {
				logger.error("Table {} doesn't exist in the {} application",
						invTable, getAppName());
				return;
			}
			
			deleteLinkData(tabDefInverse, invField);
		}
	}

	private void deleteLinkData(TableDefinition tabDef, String fieldName) {
		String objectsStore = SpiderService.objectsStoreName(tabDef);
		String termsStore = SpiderService.termsStoreName(tabDef);
		
		// 1. Deleting links from the object table 
		Iterator<DRow> iRows = m_dbService.getAllRowsAllColumns(objectsStore);
		while (iRows.hasNext()) {
			DRow row = iRows.next();
			Set<String> colNames = new HashSet<>();
			Iterator<DColumn> iCols = row.getColumns();
			while (iCols.hasNext()) {
				DColumn col = iCols.next();
				if (Pattern.matches("~" + fieldName + "/.+", col.getName())) {
					colNames.add(col.getName());
				}
			}
			if (!colNames.isEmpty()) {
				m_dbTran.deleteColumns(objectsStore, row.getKey(), colNames);
				m_dbService.commit(m_dbTran);
			}
			if (isInterrupted()) {
				return;
			}
		}
		
		// 2. Deleting links from the terms table
		iRows = m_dbService.getAllRowsAllColumns(termsStore);
		int updCount = 0;
		while (iRows.hasNext()) {
			DRow nextRow = iRows.next();
			if (Pattern.matches("\\d+/~" + fieldName + "/.+", nextRow.getKey())) {
				m_dbTran.deleteRow(termsStore, nextRow.getKey());
				if (++updCount >= MAX_MUTATION_COUNT) {
					m_dbService.commit(m_dbTran);
					updCount = 0;
				}
			}
			if (isInterrupted()) {
				return;
			}
		}
		if (updCount > 0) {
			m_dbService.commit(m_dbTran);
		}
	}
}
