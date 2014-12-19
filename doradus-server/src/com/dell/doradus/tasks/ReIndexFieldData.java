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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.CommonDefs;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.core.Defs;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.DRow;
import com.dell.doradus.service.db.Tenant;
import com.dell.doradus.service.schema.SchemaService;
import com.dell.doradus.service.spider.SpiderHelper;
import com.dell.doradus.service.spider.SpiderService;

public class ReIndexFieldData extends FixDataTask {
    public static final byte[] EMPTY_BYTES = new byte[0];
	
	private final Logger logger = LoggerFactory.getLogger(ReIndexFieldData.class);
	
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
		String fieldName = getParameter();
		FieldDefinition fieldDef = tabDef.getFieldDef(fieldName);
		if (fieldDef == null || !fieldDef.isScalarField()) {
			logger.error("Scalar field {} is not defined", fieldName);
			return;
		}
		String objectStore = SpiderService.objectsStoreName(tabDef);
		String termsStore = SpiderService.termsStoreName(tabDef);
		
		// 1. Delete terms data
		deleteTerms(tabDef, fieldName);
		
		// 2. Scan objects and add terms
		m_dbTran = m_dbService.startTransaction(Tenant.getTenant(m_appDef));
		Iterator<DRow> objRows = m_dbService.getAllRowsAllColumns(Tenant.getTenant(m_appDef), objectStore);
		Set<String> fields = new HashSet<>();
		fields.add(CommonDefs.ID_FIELD);
		fields.add(fieldName);
		if (tabDef.isSharded()) {
			fields.add(tabDef.getShardingField().getName());
		}
		while (objRows.hasNext()) {
			List<String> keys = new ArrayList<String>();
			for (int count = 0; objRows.hasNext() && count < MAX_MUTATION_COUNT; count ++) {
				keys.add(objRows.next().getKey());
			}
			Iterator<DRow> data = m_dbService.getRowsColumns(Tenant.getTenant(m_appDef), objectStore, keys, fields);
			while (data.hasNext()) {
				DRow dataRow = data.next();
				String objId = null;
				String shardPrefix = "";
				Set<String> terms = new HashSet<>();
				for (Iterator<DColumn> cols = dataRow.getColumns(); cols.hasNext(); ) {
					DColumn col = cols.next();
					String name = col.getName();
					if (tabDef.isSharded() && name.equals(tabDef.getShardingField().getName())) {
						int shard = tabDef.computeShardNumber(Utils.dateFromString(col.getValue()));
						if (shard > 0) {
							shardPrefix = shard + "/";
						}
					}
					if (name.equals(fieldName)) {
						terms.addAll(SpiderHelper.getTerms(fieldName, col.getValue(), tabDef));
					}
					if (name.equals(CommonDefs.ID_FIELD)) {
						objId = col.getValue();
					}
				}
				for (String term : terms) {
					m_dbTran.addColumn(
							termsStore,
							shardPrefix + fieldName + "/" + term,
							objId, EMPTY_BYTES);
					m_dbTran.addColumn(
							termsStore,
							shardPrefix + Defs.TERMS_REGISTRY_ROW_PREFIX + "/" + fieldName,
							term, EMPTY_BYTES);
					if (isInterrupted()) {
						return;
					}
				}
				m_dbService.commit(m_dbTran);
			}
		}
	}

}
