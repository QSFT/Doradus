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
import java.util.Set;

import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.core.Defs;
import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.spider.SpiderService;

public abstract class FixDataTask extends DoradusTask {
	
	protected DBService m_dbService = DBService.instance();
	protected DBTransaction m_dbTran = m_dbService.startTransaction();
	protected final int MAX_MUTATION_COUNT = ServerConfig.getInstance().batch_mutation_threshold;

	protected void deleteTerms(TableDefinition tabDef, String fieldName) {
		String termsStore = SpiderService.termsStoreName(tabDef);
		
		// 1. Deleting terms from unsharded rows
		Iterator<DColumn> iTerms = m_dbService.getAllColumns(
				termsStore,  Defs.TERMS_REGISTRY_ROW_PREFIX + "/" + fieldName);
		if (iTerms != null) {
			while (iTerms.hasNext()) {
				for (int count = 0; iTerms.hasNext() && count < MAX_MUTATION_COUNT; ++count) {
					m_dbTran.deleteRow(termsStore, fieldName + "/" + iTerms.next().getName());
					if (isInterrupted()) {
						return;
					}
				}
				m_dbService.commit(m_dbTran);
			}
			m_dbTran.deleteRow(termsStore, Defs.TERMS_REGISTRY_ROW_PREFIX + "/" + fieldName);
			m_dbService.commit(m_dbTran);
		}
		
		// 2. Same for sharded rows
		Set<Integer> shardsSet = SpiderService.instance().getShards(tabDef).keySet();
		for (Integer shard : shardsSet) {
			iTerms = m_dbService.getAllColumns(
					termsStore,  shard + "/" + Defs.TERMS_REGISTRY_ROW_PREFIX + "/" + fieldName);
			if (iTerms != null) {
				while (iTerms.hasNext()) {
					for (int count = 0; iTerms.hasNext() && count < MAX_MUTATION_COUNT; ++count) {
						m_dbTran.deleteRow(termsStore, shard + "/" + fieldName + "/" + iTerms.next().getName());
						if (isInterrupted()) {
							return;
						}
					}
					m_dbService.commit(m_dbTran);
				}
				m_dbTran.deleteRow(termsStore, shard + "/" + Defs.TERMS_REGISTRY_ROW_PREFIX + "/" + fieldName);
				m_dbService.commit(m_dbTran);
			}
		}
	}
}
