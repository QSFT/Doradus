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

package com.dell.doradus.olap.search;

import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.olap.store.IdSearcher;
import com.dell.doradus.search.util.HeapList;

public class IdFieldCollector extends AggregationCollector {
	private CubeSearcher m_searcher;
	private TableDefinition m_table;
	
	private int[] m_counts;
	private int[] m_lastDocs;

	public IdFieldCollector(CubeSearcher searcher, TableDefinition table) {
		m_searcher = searcher;
		m_table = table;
		int docs = searcher.getDocs(table.getTableName());
		m_counts = new int[docs];
		m_lastDocs = new int[docs];
		for(int i = 0; i < m_lastDocs.length; i++) m_lastDocs[i] = -1;
	}
	
	@Override public void collect(int doc, int value) {
		if(m_lastDocs[value] == doc) return;
		m_lastDocs[value] = doc;
		m_counts[value]++;
	}

	@Override public GroupResult getResult(int top) {
		GroupResult groupResult = new GroupResult();
		groupResult.isSortByCount = top > 0;
		IdSearcher id_searcher = m_searcher.getIdSearcher(m_table.getTableName());
		if(!groupResult.isSortByCount) {
			for(int i = 0; i < m_counts.length; i++) {
				if(m_counts[i] == 0) continue;
				groupResult.groupsCount++;
				GroupCount gc = new GroupCount(id_searcher.getId(i).toString(), m_counts[i]);
				groupResult.groups.add(gc);
			}
		}
		else {
			HeapList<NumCount> hs = new HeapList<NumCount>(top);
			for(int i = 0; i < m_counts.length; i++) {
				if(m_counts[i] == 0) continue;
				groupResult.groupsCount++;
				hs.Add(new NumCount(i, m_counts[i]));
			}
			for(NumCount nc : hs.GetValues(NumCount.class)) {
				GroupCount gc = new GroupCount(id_searcher.getId(nc.num).toString(), nc.count);
				groupResult.groups.add(gc);
			}
		}
		return groupResult;
	}
}

