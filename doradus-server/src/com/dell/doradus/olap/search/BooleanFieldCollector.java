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

import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.olap.store.NumSearcher;
import com.dell.doradus.search.util.HeapList;

public class BooleanFieldCollector extends AggregationCollector {
	private NumSearcher m_num_searcher;
	private int[] m_counts;
	private int[] m_lastDocs;

	public BooleanFieldCollector(CubeSearcher searcher, FieldDefinition field) {
		m_num_searcher = searcher.getNumSearcher(field.getTableName(), field.getName());
		m_counts = new int[2];
		m_lastDocs = new int[2];
		for(int i = 0; i < m_lastDocs.length; i++) m_lastDocs[i] = -1;
	}
	
	@Override public void collect(int doc, int value) {
		if(m_num_searcher.isNull(value)) return; 
		long v = m_num_searcher.get(value);
		value = (int) v;
		if(m_lastDocs[value] == doc) return;
		m_lastDocs[value] = doc;
		m_counts[value]++;
	}

	@Override public GroupResult getResult(int top) {
		GroupResult groupResult = new GroupResult();
		groupResult.isSortByCount = top > 0;
		if(!groupResult.isSortByCount) {
			for(int i = 0; i < m_counts.length; i++) {
				if(m_counts[i] == 0) continue;
				groupResult.groupsCount++;
				String v = i == 0 ? "false" : "true";
				GroupCount gc = new GroupCount(v, m_counts[i]);
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
				String v = nc.num == 0 ? "false" : "true";
				GroupCount gc = new GroupCount(v, nc.count);
				groupResult.groups.add(gc);
			}
		}
		return groupResult;
	}
}

