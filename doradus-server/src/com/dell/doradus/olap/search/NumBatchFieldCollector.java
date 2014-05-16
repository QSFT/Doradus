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

import java.util.List;

import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.olap.store.NumSearcherMV;
import com.dell.doradus.search.util.HeapList;

public class NumBatchFieldCollector extends AggregationCollector {
	private NumSearcherMV m_num_searcher;
	private int[] m_counts;
	private int[] m_lastDocs;
	private long[] m_batches;

	public NumBatchFieldCollector(CubeSearcher searcher, FieldDefinition field, List<? extends Object> batches) {
		m_num_searcher = searcher.getNumSearcher(field.getTableName(), field.getName());
		m_batches = new long[batches.size()];
		for(int i = 0; i < m_batches.length; i++) {
			m_batches[i] = Long.parseLong(batches.get(i).toString());
		}
		m_counts = new int[batches.size() + 1];
		m_lastDocs = new int[batches.size() + 1];
		for(int i = 0; i < m_lastDocs.length; i++) m_lastDocs[i] = -1;
	}
	
	@Override public void collect(int doc, int value) {
		int fcount = m_num_searcher.size(value);
		for(int index = 0; index < fcount; index++) {
			long v = m_num_searcher.get(value, index);
			int pos = 0;
			while(pos < m_batches.length && m_batches[pos] <= v) pos++;
			if(m_lastDocs[pos] == doc) return;
			m_lastDocs[pos] = doc;
			m_counts[pos]++;
		}
	}

	@Override public GroupResult getResult(int top) {
		GroupResult groupResult = new GroupResult();
		groupResult.isSortByCount = top > 0;
		if(!groupResult.isSortByCount) {
			for(int i = 0; i < m_counts.length; i++) {
				groupResult.groupsCount++;
				String v = null;
				if(i == 0) v = "< " + m_batches[0];
				else if(i == m_batches.length) v = ">= " + m_batches[i - 1];
				else v = m_batches[i - 1] + " - " + m_batches[i]; 
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
				String v = null;
				if(nc.num == 0) v = "< " + m_batches[0];
				else if(nc.num == m_batches.length) v = ">= " + m_batches[nc.num - 1];
				else v = m_batches[nc.num - 1] + " - " + m_batches[nc.num]; 
				GroupCount gc = new GroupCount(v, nc.count);
				groupResult.groups.add(gc);
			}
		}
		return groupResult;
	}
}

