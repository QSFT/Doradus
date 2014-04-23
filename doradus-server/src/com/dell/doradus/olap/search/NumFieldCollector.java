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

public class NumFieldCollector extends AggregationCollector {
	private NumSearcher m_num_searcher;
	private int[] m_counts;
	private int[] m_lastDocs;
	private boolean m_isPositiveOnly; 
	private long m_min;
	private long m_max;

	public NumFieldCollector(CubeSearcher searcher, FieldDefinition field) {
		m_num_searcher = searcher.getNumSearcher(field.getTableName(), field.getName());
		m_isPositiveOnly = m_num_searcher.min() == 0;
		m_min = m_isPositiveOnly ? m_num_searcher.minPos() : m_num_searcher.min();
		m_max = m_num_searcher.max();
		long cnt = m_isPositiveOnly ? m_max - m_min + 2 : m_max - m_min + 1;
		if(cnt > 100000) throw new IllegalArgumentException("Numeric range too big; use BATCH(...) instead");
		m_counts = new int[(int)cnt];
		m_lastDocs = new int[(int)cnt];
		for(int i = 0; i < m_lastDocs.length; i++) m_lastDocs[i] = -1;
	}
	
	@Override public void collect(int doc, int value) {
		if(m_num_searcher.isNull(value)) return; 
		long v = m_num_searcher.get(value);
		if(m_isPositiveOnly) {
			if(v != 0) value = (int)(v - m_min + 1);
			else value = 0;
		} else {
			value = (int)(v - m_min);
		}
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
				String v = null;
				if(m_isPositiveOnly) {
					if(i == 0) v = "0";
					else v = "" + (i + m_min - 1);
				} else v = "" + (i + m_min);
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
				if(m_isPositiveOnly) {
					if(nc.num == 0) v = "0";
					else v = "" + (nc.num + m_min - 1);
				} else v = "" + (nc.num + m_min);
				GroupCount gc = new GroupCount(v, nc.count);
				groupResult.groups.add(gc);
			}
		}
		return groupResult;
	}
}

