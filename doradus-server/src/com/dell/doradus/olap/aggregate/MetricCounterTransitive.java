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

package com.dell.doradus.olap.aggregate;

import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.olap.collections.BdLongSet;
import com.dell.doradus.olap.search.Result;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.olap.store.FieldSearcher;
import com.dell.doradus.olap.store.IntIterator;

public abstract class MetricCounterTransitive extends MetricCounter {
	protected Result m_filter;
	protected FieldSearcher m_fs;
	protected IntIterator m_iter;
	protected BdLongSet m_set;
	protected int m_depth;
	
	public abstract void addInternal(IMetricValue value);
	
	public MetricCounterTransitive(Result filter, FieldDefinition fieldDef, int depth, CubeSearcher cs) {
		m_filter = filter;
		m_fs = cs.getFieldSearcher(fieldDef.getTableName(), fieldDef.getName());
		m_iter = new IntIterator();
		m_set = new BdLongSet(1024);
		m_set.enableClearBuffer();
		m_depth = Math.min(depth, 1024);
		if(m_depth == 0) m_depth = 1024;
	}
	
	@Override public void add(int doc, IMetricValue value) {
		m_set.clear();
		m_set.add(doc);
		int last_size = 0;
		
		for(int depth = 0; depth < m_depth; depth++) {
			int current_size = m_set.size(); 
			if(current_size == last_size) break;
			for(int i = last_size; i < current_size; i++) {
				doc = (int)m_set.get(i);
				m_fs.fields(doc, m_iter);
				for(int j = 0; j < m_iter.count(); j++) {
					int d = m_iter.get(j);
					if(m_filter != null && !m_filter.get(d)) continue;
					m_set.add(d);
				}
			}
			last_size = current_size;
		}
		
		addInternal(value);
		
		m_set.clear();
	}
	
	
	public static class TransitiveLink extends MetricCounterTransitive {
		private MetricCounter m_inner;
		
		public TransitiveLink(Result filter, FieldDefinition fieldDef, int depth, CubeSearcher cs, MetricCounter inner) {
			super(filter, fieldDef, depth, cs);
			m_inner = inner;
		}
		
		@Override public void addInternal(IMetricValue value) {
			//m_set.sort();
			//start with 1 because original doc resides at index 0 
			for(int i = 1; i < m_set.size(); i++) {
				int d = (int)m_set.get(i);
				m_inner.add(d, value);
			}
		}
	}
	
	public static class TransitiveLinkCount extends MetricCounterTransitive {
		
		public TransitiveLinkCount(Result filter, FieldDefinition fieldDef, int depth, CubeSearcher cs) {
			super(filter, fieldDef, depth, cs);
		}
		
		@Override public void addInternal(IMetricValue value) {
			if(m_set.size() <= 1) return;
			value.add(m_set.size() - 1);
		}
	}
	
	public static class TransitiveLinkValue extends MetricCounterTransitive {
		
		public TransitiveLinkValue(Result filter, FieldDefinition fieldDef, int depth, CubeSearcher cs) {
			super(filter, fieldDef, depth, cs);
		}
		
		@Override public void addInternal(IMetricValue value) {
			for(int i = 1; i < m_set.size(); i++) {
				int d = (int)m_set.get(i);
				value.add(d);
			}
		}
	}

}
