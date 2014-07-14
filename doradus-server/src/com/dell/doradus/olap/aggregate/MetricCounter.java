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
import com.dell.doradus.olap.search.Result;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.olap.store.FieldSearcher;
import com.dell.doradus.olap.store.IntIterator;
import com.dell.doradus.olap.store.NumSearcherMV;

public abstract class MetricCounter {
	
	public abstract void add(int doc, IMetricValue value);

	public static class FilteredCounter extends MetricCounter {
		private Result m_filter;
		private MetricCounter m_inner;
		public FilteredCounter(Result filter, MetricCounter inner) {
			m_filter = filter;
			m_inner = inner;
		}
		
		@Override public void add(int doc, IMetricValue value) {
			if(doc < 0 || m_filter.get(doc)) m_inner.add(doc, value);
		}
	}
	
	
	public static class Count extends MetricCounter {
		
		public Count() { }
		
		@Override public void add(int doc, IMetricValue value) {
			value.add(1);
		}
	}
	
	public static class Link extends MetricCounter {
		private Result m_filter;
		private FieldSearcher m_fs;
		private MetricCounter m_inner;
		private IntIterator m_iter;
		
		public Link(Result filter, FieldDefinition fieldDef, CubeSearcher cs, MetricCounter inner) {
			m_filter = filter;
			m_fs = cs.getFieldSearcher(fieldDef.getTableName(), fieldDef.getName());
			m_inner = inner;
			m_iter = new IntIterator();
		}
		
		@Override public void add(int doc, IMetricValue value) {
			m_fs.fields(doc, m_iter);
			for(int i = 0; i < m_iter.count(); i++) {
				int d = m_iter.get(i);
				if(m_filter != null && !m_filter.get(d)) continue;
				m_inner.add(d, value);
			}
		}
	}
	
	public static class Num extends MetricCounter {
		private NumSearcherMV m_ns;
		
		public Num(FieldDefinition fieldDef, CubeSearcher cs) {
			m_ns = cs.getNumSearcher(fieldDef.getTableName(), fieldDef.getName());
		}
		
		@Override public void add(int doc, IMetricValue value) {
			int fcount = m_ns.size((int)doc);
			for(int index = 0; index < fcount; index++) {
				value.add(m_ns.get(doc, index));
			}
		}
	}

	public static class NumCount extends MetricCounter {
		private NumSearcherMV m_ns;
		
		public NumCount(FieldDefinition fieldDef, CubeSearcher cs) {
			m_ns = cs.getNumSearcher(fieldDef.getTableName(), fieldDef.getName());
		}
		
		@Override public void add(int doc, IMetricValue value) {
			int fcount = m_ns.size((int)doc);
			value.add(fcount);
		}
	}
	
	public static class FieldCount extends MetricCounter {
		private Result m_filter;
		private FieldSearcher m_fs;
		private IntIterator m_iter;
		
		public FieldCount(Result filter, FieldDefinition fieldDef, CubeSearcher cs) {
			m_filter = filter;
			m_fs = cs.getFieldSearcher(fieldDef.getTableName(), fieldDef.getName());
			m_iter = new IntIterator();
		}
		
		@Override public void add(int doc, IMetricValue value) {
			m_fs.fields(doc, m_iter);
			int val = 0;
			if(m_filter == null) val = m_iter.count();
			else {
				for(int i = 0; i < m_iter.count(); i++) {
					int d = m_iter.get(i);
					if(m_filter != null && !m_filter.get(d)) continue;
					val++;
				}
			}
			value.add(val);
		}
	}
	
	public static class FieldValue extends MetricCounter {
		private Result m_filter;
		private FieldSearcher m_fs;
		private IntIterator m_iter;
		
		public FieldValue(Result filter, FieldDefinition fieldDef, CubeSearcher cs) {
			m_filter = filter;
			m_fs = cs.getFieldSearcher(fieldDef.getTableName(), fieldDef.getName());
			m_iter = new IntIterator();
		}
		
		@Override public void add(int doc, IMetricValue value) {
			m_fs.fields(doc, m_iter);

			for(int i = 0; i < m_iter.count(); i++) {
				int d = m_iter.get(i);
				if(m_filter != null && !m_filter.get(d)) continue;
				value.add(d);
			}
		}
	}
	

}
