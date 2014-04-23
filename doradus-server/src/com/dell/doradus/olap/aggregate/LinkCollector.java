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

public class LinkCollector implements IFieldCollector {
	private Result m_filter;
	private FieldSearcher m_searcher;
	private IFieldCollector m_inner;
	private IntIterator m_iter;
	
	@Override public void reset(MetricCollectorSet collectorSet) { m_inner.reset(collectorSet); }
	@Override public AggregationResult getResult() { return m_inner.getResult(); }
	
	public LinkCollector(Result filter, FieldDefinition fieldDefinition, CubeSearcher cubeSearcher, IFieldCollector inner) {
		m_filter = filter;
		m_searcher = cubeSearcher.getFieldSearcher(fieldDefinition.getTableName(), fieldDefinition.getName());
		m_inner = inner;
		m_iter = new IntIterator();
	}

	@Override public void add(int doc, int field, MetricValueSet valueSet) {
		m_searcher.fields(field, m_iter);
		for(int i = 0; i < m_iter.count(); i++) {
			int d = m_iter.get(i);
			if(m_filter != null && !m_filter.get(d)) continue;
			m_inner.add(doc, d, valueSet);
		}
	}

}
