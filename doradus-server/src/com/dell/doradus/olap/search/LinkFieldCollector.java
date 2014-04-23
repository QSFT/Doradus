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
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.olap.store.FieldSearcher;
import com.dell.doradus.olap.store.IntIterator;
import com.dell.doradus.search.query.Query;

public class LinkFieldCollector extends AggregationCollector {
	private FieldSearcher m_link_searcher;
	private Result m_filter;
	private AggregationCollector m_inner;
	private IntIterator m_iter;

	public LinkFieldCollector(CubeSearcher searcher, FieldDefinition link, Query filter, AggregationCollector inner) {
		m_link_searcher = searcher.getFieldSearcher(link.getTableName(), link.getName());
		if(filter != null) {
			TableDefinition projectedTable = link.getTableDef().getAppDef().getTableDef(link.getLinkExtent());
			m_filter = ResultBuilder.search(projectedTable, filter, searcher);
		}
		m_inner = inner;
		m_iter = new IntIterator();
	}
	
	@Override public void collect(int doc, int value) {
		m_link_searcher.fields(value, m_iter);
		for(int i = 0; i < m_iter.count(); i++) {
			int d = m_iter.get(i);
			if(m_filter != null && !m_filter.get(d)) continue;
			m_inner.collect(doc, d);
		}
	}

	@Override public GroupResult getResult(int top) {
		return m_inner.getResult(top);
	}
}

