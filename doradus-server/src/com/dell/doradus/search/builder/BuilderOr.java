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

package com.dell.doradus.search.builder;

import com.dell.doradus.search.FilteredIterable;
import com.dell.doradus.search.filter.Filter;
import com.dell.doradus.search.filter.FilterOr;
import com.dell.doradus.search.iterator.OrIterable;
import com.dell.doradus.search.query.OrQuery;
import com.dell.doradus.search.query.Query;

public class BuilderOr extends SearchBuilder {
    
	@Override public FilteredIterable search(Query query) {
		OrQuery qu = (OrQuery)query;
		OrIterable iter = new OrIterable(qu.subqueries.size());
		for(Query q: qu.subqueries) {
	        FilteredIterable seq = m_searcher.search(m_params, m_table, q);
	        iter.add(seq);
		}
		return create(iter, null);
	}

	@Override public Filter filter(Query query) {
		OrQuery qu = (OrQuery)query;
		FilterOr filter = new FilterOr();
		for(Query q: qu.subqueries) {
			filter.add(m_searcher.filter(m_params, m_table, q));
		}
		return filter;
	}
   
}
