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
import com.dell.doradus.search.filter.FilterNot;
import com.dell.doradus.search.iterator.AndNotIterable;
import com.dell.doradus.search.query.NotQuery;
import com.dell.doradus.search.query.Query;

public class BuilderNot extends SearchBuilder {
    
	@Override public FilteredIterable search(Query query) {
		NotQuery qu = (NotQuery)query;
		FilteredIterable seq = m_searcher.search(m_params, m_table, qu.innerQuery);
		return create(new AndNotIterable(all(), seq), null);
	}

	@Override public Filter filter(Query query) {
		NotQuery qu = (NotQuery)query;
		return new FilterNot(m_searcher.filter(m_params, m_table, qu.innerQuery));
	}
   
}
