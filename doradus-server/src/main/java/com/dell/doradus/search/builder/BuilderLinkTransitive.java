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
import com.dell.doradus.search.SearchParameters;
import com.dell.doradus.search.filter.Filter;
import com.dell.doradus.search.filter.FilterLinkTransitive;
import com.dell.doradus.search.query.Query;
import com.dell.doradus.search.query.TransitiveLinkQuery;

public class BuilderLinkTransitive extends SearchBuilder {
    
	@Override public FilteredIterable search(Query query) {
		return null;
	}
	
	@Override public Filter filter(Query query) {
		TransitiveLinkQuery qu = (TransitiveLinkQuery)query;
    	SearchParameters newparams = new SearchParameters();
    	newparams.l2r = m_params.l2r;
    	Filter inner = m_searcher.filter(newparams, m_table, qu.getInnerQuery());
        FilterLinkTransitive condition = new FilterLinkTransitive(m_searcher, m_table, qu, inner);
        return condition;
	}
   
}
