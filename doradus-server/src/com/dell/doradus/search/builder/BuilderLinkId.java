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

import com.dell.doradus.common.Utils;
import com.dell.doradus.search.FilteredIterable;
import com.dell.doradus.search.filter.Filter;
import com.dell.doradus.search.query.IdQuery;
import com.dell.doradus.search.query.LinkCountQuery;
import com.dell.doradus.search.query.LinkIdQuery;
import com.dell.doradus.search.query.LinkQuery;
import com.dell.doradus.search.query.Query;

public class BuilderLinkId extends SearchBuilder {
    
	@Override public FilteredIterable search(Query query) {
		LinkIdQuery qu = (LinkIdQuery)query;
		//IS NULL
		if(qu.id == null) return null;
		LinkQuery linkQuery = new LinkQuery(qu.quantifier, qu.link, new IdQuery(qu.id));
		return m_searcher.search(m_params, m_table, linkQuery, m_shards);
	}
	
	@Override public Filter filter(Query query) {
		LinkIdQuery qu = (LinkIdQuery)query;
		if(qu.id == null) {
			LinkCountQuery lc = new LinkCountQuery(qu.link, 0);
			return m_searcher.filter(m_params, m_table, lc);
		}
		Utils.require(qu.id != null, "IS NULL query is not supported");
		LinkQuery linkQuery = new LinkQuery(qu.quantifier, qu.link, new IdQuery(qu.id));
		return m_searcher.filter(m_params, m_table, linkQuery);
	}
   
}
