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

import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.search.FilteredIterable;
import com.dell.doradus.search.SearchParameters;
import com.dell.doradus.search.filter.Filter;
import com.dell.doradus.search.filter.FilterAll;
import com.dell.doradus.search.filter.FilterLink;
import com.dell.doradus.search.iterator.LinksIterable;
import com.dell.doradus.search.query.LinkQuery;
import com.dell.doradus.search.query.Query;

public class BuilderLink extends SearchBuilder {
    
	@Override public FilteredIterable search(Query query) {
		LinkQuery qu = (LinkQuery)query;
    	if(!LinkQuery.ANY.equals(qu.quantifier)) return null;
    	FieldDefinition link = m_table.getFieldDef(qu.link);
    	if(link.isGroupField()) return null;
    	TableDefinition extent = m_table.getAppDef().getTableDef(link.getLinkExtent());
    	FieldDefinition inverse = extent.getFieldDef(link.getLinkInverse());
    	
    	SearchParameters newparams = new SearchParameters();
    	newparams.l2r = m_params.l2r;
    	FilteredIterable inner = m_searcher.search(newparams, extent, qu.getInnerQuery());
    	// SHARD OPTIMIZATION
    	if(inverse.isSharded()) {
            LinksIterable seq = new LinksIterable(inverse,
            		m_shards, m_params.continuation, m_params.inclusive, inner);
            return create(seq, null);
    	} else {
	        LinksIterable seq = new LinksIterable(inverse, null,
	        		m_params.continuation, m_params.inclusive, inner);
	        return create(seq, null);
    	}
	}
	
	@Override public Filter filter(Query query) {
		LinkQuery qu = (LinkQuery)query;
    	FieldDefinition link = m_table.getFieldDef(qu.link);
    	if(link.isGroupField()) {
    		for(FieldDefinition l : link.getNestedFields()) {
    			if(l.isLinkField()) {
    				link = l;
    				break;
    			}
    		}
    	}
    	TableDefinition extent = m_table.getAppDef().getTableDef(link.getLinkExtent());
    	SearchParameters newparams = new SearchParameters();
    	newparams.l2r = m_params.l2r;
    	Filter inner = m_searcher.filter(newparams, extent, qu.getInnerQuery());
    	if(inner == null) inner = new FilterAll();
		FilterLink condition = new FilterLink(m_searcher, m_table, qu, inner);
        return condition;
	}
   
}
