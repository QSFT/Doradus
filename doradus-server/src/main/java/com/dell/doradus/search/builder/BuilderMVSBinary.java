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
import com.dell.doradus.search.FilteredIterable;
import com.dell.doradus.search.filter.Filter;
import com.dell.doradus.search.filter.FilterMVSContains;
import com.dell.doradus.search.filter.FilterMVSEquals;
import com.dell.doradus.search.filter.FilterNot;
import com.dell.doradus.search.filter.FilterOr;
import com.dell.doradus.search.query.BinaryQuery;
import com.dell.doradus.search.query.LinkQuery;
import com.dell.doradus.search.query.MVSBinaryQuery;
import com.dell.doradus.search.query.Query;

public class BuilderMVSBinary extends SearchBuilder {
    
	@Override public FilteredIterable search(Query query) {
		MVSBinaryQuery qu = (MVSBinaryQuery)query;
    	if(!LinkQuery.ANY.equals(qu.quantifier)) return null;
    	else return m_searcher.search(m_params, m_table, qu.innerQuery);
	}
	
	@Override public Filter filter(Query query) {
		MVSBinaryQuery qu = (MVSBinaryQuery)query;
    	if(qu.innerQuery.field == null || qu.innerQuery.field.length() == 0 || qu.innerQuery.field.equals("*")) {
    		throw new IllegalArgumentException("all-fields MVS query not supported");
    	}
    	
		FieldDefinition f = m_table.getFieldDef(qu.innerQuery.field);
		if(f != null && f.isGroupField()) {
			FilterOr filter = new FilterOr();
			for(FieldDefinition nested : f.getNestedFields()) {
				if(!nested.isScalarField()) continue;
				Filter inner = null;
		        if(BinaryQuery.EQUALS.equals(qu.innerQuery.operation)) {
		            inner = new FilterMVSEquals(nested.getName(), qu.innerQuery.value, qu.quantifier);
		        } else if(BinaryQuery.CONTAINS.equals(qu.innerQuery.operation)) {
		            inner = new FilterMVSContains(nested.getName(), qu.innerQuery.value, qu.quantifier);
		        } else throw new IllegalArgumentException("operation " + qu.innerQuery.operation + " not supported");
				if(LinkQuery.ALL.equals(qu.quantifier)) inner = new FilterNot(inner); 
		        filter.add(inner);
			}
			if(!LinkQuery.ANY.equals(qu.quantifier)) return new FilterNot(filter);
			else return filter;
		}
    	
    	
        if(BinaryQuery.EQUALS.equals(qu.innerQuery.operation)) {
            return new FilterMVSEquals(qu.innerQuery.field, qu.innerQuery.value, qu.quantifier);
        } else if(BinaryQuery.CONTAINS.equals(qu.innerQuery.operation)) {
            return new FilterMVSContains(qu.innerQuery.field, qu.innerQuery.value, qu.quantifier);
        } else throw new IllegalArgumentException("operation " + qu.innerQuery.operation + " not supported"); 
	}
   
}
