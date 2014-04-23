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
import com.dell.doradus.search.filter.FilterDatePart;
import com.dell.doradus.search.query.BinaryQuery;
import com.dell.doradus.search.query.DatePartBinaryQuery;
import com.dell.doradus.search.query.Query;

public class BuilderDatePartBinary extends SearchBuilder {
    
	@Override public FilteredIterable search(Query query) {
		return null;
	}
	
	@Override public Filter filter(Query query) {
		DatePartBinaryQuery qu = (DatePartBinaryQuery)query;
    	if(qu.innerQuery.field == null || qu.innerQuery.field.length() == 0 || qu.innerQuery.field.equals("*")) {
    		throw new IllegalArgumentException("all-fields DatePart query not supported");
    	}
        if(!BinaryQuery.EQUALS.equals(qu.innerQuery.operation) && !BinaryQuery.CONTAINS.equals(qu.innerQuery.operation)) {
    		throw new IllegalArgumentException("unknown operation in DatePart query: "+qu.innerQuery.operation);
        }
        
        return new FilterDatePart(qu.part, qu.innerQuery.field, qu.innerQuery.value);
	}
   
}
