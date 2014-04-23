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
import com.dell.doradus.common.Utils;
import com.dell.doradus.search.FilteredIterable;
import com.dell.doradus.search.filter.Filter;
import com.dell.doradus.search.filter.FilterLinkCountRange;
import com.dell.doradus.search.query.LinkCountRangeQuery;
import com.dell.doradus.search.query.Query;

public class BuilderLinkCountRange extends SearchBuilder {
    
	@Override public FilteredIterable search(Query query) {
		return null;
	}
	
	@Override public Filter filter(Query query) {
		LinkCountRangeQuery qu = (LinkCountRangeQuery)query;
		FieldDefinition fieldDef = m_table.getFieldDef(qu.link);
		Utils.require(fieldDef != null, qu.link + " not found in " + m_table.getTableName());
		Utils.require(fieldDef.isLinkField(), qu.link + " is not a link field");
        FilterLinkCountRange condition = new FilterLinkCountRange(qu.link, qu.range);
        return condition;
	}
   
}
