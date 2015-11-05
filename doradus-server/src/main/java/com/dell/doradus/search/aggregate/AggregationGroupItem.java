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

package com.dell.doradus.search.aggregate;

import java.util.List;

import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.search.query.Query;

public class AggregationGroupItem {
   public boolean  isLink;
   public boolean  isID;
   public TableDefinition tableDef;
   public String name;
   public Query query;
   public FieldDefinition fieldDef;
   // transitive support
   // defines whether the link is transitive
   public boolean isTransitive;
   //defines the depth. 0 means unlimited depth
   public int transitiveDepth;

    //Nested links description if this item represents group
    public List<LinkInfo> nestedLinks;

    //temporary placeholder for XLink handling. TODO: remove from here
    public Object xlinkContext;
    
    @Override public boolean equals(Object obj) {
    	AggregationGroupItem item = (AggregationGroupItem)obj;
    	String q1 = query != null ? query.toString() : "";
    	String q2 = item.query != null ? item.query.toString() : "";
    	return name.equals(item.name) && q1.equals(q2);
    };
    
    @Override public String toString() {
    	String str = name;
    	if(isTransitive) str += "^";
    	if(transitiveDepth != 0) str += "[" + transitiveDepth + "]";
    	if(query != null) str += ".WHERE(" + query.toString() + ")";
		return str;
	}
}
