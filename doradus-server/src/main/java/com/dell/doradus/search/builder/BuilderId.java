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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.dell.doradus.core.ObjectID;
import com.dell.doradus.search.FilteredIterable;
import com.dell.doradus.search.IDHelper;
import com.dell.doradus.search.aggregate.Entity;
import com.dell.doradus.search.filter.Filter;
import com.dell.doradus.search.query.IdQuery;
import com.dell.doradus.search.query.Query;
import com.dell.doradus.service.spider.SpiderHelper;

public class BuilderId extends SearchBuilder {
    
	@Override public FilteredIterable search(Query query) {
		IdQuery qu = (IdQuery)query;
        ArrayList<ObjectID> ids = new ArrayList<ObjectID>(1);
        ids.add(IDHelper.createID(qu.id));
        List<String> fields = new ArrayList<String>(1);
        fields.add("_ID");
        Map<ObjectID, Map<String, String>> result = SpiderHelper.getScalarValues(m_table, ids, fields);
        ids.clear();
        for(ObjectID key: result.keySet()) {
        	if(result.get(key).size() == 0) continue;
        	ids.add(key);
        }
        //ids.addAll(result.keySet());
        return create(ids, null);
	}
	
	@Override public Filter filter(Query query) {
		IdQuery qu = (IdQuery)query;
        final ObjectID id = IDHelper.createID(qu.id.toString());
        return new Filter() {
            @Override public boolean check(Entity entity) {
                return id.equals(entity.id());
            }

			@Override public void addFields(Set<String> fields) {}
		};
	}
   
}
