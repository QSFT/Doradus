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

package com.dell.doradus.search;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.dell.doradus.common.CommonDefs;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.search.aggregate.Entity;
import com.dell.doradus.search.aggregate.EntitySequence;
import com.dell.doradus.search.aggregate.SortOrder;
import com.dell.doradus.search.filter.Filter;
import com.dell.doradus.search.util.HeapList;

public class FieldSetCreator {
	/**
	 * limits the number of 
	 */
	public int limit;
	public Filter filter;
	public SortOrder order;
	public TableDefinition tableDef;
	public List<String> scalarFields;
	public List<String> loadedFields;
	public TreeMap<String, FieldSetCreator> links = new TreeMap<String, FieldSetCreator>();
	public FieldSet fieldSet;
	
	public FieldSetCreator(FieldSet fieldSet, SortOrder order) {
		tableDef = fieldSet.tableDef;
		limit = fieldSet.limit;
		if(limit == -1) limit = Integer.MAX_VALUE;
		scalarFields = fieldSet.ScalarFields;
		loadedFields = scalarFields;
		this.order = order;
		this.fieldSet = fieldSet;
		
		Set<String> flds = new HashSet<String>(scalarFields.size());
		flds.addAll(scalarFields);
		if(fieldSet.filter != null) {
			filter = new QueryExecutor(tableDef).filter(fieldSet.filter);
			filter.addFields(flds);
		}
		if(order != null) {
			if(order.items.size() != 1) throw new IllegalArgumentException("Link paths not supported in sort order");
			if(!order.items.get(0).isLink) {
				flds.add(order.items.get(0).name);
			}
		}
		if(flds.contains("*")) {
			flds.clear();
			flds.add("*");
		}
		loadedFields = new ArrayList<String>(flds);
		
		for(Map.Entry<String, FieldSet> e : fieldSet.LinkFields.entrySet()) {
			links.put(e.getKey(), new FieldSetCreator(e.getValue(), null));
		}
		
	}
	
	public SearchResultList create(EntitySequence sequence, int skip) {
		int maxSize = limit;
		if (limit < Integer.MAX_VALUE - skip) {
			maxSize += skip;
		}
		
		SearchResultList resultList = new SearchResultList();
		
		if(order == null || limit == Integer.MAX_VALUE) {
	        for(Entity entity: sequence) {
	        	if(filter != null && !filter.check(entity)) continue;
	        	SearchResult result = createResult(entity);
	            resultList.results.add(result);
	            if(resultList.results.size() >= maxSize) break;
	        }
		}
		else {
			HeapList<SearchResult> results = new HeapList<SearchResult>(maxSize);
	        for(Entity entity: sequence) {
	        	if(filter != null && !filter.check(entity)) continue;
	        	SearchResult result = createResult(entity);
	            results.Add(result);
	        }
	        SearchResult[] arr = results.GetValues(SearchResult.class);
	        for(SearchResult result : arr) resultList.results.add(result);
		}
        if(limit == Integer.MAX_VALUE && order != null) Collections.sort(resultList.results);
        if (skip > 0) {
        	if (skip > resultList.results.size()) {
        		resultList.results.clear();
        	} else {
        		resultList.results.subList(0, skip).clear();
        	}
        }
        if(resultList.results.size() >= limit) resultList.continuation_token = resultList.results.get(resultList.results.size() - 1).id.toString();
        return resultList;
	}

	private SearchResult createResult(Entity entity) {
    	SearchResult result = new SearchResult();
        result.scalars.put(CommonDefs.ID_FIELD, entity.id().toString());
        result.id = entity.id();
        result.order = order;
        result.fieldSet = fieldSet;
        for(String f: scalarFields) {
            if(f.equals(CommonDefs.ID_FIELD)) continue;
            else if(f.equals("*")) {
                for(String field: entity.getAllFields()) {
                    String value = entity.get(field);
                    if(value != null) result.scalars.put(field, value);
                }
            }else {
                String v = entity.get(f);
                if(v != null) result.scalars.put(f, v);
            }
        }
        for(Map.Entry<String, FieldSetCreator> entry: links.entrySet()) {
            String linkName = entry.getKey();
            FieldSetCreator linkedSet = entry.getValue();
            EntitySequence linkedSequence = entity.getLinkedEntities(linkName, linkedSet.loadedFields);
            SearchResultList linkedResultList = linkedSet.create(linkedSequence, 0);
            result.links.put(linkName, linkedResultList);
        }
    	return result;
	}
}
