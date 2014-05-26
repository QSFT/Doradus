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

package com.dell.doradus.olap.search;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.search.FieldSet;
import com.dell.doradus.search.aggregate.SortOrder;

public class FieldSetCreator {
	public int limit;
	public Result filter;
	public SortOrder order;
	public TableDefinition tableDef;
	public List<String> scalarFields;
	public TreeMap<String, List<FieldSetCreator>> links = new TreeMap<String, List<FieldSetCreator>>();
	public FieldSet fieldSet;
	
	public FieldSetCreator(CubeSearcher searcher, FieldSet fieldSet, SortOrder order) {
		tableDef = fieldSet.tableDef;
		limit = fieldSet.limit;
		if(limit == -1) limit = Integer.MAX_VALUE;
		scalarFields = fieldSet.ScalarFields;
		this.order = order;
		this.fieldSet = fieldSet;

		if(fieldSet.filter != null) {
			filter = ResultBuilder.search(tableDef, fieldSet.filter, searcher);
		}
		
		for(String linkName: fieldSet.getLinks()) {
			List<FieldSet> linkSetList = fieldSet.getLinks(linkName);
			List<FieldSetCreator> creatorList = new ArrayList<FieldSetCreator>();
			links.put(linkName, creatorList);
			for(FieldSet linkSet: linkSetList) {
				creatorList.add(new FieldSetCreator(searcher, linkSet, null));
			}
		}
		
	}
	
}
