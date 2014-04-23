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

import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.search.FieldSet;
import com.dell.doradus.search.SearchResultList;
import com.dell.doradus.search.aggregate.AggregationGroup;
import com.dell.doradus.search.aggregate.AggregationMetric;
import com.dell.doradus.search.aggregate.SortOrder;
import com.dell.doradus.search.parser.AggregationQueryBuilder;
import com.dell.doradus.search.parser.DoradusQueryBuilder;
import com.dell.doradus.search.query.Query;

public class Searcher {
	
	
	// for unit tests
	public static SearchResultList search(CubeSearcher searcher, TableDefinition tableDef, String query, String fields, int size, SortOrder sortOrder) {
    	Query qu = DoradusQueryBuilder.Build(query, tableDef);
    	FieldSet fieldSet = new FieldSet(tableDef, fields);
    	Result documents = ResultBuilder.search(tableDef, qu, searcher);
		SearchResultList list = SearchResultBuilder.build(searcher, documents, fieldSet, size, sortOrder);
		return list;
	}

	
	// for unit tests
	public static GroupResult aggregate(CubeSearcher searcher, TableDefinition tableDef, String query, String fields, String metric) {
    	Query qu = DoradusQueryBuilder.Build(query, tableDef);
    	Result documents = ResultBuilder.search(tableDef, qu, searcher);
    	ArrayList<ArrayList<AggregationGroup>> groupsSet = AggregationGroup.GetAggregationList(fields, tableDef);
    	AggregationMetric aggMetric = AggregationQueryBuilder.BuildAggregationMetric(metric, tableDef);
    	return AggregationBuilder.aggregate(searcher, documents, groupsSet.get(0).get(0), aggMetric);
	}
	
	
	public static SearchResultList search(CubeSearcher searcher, TableDefinition tableDef, Query query, FieldSet fieldSet, int size, SortOrder sortOrder) {
    	Result documents = ResultBuilder.search(tableDef, query, searcher);
		SearchResultList list = SearchResultBuilder.build(searcher, documents, fieldSet, size, sortOrder);
		return list;
	}

	
	public static GroupResult aggregate(CubeSearcher searcher, TableDefinition tableDef, Query query, AggregationGroup group, AggregationMetric metric) {
    	Result documents = ResultBuilder.search(tableDef, query, searcher);
    	GroupResult r = AggregationBuilder.aggregate(searcher, documents, group, metric);
    	return r;
	}
	
}
