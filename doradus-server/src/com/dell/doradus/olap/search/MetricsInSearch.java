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

import java.util.HashMap;

import com.dell.doradus.common.AggregateResult;
import com.dell.doradus.common.AggregateResult.AggGroup;
import com.dell.doradus.common.AggregateResult.AggGroupSet;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.olap.Olap;
import com.dell.doradus.olap.OlapAggregate;
import com.dell.doradus.olap.OlapQuery;
import com.dell.doradus.olap.aggregate.AggregateResultConverter;
import com.dell.doradus.olap.aggregate.AggregationResult;
import com.dell.doradus.search.SearchResult;
import com.dell.doradus.search.SearchResultList;

public class MetricsInSearch {
	public static void addMetricsInSearch(Olap olap, TableDefinition tableDef, SearchResultList result, OlapQuery olapQuery) {
		if(olapQuery.getMetrics() == null) return;
		if(result.results.size() == 0) return;
		//List<MetricExpression> metrics = AggregationQueryBuilder.BuildAggregationMetricsExpression(metric, tableDef);
		StringBuilder ids = new StringBuilder();
		for(SearchResult r: result.results) {
			ids.append("'");
			ids.append(r.id().replace("'", "\'"));
			ids.append("'");
			ids.append(",");
		}
		ids.setLength(ids.length() - 1);
		
		String query = "(" + olapQuery.getOriginalQuery() + ") AND _ID IN(" + ids.toString() + ")";
		
		OlapAggregate aggregate = new OlapAggregate("x", query, "_ID", olapQuery.getMetrics(), olapQuery.getPair());
		aggregate.setShards(olapQuery.getShards(), olapQuery.getShardsRange());
		aggregate.setXShards(olapQuery.getXShards(), olapQuery.getXShardsRange());
		AggregationResult aresult = olap.aggregate(tableDef.getAppDef(), tableDef.getTableName(), aggregate);
		AggregateResult r = AggregateResultConverter.create(aresult, aggregate);
		for(AggGroupSet groupSet: r.getGroupsets()) {
			HashMap<String, String> values = new HashMap<String, String>();
			String metricName = groupSet.getMetricParam();
			for(AggGroup group: groupSet.getGroups()) {
				values.put(group.getFieldValue(), group.getGroupValue());
			}
			for(SearchResult sr: result.results) {
				String value = values.get(sr.id());
				if(value != null) sr.scalars.put(metricName, value);
			}
			
		}
	}
}








