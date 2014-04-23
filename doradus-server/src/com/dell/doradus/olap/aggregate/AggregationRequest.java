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

package com.dell.doradus.olap.aggregate;

import java.util.ArrayList;
import java.util.List;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.olap.Olap;
import com.dell.doradus.olap.xlink.XLinkContext;
import com.dell.doradus.olap.xlink.XLinkGroupContext;
import com.dell.doradus.olap.xlink.XLinkMetricContext;
import com.dell.doradus.search.aggregate.AggregationGroup;
import com.dell.doradus.search.aggregate.AggregationMetric;
import com.dell.doradus.search.aggregate.MetricExpression;
import com.dell.doradus.search.aggregate.AggregationGroup.Selection;
import com.dell.doradus.search.parser.AggregationQueryBuilder;
import com.dell.doradus.search.parser.DoradusQueryBuilder;
import com.dell.doradus.search.query.Query;

public class AggregationRequest {
	public Olap olap;
	public String application;
	public ApplicationDefinition appDef;
	public TableDefinition tableDef;
	public List<String> shards;
	public List<String> xshards;
	public List<MetricExpression> metrics;
	public Part[] parts;
	
	public static class Part {
		public Query query;
		public List<AggregationGroup> groups;
		public AggregationGroup getSingleGroup() {
			if(groups == null || groups.size() == 0) return null;
			Utils.require(groups.size() == 1, "More than one group present");
			return groups.get(0);
		}
	}
	
	public AggregationRequest(Olap olap, AggregationRequestData requestData) {
		this.olap = olap;
		this.application = requestData.application;
		ApplicationDefinition appDef = olap.getApplicationDefinition(requestData.application);
		Utils.require(appDef != null, "Application " + requestData.application + " not found");
		tableDef = appDef.getTableDef(requestData.table);
		Utils.require(tableDef != null, "Table " + requestData.table + " not found");
		shards = requestData.shards;
		xshards = requestData.xshards;
		metrics = AggregationQueryBuilder.BuildAggregationMetricsExpression(requestData.metrics, tableDef);
		
		parts = new AggregationRequest.Part[requestData.parts.length];
		for(int i = 0; i < parts.length; i++) {
			parts[i] = new AggregationRequest.Part();
			parts[i].query = DoradusQueryBuilder.Build(requestData.parts[i].query, tableDef);
			if(requestData.parts[i].field != null) {
	    		ArrayList<ArrayList<AggregationGroup>> groupsSet = AggregationGroup.GetAggregationList(requestData.parts[i].field, tableDef);
	    		Utils.require(groupsSet.size() == 1, "Olap does not support multiple group sets");
				parts[i].groups = groupsSet.get(0);
			} else parts[i].groups = new ArrayList<AggregationGroup>(0);
		}
		
    	XLinkContext xcontext = new XLinkContext(requestData.application, olap, xshards, tableDef);
    	XLinkGroupContext xgroupContext = new XLinkGroupContext(xcontext);
    	for(int i=0; i<parts.length; i++) {
	    	xcontext.setupXLinkQuery(tableDef, parts[i].query);
	    	for(AggregationGroup group: parts[i].groups) {
	    		xgroupContext.setupXLinkGroup(group);
	    	}
    	}
    	XLinkMetricContext xmetriccontext = new XLinkMetricContext(xcontext);
    	xmetriccontext.setupXLinkMetric(metrics);
	}
	
	public boolean isOnlyCountStar() {
		if(metrics.size() != 1) return false;
		MetricExpression me = metrics.get(0);
		if(!(me instanceof AggregationMetric)) return false;
		AggregationMetric am = (AggregationMetric)me;
		if(!"COUNT".equals(am.function)) return false;
		if(am.items != null && am.items.size() != 0) return false;
		for(Part part : parts) {
			if(part.groups != null && part.groups.size() != 0) return false;
		}
		return true;
	}
	
	/**
	 * @return N if TOP(N, f) is specified; -N if BOTTOM(N, f) is specified, and 0 otherwise 
	 */
	public int getTop(int group) {
		if(parts[0].groups == null || parts[0].groups.size() <= group) return 0; 
		int top = parts[0].groups.get(group).selectionValue;
		if(parts[0].groups.get(0).selection == Selection.Bottom) top = -top;
		return top;
	}
	
}
