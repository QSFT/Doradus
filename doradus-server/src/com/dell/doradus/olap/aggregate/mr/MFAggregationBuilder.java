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

package com.dell.doradus.olap.aggregate.mr;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.olap.Olap;
import com.dell.doradus.olap.aggregate.AggregationRequest;
import com.dell.doradus.olap.aggregate.AggregationResult;
import com.dell.doradus.olap.aggregate.MetricCollectorFactory;
import com.dell.doradus.olap.aggregate.MetricCollectorSet;
import com.dell.doradus.olap.aggregate.MetricCounterFactory;
import com.dell.doradus.olap.aggregate.MetricCounterSet;
import com.dell.doradus.olap.aggregate.MetricValueSet;
import com.dell.doradus.olap.io.FileDeletedException;
import com.dell.doradus.olap.search.Result;
import com.dell.doradus.olap.search.ResultBuilder;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.olap.xlink.XLinkGroupContext;
import com.dell.doradus.search.aggregate.AggregationGroup;

public class MFAggregationBuilder {
    private static Logger LOG = LoggerFactory.getLogger("MFggregationBuilder");
	
    public static boolean ALWAYS_USE_MLG = false;
    
    public static boolean shouldUseMF(List<AggregationGroup> groups) {
    	if(ALWAYS_USE_MLG) return true;
    	if(groups.size() > 1) return true;
    	if(groups.size() == 0) return false;
    	if(XLinkGroupContext.hasXLink(groups.get(0))) return true;
    	//AggregationGroup ag = groups.get(0);
    	//if(ag.items.size() == 0) return false;
    	//FieldDefinition fieldDef = ag.items.get(ag.items.size() - 1).fieldDef;
    	//if((fieldDef.getType() == FieldType.INTEGER || fieldDef.getType() == FieldType.LONG) && ag.batch == null) return true;
    	//if(fieldDef.getType() == FieldType.TIMESTAMP && ag.truncate == null) return true;
    	return false;
    }
    
	public static AggregationResult aggregate(Olap olap, AggregationRequest request) {
		AggregationResult[] results = new AggregationResult[request.shards.size()];
		for(int i = 0; i < results.length; i++) {
			results[i] = aggregate(olap, request.application, request.shards.get(i), request);
		}
		AggregationResult result = AggregationResult.merge(results, request.getTop(0));
		AggregationResult.applyLimits(result, request, 0);
		return result;
	}
	
	
	private static AggregationResult aggregate(Olap olap, String application, String shard, AggregationRequest request) {
		// repeat if segment was merged
		for(int i = 0; i < 2; i++) {
			try {
				CubeSearcher searcher = olap.getSearcher(application, shard);
				return aggregate(searcher, request);
			}catch(FileDeletedException ex) {
				LOG.warn(ex.getMessage() + " - retrying: " + i);
				continue;
			}
		}
		CubeSearcher searcher = olap.getSearcher(application, shard);
		return aggregate(searcher, request);
	}
	
	public static AggregationResult aggregate(CubeSearcher searcher, AggregationRequest request) {
		for(AggregationRequest.Part p : request.parts) if(p.groups == null) p.groups=new ArrayList<AggregationGroup>();
		
		int groupsCount = request.parts[0].groups == null ? 0 : request.parts[0].groups.size();
		if(groupsCount == 0) groupsCount = 1;
		MetricCollectorSet collectorSet = MetricCollectorFactory.create(searcher, request.metrics);
		MetricCounterSet counterSet = MetricCounterFactory.create(searcher, request.metrics);
		MGBuilder builder = new MGBuilder(searcher, collectorSet, groupsCount);
		List<Set<Long>> sets = new ArrayList<Set<Long>>(groupsCount);
		for(int i = 0; i < groupsCount; i++)sets.add(new HashSet<Long>());
		
		Result[] filters = new Result[request.parts.length];
		MFCollectorSet[] fieldCollectors = new MFCollectorSet[filters.length];
		for(int i = 0; i < filters.length; i++) {
			filters[i] = ResultBuilder.search(request.tableDef, request.parts[i].query, searcher);
			fieldCollectors[i] = new MFCollectorSet(searcher, request.parts[i].groups); 
		}
		
		
		if(request.isOnlyCountStar()) {
			Result r = filters[0];
			for(int i = 1; i < filters.length; i++) {
				r.or(filters[i]);
			}
			AggregationResult res = new AggregationResult();
			res.documentsCount = r.countSet();
			res.groupsCount = 0;
			return res;
		}
		
		MetricValueSet valueSet = collectorSet.get(-1);
		//collect emtpy groups: only for top group
		if(fieldCollectors[0].collectors.length > 0) {
			fieldCollectors[0].collectors[0].collectEmptyGroups(sets.get(0));
			if(!sets.get(0).isEmpty()) {
				builder.add(-1, sets, valueSet);
			}
			sets.get(0).clear();
		}
		
		for(int doc = 0; doc < filters[0].size(); doc++) {
			boolean collected = false;
			for(int i = 0; i < filters.length; i++) {
				if(!filters[i].get(doc)) continue;
				if(!collected) {
					collected = true;
					valueSet.reset();
					counterSet.add(doc, valueSet);
				}
				fieldCollectors[i].collect(doc, sets);
			}
			if(collected) {
				builder.add(doc, sets, valueSet);
				for(int i = 0; i < sets.size(); i++) sets.get(i).clear();
			}
		}
		
		AggregationResult result = builder.createResult(request.parts[0].groups, collectorSet, fieldCollectors[0]);
		return result;
	}

}
