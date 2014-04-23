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
import java.util.HashSet;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.common.Utils;
import com.dell.doradus.olap.Olap;
import com.dell.doradus.olap.io.FileDeletedException;
import com.dell.doradus.olap.search.Result;
import com.dell.doradus.olap.search.ResultBuilder;
import com.dell.doradus.olap.store.CubeSearcher;

public class AggregationBuilder {
    private static Logger LOG = LoggerFactory.getLogger("AggregationBuilder");
	
	public static AggregationResult aggregate(Olap olap, AggregationRequest request) {
		AggregationResult[] results = new AggregationResult[request.shards.size()];
		for(int i = 0; i < results.length; i++) {
			results[i] = aggregate(olap, request.application, request.shards.get(i), request);
		}
		AggregationResult result = AggregationResult.merge(results, request.getTop(0));
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
		MetricCollectorSet collectorSet = MetricCollectorFactory.create(searcher, request.metrics);
		MetricCounterSet counterSet = MetricCounterFactory.create(searcher, request.metrics);
		
		Result[] filters = new Result[request.parts.length];
		IFieldCollector[] fieldCollectors = new IFieldCollector[filters.length];
		for(int i = 0; i < filters.length; i++) {
			filters[i] = ResultBuilder.search(request.tableDef, request.parts[i].query, searcher);
			fieldCollectors[i] = FieldCollectorFactory.create(searcher, collectorSet, request.parts[i].getSingleGroup());
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
		for(int doc = 0; doc < filters[0].size(); doc++) {
			boolean collected = false;
			for(int i = 0; i < filters.length; i++) {
				if(!filters[i].get(doc)) continue;
				if(!collected) {
					collected = true;
					valueSet.reset();
					counterSet.add(doc, valueSet);
				}
				fieldCollectors[i].add(doc, doc, valueSet);
			}
			if(collected) collectorSet.add(doc, valueSet);
		}
		
		AggregationResult result = fieldCollectors[0].getResult();

		List<String> excl = request.parts[0].groups.size() == 0 ? null : request.parts[0].groups.get(0).exclude; 
		if(excl != null && excl.size() != 0) {
			HashSet<String> exclude = new HashSet<>(excl);
			List<AggregationResult.AggregationGroup> newg = new ArrayList<AggregationResult.AggregationGroup>();
			for(AggregationResult.AggregationGroup a : result.groups) {
				if(a.name != null && exclude.contains(a.name)) {
					continue;
				} else newg.add(a);
			}
			result.groups = newg;
		}

		List<String> incl = request.parts[0].groups.size() == 0 ? null : request.parts[0].groups.get(0).include; 
		if(incl != null && incl.size() != 0) {
			HashSet<String> include = new HashSet<>(incl);
			List<AggregationResult.AggregationGroup> newg = new ArrayList<AggregationResult.AggregationGroup>();
			for(AggregationResult.AggregationGroup a : result.groups) {
				if(a.name != null && include.contains(a.name)) newg.add(a);
			}
			result.groups = newg;
		}
		

		if(request.parts[0].groups.size() != 0 && request.parts[0].groups.get(0).stopWords != null) {
			AggregationGroupTokenizer.tokenizeValues(result, collectorSet);
		}
		
		if(request.parts[0].groups.size() != 0 && request.parts[0].groups.get(0).tocase != null) {
			String tocase =  request.parts[0].groups.get(0).tocase;
			Utils.require("UPPER".equals(tocase) || "LOWER".equals(tocase), "only UPPER and LOWER casing are supported");
			AggregationGroupChangeCasing.changeCasing(result, "UPPER".equals(tocase));
		}

		return result;
	}

}
