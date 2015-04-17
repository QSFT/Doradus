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
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.olap.Olap;
import com.dell.doradus.olap.aggregate.AggregationRequest;
import com.dell.doradus.olap.aggregate.AggregationResult;
import com.dell.doradus.olap.aggregate.MetricCollectorFactory;
import com.dell.doradus.olap.aggregate.MetricCollectorSet;
import com.dell.doradus.olap.aggregate.MetricCounterFactory;
import com.dell.doradus.olap.aggregate.MetricCounterSet;
import com.dell.doradus.olap.aggregate.MetricValueSet;
import com.dell.doradus.olap.collections.BdLongSet;
import com.dell.doradus.olap.io.FileDeletedException;
import com.dell.doradus.olap.search.Result;
import com.dell.doradus.olap.search.ResultBuilder;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.search.aggregate.AggregationGroup;

public class MFAggregationBuilder {
    private static Logger LOG = LoggerFactory.getLogger("MFAggregationBuilder");
	
	public static AggregationResult aggregate(Olap olap, ApplicationDefinition appDef, AggregationRequest request) {
	    AggregationCollector collector = Olap.getSearchThreadPool() == null ? 
	            searchSinglethreaded(olap, appDef, request) :
                searchMultithreaded(olap, appDef, request);
		AggregationResult result = AggregationResultBuilder.build(request, collector);
		
		
		if(request.flat) {
			int topGroupsCount = 0; // number of top-level groups to leave
			int lastSubgroups = 0; // number of nested groups to leave in the last top-level groups
			int maxGroups = request.getTop(0); // total number of subgroups to leave
			int curGroups = 0;
			for(int i = 0; i < result.groups.size(); i++) {
				topGroupsCount++;
				AggregationResult.AggregationGroup group = result.groups.get(i);
				int subGroups = group.innerResult.groups.size();
				if(curGroups + subGroups >= maxGroups) {
					lastSubgroups = maxGroups - curGroups;
					break;
				}
				curGroups += subGroups;
			}
			if(result.groups.size() > topGroupsCount) {
				List<AggregationResult.AggregationGroup> grp = new ArrayList<AggregationResult.AggregationGroup>(topGroupsCount);
				for(int i = 0; i < topGroupsCount; i++) grp.add(result.groups.get(i));
				result.groups = grp;
			}
			AggregationResult last = result.groups.get(topGroupsCount - 1).innerResult;
			if(last.groups.size() > lastSubgroups) {
				List<AggregationResult.AggregationGroup> grp = new ArrayList<AggregationResult.AggregationGroup>(lastSubgroups);
				for(int i = 0; i < lastSubgroups; i++) grp.add(last.groups.get(i));
				last.groups = grp;
			}
		}
		
		return result;
	}
	
	
	private static AggregationCollector searchSinglethreaded(Olap olap, ApplicationDefinition appDef, AggregationRequest request) {
        AggregationCollector collector = null;
        for(String shard: request.shards) {
            AggregationCollector agg = aggregate(olap, appDef, shard, request);
            if(collector == null) collector = agg;
            else collector.merge(agg);
        }
        if(collector == null) collector = new AggregationCollector(0);
        return collector;
	}

    private static AggregationCollector searchMultithreaded(Olap olap, ApplicationDefinition appDef, AggregationRequest request) {
        try {
            final List<AggregationCollector> results = new ArrayList<AggregationCollector>();
            List<Future<?>> futures = new ArrayList<>();
            for(String shard: request.shards) {
                final Olap f_olap = olap;
                final ApplicationDefinition f_appDef = appDef;
                final String f_shard = shard;
                final AggregationRequest f_request = request;
                futures.add(Olap.getSearchThreadPool().submit(new Runnable() {
                    @Override public void run() {
                        AggregationCollector agg = aggregate(f_olap, f_appDef, f_shard, f_request);
                        synchronized (results) {
                            results.add(agg);
                        }
                                
                    }}));
            }
            
            for(Future<?> f: futures) f.get();
            futures.clear();
            

            AggregationCollector collector = null;
            for(AggregationCollector agg: results) {
                if(collector == null) collector = agg;
                else collector.merge(agg);
            }
            if(collector == null) collector = new AggregationCollector(0);
            return collector;
        }catch(ExecutionException ee) {
            throw new RuntimeException(ee);
        }catch(InterruptedException ee) {
            throw new RuntimeException(ee);
        }
        
        
    }
	
	
	private static AggregationCollector aggregate(Olap olap, ApplicationDefinition appDef, String shard, AggregationRequest request) {
		// repeat if segment was merged
		for(int i = 0; i < 2; i++) {
			try {
				CubeSearcher searcher = olap.getSearcher(appDef, shard);
				return aggregate(searcher, request);
			}catch(FileDeletedException ex) {
				LOG.warn(ex.getMessage() + " - retrying: " + i);
				continue;
			}
		}
		CubeSearcher searcher = olap.getSearcher(appDef, shard);
		return aggregate(searcher, request);
	}
	
	public static AggregationCollector aggregate(CubeSearcher searcher, AggregationRequest request) {
		for(AggregationRequest.Part p : request.parts) {
			if(p.groups == null) {
				p.groups = new ArrayList<AggregationGroup>();
			}
		}
		int groupsCount = request.parts[0].groups.size();
		int partsCount = request.parts.length;
		
		Result[] filters = new Result[partsCount];
		MFCollectorSet[] fieldCollectors = new MFCollectorSet[partsCount];
		MetricCollectorSet[] collectorSets = new MetricCollectorSet[partsCount];
		MetricCounterSet[] counterSets = new MetricCounterSet[partsCount];
		for(int i = 0; i < filters.length; i++) {
			filters[i] = ResultBuilder.search(request.tableDef, request.parts[i].query, searcher);
			fieldCollectors[i] = new MFCollectorSet(searcher, request.parts[i].groups, filters.length == 1); 
			collectorSets[i] = MetricCollectorFactory.create(searcher, request.parts[i].metrics);
			counterSets[i] = MetricCounterFactory.create(searcher, request.parts[i].metrics);
		}
		
		if(request.isOnlyCountStar()) {
			Result r = filters[0];
			for(int i = 1; i < filters.length; i++) {
				r.or(filters[i]);
			}
			
			AggregationCollector collector = new AggregationCollector(r.countSet());
			return collector;
		}

		BdLongSet[] sets = new BdLongSet[groupsCount];
		for(int i = 0; i < groupsCount; i++) {
			sets[i] = new BdLongSet(1024);
			sets[i].enableClearBuffer();
		}
		MGBuilder builder = new MGBuilder(searcher, collectorSets[0], groupsCount);
		
		MetricValueSet valueSet = collectorSets[0].get();
		//collect empty groups: only for top group
		if(groupsCount > 0 && fieldCollectors[0].collectors.length > 0) {
			fieldCollectors[0].collectors[0].collectEmptyGroups(sets[0]);
			if(sets[0].size() > 0) {
				builder.add(-1, sets, valueSet);
			}
			sets[0].clear();
		}
		
		boolean hasCommonPart = filters.length == 1 && fieldCollectors[0].commonPartCollector != null;
		int count = filters[0].size(); 
		
		if(hasCommonPart) {
			BdLongSet commonSet = new BdLongSet(1024);
			commonSet.enableClearBuffer();
			
			for(int doc = 0; doc < count; doc++) {
				for(int i = 0; i < filters.length; i++) {
					if(!filters[i].get(doc)) continue;
					valueSet.reset();
					counterSets[i].add(doc, valueSet);
					// common part in groups
					fieldCollectors[0].commonPartCollector.collect(doc, commonSet);
					for(int d = 0; d < commonSet.size(); d++) {
						long commonDoc = commonSet.get(d);
						fieldCollectors[i].collect(commonDoc, sets);
						if(i > 0 && request.differentMetricsForPairs) builder.add(doc + i * count, sets, valueSet);
						else builder.add(doc, sets, valueSet);
						for(int j = 0; j < sets.length; j++) sets[j].clear();
					}
					commonSet.clear();
				}
			}
		}
		else {
			for(int doc = 0; doc < count; doc++) {
				for(int i = 0; i < filters.length; i++) {
					if(!filters[i].get(doc)) continue;
					valueSet.reset();
					counterSets[i].add(doc, valueSet);
					fieldCollectors[i].collect(doc, sets);
					if(i > 0 && request.differentMetricsForPairs) builder.add(doc + i * count, sets, valueSet);
					else builder.add(doc, sets, valueSet);
					for(int j = 0; j < sets.length; j++) sets[j].clear();
				}
			}
		}
		AggregationCollector result = builder.createResult(request.parts[0].groups, fieldCollectors[0]);

		return result;
	}

}
