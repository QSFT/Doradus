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

import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.olap.store.IntIterator;
import com.dell.doradus.search.aggregate.AggregationGroup;
import com.dell.doradus.search.aggregate.AggregationMetric;

public class AggregationBuilder {
	
	public static GroupResult aggregate(
			CubeSearcher searcher,
			Result result,
			AggregationGroup group,
			AggregationMetric metric) {
		if(!"COUNT".equals(metric.function)) throw new IllegalArgumentException("Only count supported");
		if(metric.items != null) throw new IllegalArgumentException("Only count(*) supported");
		
		if(group == null || group.items == null || group.items.size() == 0) {
			GroupResult r = new GroupResult();
			r.totalCount = result.countSet();
			r.groupsCount = 0;
			return r;
		}
		
		AggregationCollector collector = AggregationCollector.build(searcher, group);
		IntIterator iter = result.iterate();
		int cnt = 0;
		for(int i = 0; i < iter.count(); i++) {
			int doc = iter.get(i);
			collector.collect(doc, doc);
			cnt++;
		}
		GroupResult r = collector.getResult(group.selectionValue);
		r.totalCount = cnt;
		return r;
	}
	
	
}
