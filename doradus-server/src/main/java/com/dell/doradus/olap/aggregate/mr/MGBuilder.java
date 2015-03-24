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

import java.util.List;

import com.dell.doradus.olap.aggregate.MetricCollectorSet;
import com.dell.doradus.olap.aggregate.MetricValueSet;
import com.dell.doradus.olap.collections.BdLongSet;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.search.aggregate.AggregationGroup;

public class MGBuilder {
	private AggregationCollectorRaw m_rawCollector;
	
	public MGBuilder(CubeSearcher searcher, MetricCollectorSet metricSet, int groupsCount) {
		m_rawCollector = new AggregationCollectorRaw(metricSet);
	}
	
	public void add(int doc, BdLongSet[] sets, MetricValueSet valueSet) {
		m_rawCollector.add(doc, sets, valueSet);
	}
	
	public AggregationCollector createResult(List<AggregationGroup> groups, MFCollectorSet mfc) {
		AggregationCollector collector = new AggregationCollector(mfc, m_rawCollector, groups);
		return collector;
	}
	
}



