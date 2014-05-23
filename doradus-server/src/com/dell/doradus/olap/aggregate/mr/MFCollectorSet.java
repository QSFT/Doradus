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

import com.dell.doradus.olap.collections.BdLongSet;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.search.aggregate.AggregationGroup;

public class MFCollectorSet {
	public MFCollector[] collectors;
	
	public MFCollectorSet(CubeSearcher searcher, List<AggregationGroup> groups) {
		collectors = new MFCollector[groups.size()];
		for(int i = 0; i < collectors.length; i++) {
			collectors[i] = MFCollector.create(searcher, groups.get(i));
		}
	}
	
	public int size() { return collectors.length; }
	
	public void collect(long doc, BdLongSet[] values) {
		for(int i = 0; i < collectors.length; i++) {
			collectors[i].collect(doc, values[i]);
		}
	}
	
}
