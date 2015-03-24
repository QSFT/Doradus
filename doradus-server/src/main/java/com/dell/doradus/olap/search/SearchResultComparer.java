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

import com.dell.doradus.olap.aggregate.mr.MFCollectorSet;
import com.dell.doradus.olap.collections.BdLongSet;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.olap.store.IntIterator;
import com.dell.doradus.search.aggregate.AggregationGroup;
import com.dell.doradus.search.aggregate.SortOrder;
import com.dell.doradus.search.util.HeapList;

public class SearchResultComparer {
	
	public static IntIterator sort(CubeSearcher searcher, Result result, SortOrder[] orders, int size) {
		if(orders == null || orders.length == 0 || size >= result.countSet()) {
			int[] res = new int[Math.min(size, result.countSet())];
			int num = 0;
			for(int i = 0; i < result.size(); i++) {
				if(num >= res.length) break;
				if(!result.get(i)) continue;
				res[num++] = i;
			}
			return new IntIterator(res, 0, res.length);
		}

		BdLongSet[] sets = new BdLongSet[orders.length];
		for(int i = 0; i < orders.length; i++) {
			sets[i] = new BdLongSet(1024);
			sets[i].enableClearBuffer();
		}
		
		List<AggregationGroup> aggGroups = new ArrayList<AggregationGroup>(orders.length);
		for(SortOrder order: orders) { aggGroups.add(order.getAggregationGroup()); }
		MFCollectorSet collectorSet = new MFCollectorSet(searcher, aggGroups, false);
		
		HeapList<SortKey> heap = new HeapList<SortKey>(size);
		SortKey cur = null;
		
		for(int doc = 0; doc < result.size(); doc++) {
			if(!result.get(doc)) continue;
			collectorSet.collect(doc, sets);
			if(cur == null) cur = new SortKey(orders);
			cur.set(doc, sets);
			cur = heap.AddEx(cur);
			for(int i = 0; i < sets.length; i++) sets[i].clear();
		}
		
		SortKey[] keys = heap.GetValues(SortKey.class);
		int[] res = new int[keys.length];
		for(int i = 0; i < keys.length; i++) {
			res[i] = keys[i].doc();
		}
		return new IntIterator(res, 0, res.length);
	}
	
}
