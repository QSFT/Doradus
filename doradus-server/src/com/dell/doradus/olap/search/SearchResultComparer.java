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
import com.dell.doradus.olap.store.FieldSearcher;
import com.dell.doradus.olap.store.IntIterator;
import com.dell.doradus.olap.store.NumSearcher;
import com.dell.doradus.search.aggregate.AggregationGroupItem;
import com.dell.doradus.search.aggregate.SortOrder;
import com.dell.doradus.search.util.HeapList;

public class SearchResultComparer {
	public static IntIterator sort(CubeSearcher searcher, Result result, SortOrder order, int size) {
		if(size >= result.countSet()) return result.iterate();
		if(order == null) {
			int[] res = new int[size];
			int num = 0;
			for(int i = 0; i < result.size(); i++) {
				if(num >= size) break;
				if(!result.get(i)) continue;
				res[num++] = i;
			}
			return new IntIterator(res, 0, res.length);
		}
		if(order.items.size() != 1) throw new IllegalArgumentException("Paths are not supported in the sort order");
		AggregationGroupItem item = order.items.get(0);
		
		HeapList<DocAndField> heap = new HeapList<DocAndField>(size);
		DocAndField cur = new DocAndField();
		
		if(NumSearcher.isNumericType(item.fieldDef.getType())) {
			NumSearcher s = searcher.getNumSearcher(item.fieldDef.getTableName(), item.fieldDef.getName());
			for(int i = 0; i < result.size(); i++) {
				if(!result.get(i)) continue;
				if(cur == null) cur = new DocAndField();
				cur.doc = i;
				cur.field = s.isNull(i) ? Long.MIN_VALUE : s.get(i);
				if(!order.ascending) cur.field = -cur.field;
				cur = heap.AddEx(cur);
			}
		}
		else {
			FieldSearcher s = searcher.getFieldSearcher(item.fieldDef.getTableName(), item.fieldDef.getName());
			IntIterator ii = new IntIterator();
			for(int i = 0; i < result.size(); i++) {
				if(!result.get(i)) continue;
				if(cur == null) cur = new DocAndField();
				cur.doc = i;
				s.fields(i, ii);
				if(ii.count() != 0) cur.field = ii.get(0);
				if(!order.ascending) cur.field = -cur.field;
				cur = heap.AddEx(cur);
			}
		}
		DocAndField[] arr = heap.GetValues(DocAndField.class);
		int[] res = new int[arr.length];
		for(int i = 0; i < arr.length; i++) {
			res[i] = arr[i].doc;
		}
		return new IntIterator(res, 0, res.length);
	}
	
	public static class DocAndField implements Comparable<DocAndField> {
		public int doc;
		public long field;
		@Override public int compareTo(DocAndField o) {
			if(field != o.field) return field > o.field ? 1 : -1;
			else return doc - o.doc;
		}
	}
}
