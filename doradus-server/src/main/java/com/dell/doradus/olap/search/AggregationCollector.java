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

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.search.aggregate.AggregationGroup;
import com.dell.doradus.search.aggregate.AggregationGroupItem;

public abstract class AggregationCollector {
	public abstract void collect(int doc, int value);
	public abstract GroupResult getResult(int top);
	
	public static AggregationCollector build(CubeSearcher searcher, AggregationGroup group) {
		return build(searcher, group, 0);
	}
	
	private static AggregationCollector build(CubeSearcher searcher, AggregationGroup group, int pathIndex) {
		AggregationGroupItem item = group.items.get(pathIndex);
		if(pathIndex < group.items.size() - 1) {
			if(!item.isLink) throw new IllegalArgumentException("items in path should be links");
			AggregationCollector inner = build(searcher, group, pathIndex + 1);
			return new LinkFieldCollector(searcher, item.fieldDef, item.query, inner);
		}
	
		switch(item.fieldDef.getType()) {
		case BOOLEAN: return new BooleanFieldCollector(searcher, item.fieldDef);
		case INTEGER: {
			if(group.batch == null) return new NumFieldCollector(searcher, item.fieldDef);
			else return new NumBatchFieldCollector(searcher, item.fieldDef, group.batch);
		}
		case LONG: {
			if(group.batch == null) return new NumFieldCollector(searcher, item.fieldDef);
			else return new NumBatchFieldCollector(searcher, item.fieldDef, group.batch);
		}
		case TEXT: return new TextFieldCollector(searcher, item.fieldDef);
		case TIMESTAMP: return new DateFieldCollector(searcher, item.fieldDef, group.truncate, group.timeZone);
		case LINK: {
			ApplicationDefinition appDef = item.tableDef.getAppDef();
			TableDefinition extentTable = appDef.getTableDef(item.fieldDef.getLinkExtent());
			IdFieldCollector inner = new IdFieldCollector(searcher, extentTable);
			return new LinkFieldCollector(searcher, item.fieldDef, item.query, inner);
		}
		default: throw new IllegalArgumentException("Unknown type: " + item.fieldDef.getType());
		}
	}
	
	public static class NumCount implements Comparable<NumCount> {
		public int num;
		public int count;
		public NumCount(int num, int count) {
			this.num = num;
			this.count = count;
		}
		
		@Override public int compareTo(NumCount o) {
			int c = count - o.count;
			if(c != 0) return -c;
			else return num - o.num;
		}
	}
	
}
