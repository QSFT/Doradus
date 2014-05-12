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

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.olap.search.Result;
import com.dell.doradus.olap.search.ResultBuilder;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.search.aggregate.AggregationGroup;
import com.dell.doradus.search.aggregate.AggregationGroupItem;

public class FieldCollectorFactory {
	
	public static IFieldCollector create(CubeSearcher searcher, MetricCollectorSet collectorSet, AggregationGroup group) {
		IFieldCollector result = null;
		if(group == null) result = new EndFieldCollector.EmptyCollector();
		else result = create(searcher, group, 0);
		result.reset(collectorSet);
		return result;
	}

	private static IFieldCollector create(CubeSearcher searcher, AggregationGroup group, int index) {
		AggregationGroupItem item = group.items.get(index);
		FieldDefinition fieldDef = item.fieldDef;
		Result filter = null;
		if(item.query != null) {
			filter = ResultBuilder.search(item.tableDef, item.query, searcher);
		}
		if(index < group.items.size() - 1) {
			Utils.require(item.fieldDef.isLinkField(), item.name + " is not allowed because it is not a link");
			IFieldCollector inner = create(searcher, group, index + 1);
			return new LinkCollector(filter, fieldDef, searcher, inner);
		}
	
		switch(item.fieldDef.getType()) {
		case BOOLEAN: {
			return new EndFieldCollector.BooleanCollector(searcher, fieldDef);
		}
		case INTEGER: {
			if(group.batch == null) return new EndFieldCollector.NumCollector(searcher, fieldDef);
			else return new EndFieldCollector.NumBatchCollector(searcher, fieldDef, group.batch);
		}
		case LONG: {
			if(group.batch == null) return new EndFieldCollector.NumCollector(searcher, fieldDef);
			else return new EndFieldCollector.NumBatchCollector(searcher, fieldDef, group.batch);
		}
		case FLOAT: {
			if(group.batch == null) return new EndFieldCollector.NumFloatCollector(searcher, fieldDef);
			else return new EndFieldCollector.NumFloatBatchCollector(searcher, fieldDef, group.batch);
		}
		case DOUBLE: {
			if(group.batch == null) return new EndFieldCollector.NumDoubleCollector(searcher, fieldDef);
			else return new EndFieldCollector.NumDoubleBatchCollector(searcher, fieldDef, group.batch);
		}
		case TEXT: {
			IFieldCollector inner = new EndFieldCollector.TextCollector(searcher, fieldDef);
			return new LinkCollector(filter, fieldDef, searcher, inner);
		}
		case TIMESTAMP: {
			if(group.subField != null) return new DateSubfieldCollector(searcher, fieldDef, group.subField);
			else return new DateFieldCollector(searcher, fieldDef, group.truncate, group.timeZone);
		}
		case LINK: {
			ApplicationDefinition appDef = item.tableDef.getAppDef();
			TableDefinition extentTable = appDef.getTableDef(fieldDef.getLinkExtent());
			IFieldCollector inner = new EndFieldCollector.IdCollector(searcher, extentTable);
			return new LinkCollector(filter, fieldDef, searcher, inner);
		}
		default: throw new IllegalArgumentException("Unknown type: " + item.fieldDef.getType());
		}
	}
	
}
