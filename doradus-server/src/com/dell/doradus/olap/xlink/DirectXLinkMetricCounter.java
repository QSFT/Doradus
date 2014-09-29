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

package com.dell.doradus.olap.xlink;

import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.olap.aggregate.IMetricValue;
import com.dell.doradus.olap.aggregate.MetricCounter;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.olap.store.FieldSearcher;
import com.dell.doradus.olap.store.IntIterator;
import com.dell.doradus.olap.store.ValueSearcher;

public class DirectXLinkMetricCounter extends MetricCounter
{
	private FieldSearcher fs;
	private IntIterator iter;
	private IMetricValue[] values;
	
	public DirectXLinkMetricCounter(CubeSearcher searcher, FieldDefinition fieldDef, XMetrics xmetrics) {
		ValueSearcher vs = searcher.getValueSearcher(fieldDef.getTableName(), fieldDef.getXLinkJunction());
		values = new IMetricValue[vs.size()];
		for(int i=0; i<vs.size(); i++) {
			BSTR val = vs.getValue(i);
			IMetricValue value = xmetrics.metricsMap.get(val);
			values[i] = value;
		}
		fs = searcher.getFieldSearcher(fieldDef.getTableName(), fieldDef.getXLinkJunction());
		iter = new IntIterator();
	}

	@Override public void add(int doc, IMetricValue value) {
		fs.fields((int)doc, iter);
		for(int i = 0; i < iter.count(); i++) {
			int val = iter.get(i);
			if(values[val] != null) value.add(values[val]);
		}
	}

}
