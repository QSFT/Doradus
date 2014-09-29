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
import com.dell.doradus.olap.store.IdSearcher;

public class InverseXLinkMetricCounter extends MetricCounter
{
	private IMetricValue[] values;
	
	public InverseXLinkMetricCounter(CubeSearcher searcher, FieldDefinition fieldDef, XMetrics metrics) {
		IdSearcher ids = searcher.getIdSearcher(fieldDef.getTableName());
		values = new IMetricValue[ids.size()];
		for(int i = 0; i < ids.size(); i++) {
			BSTR id = ids.getId(i);
			values[i] = metrics.metricsMap.get(id);
		}
	}
	
	@Override public void add(int doc, IMetricValue value) {
		if(values[doc] != null) value.add(values[doc]);
	}
}
