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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.olap.collections.BdLongSet;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.olap.store.IdSearcher;
import com.dell.doradus.olap.store.ValueSearcher;

public class MetricCollectorDistinct implements IMetricCollectorWithContext {
	protected CubeSearcher searcher;
	protected FieldDefinition fieldDef;
	
	@Override public void setContext(CubeSearcher searcher, FieldDefinition fieldDefinition) {
		this.searcher = searcher;
		this.fieldDef = fieldDefinition;
	}

	@Override public IMetricValue convert(IMetricValue value) { return value; }
	@Override public IMetricValue get() { return new MetricValueDistinct(); }
	
	public static class Text extends MetricCollectorDistinct {
		@Override public IMetricValue convert(IMetricValue value) {
			MetricValueDistinct m = (MetricValueDistinct)value;
			BdLongSet longValues = m.getLongValues();
			Set<Object> values = m.getValues();
			if(longValues.size() == 0) return m;
			longValues.sort();
			ValueSearcher vs = searcher.getValueSearcher(fieldDef.getTableName(), fieldDef.getName());
			for(int i = 0; i < longValues.size(); i++) {
				long l = longValues.get(i);
				if(l >= 0) {
					String text = vs.getValue((int)l).toString();
					values.add(text);
				}
			}
			longValues.clear();
			return value;
		}
		
	}		

	public static class Id extends MetricCollectorDistinct {
		@Override public IMetricValue convert(IMetricValue value) {
			MetricValueDistinct m = (MetricValueDistinct)value;
			Set<Object> values = m.getValues();
			if(values.size() == 0) return m; 
			List<Long> longValues = new ArrayList<Long>(values.size());
			for(Object v: values) if(v instanceof Long) longValues.add((Long)v);
			Collections.sort(longValues);
			values.clear();
			IdSearcher ids = searcher.getIdSearcher(fieldDef.getLinkExtent());
			for(Long v: longValues) {
				String text = ids.getId(v.intValue()).toString();
				values.add(text);
			}
			return value;
		}
	}		
	
}
