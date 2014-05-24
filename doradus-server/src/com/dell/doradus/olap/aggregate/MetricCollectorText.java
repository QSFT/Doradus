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

import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.olap.store.IdSearcher;
import com.dell.doradus.olap.store.ValueSearcher;

public abstract class MetricCollectorText implements IMetricCollector {
	public CubeSearcher searcher;
	public FieldDefinition fieldDef;

	public static class Min extends MetricCollectorText {
		@Override public IMetricValue get() {
			MetricValueMin.MinNum m = new MetricValueMin.MinNum();
			return m;
		}
		
		@Override public IMetricValue convert(IMetricValue value) {
			MetricValueMin m = (MetricValueMin)value;
			MetricValueText.Min txt = new MetricValueText.Min();
			if(m.metric != Long.MAX_VALUE) {
				ValueSearcher vs = searcher.getValueSearcher(fieldDef.getTableName(), fieldDef.getName()); 
				txt.value = vs.getValue((int)m.metric).toString();
			}
			return txt;
		}
		
	}
	
	public static class MinLink extends Min {
		@Override public IMetricValue convert(IMetricValue value) {
			MetricValueMin m = (MetricValueMin)value;
			MetricValueText.Min txt = new MetricValueText.Min();
			if(m.metric != Long.MAX_VALUE) {
				IdSearcher ids = searcher.getIdSearcher(fieldDef.getLinkExtent());
				txt.value = ids.getId((int)m.metric).toString();
			}
			return txt;
		}
	}

	public static class Max extends MetricCollectorText {
		@Override public IMetricValue get() {
			MetricValueMax.MaxNum m = new MetricValueMax.MaxNum();
			return m;
		}
		
		@Override public IMetricValue convert(IMetricValue value) {
			MetricValueMax m = (MetricValueMax)value;
			MetricValueText.Max txt = new MetricValueText.Max();
			if(m.metric != Long.MIN_VALUE) {
				ValueSearcher vs = searcher.getValueSearcher(fieldDef.getTableName(), fieldDef.getName()); 
				txt.value = vs.getValue((int)m.metric).toString();
			}
			return txt;
		}
	}
	
	public static class MaxLink extends Max {
		@Override public IMetricValue convert(IMetricValue value) {
			MetricValueMax m = (MetricValueMax)value;
			MetricValueText.Max txt = new MetricValueText.Max();
			if(m.metric != Long.MIN_VALUE) {
				IdSearcher ids = searcher.getIdSearcher(fieldDef.getLinkExtent());
				txt.value = ids.getId((int)m.metric).toString();
			}
			return txt;
		}
	}
	
	
}
