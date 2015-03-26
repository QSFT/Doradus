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

public abstract class MetricValueText implements IMetricValue {
	public String value;
	
	@Override public void reset() { value = null; }
	@Override public String toString() { return value; }
	@Override public void add(long value) { throw new IllegalArgumentException("Invalid operation on text metrics"); }
	
	@Override public int compareTo(IMetricValue o) {
		MetricValueText t = (MetricValueText)o;
		if(value == null && t.value == null) return 0;
		else if(value == null) return -1;
		else if(t.value == null) return 1;
		else return value.compareToIgnoreCase(t.value);
	}

	@Override public boolean isDegenerate() { return value == null; } 
	
	public static class Min extends MetricValueText {
		@Override public void add(IMetricValue o) {
			Min t = (Min)o;
			if(value == null || this.compareTo(t) > 0) value = t.value;
		}
		@Override public IMetricValue newInstance() { return new Min(); }
		@Override public IMetricValue convert(MetricCollector collector) { return this; }
	}
	public static class Max extends MetricValueText {
		@Override public void add(IMetricValue o) {
			Max t = (Max)o;
			if(value == null || this.compareTo(t) < 0) value = t.value;
		}
		@Override public IMetricValue newInstance() { return new Max(); }
		@Override public IMetricValue convert(MetricCollector collector) { return this; }
	}
	
	public static class MinText extends MetricValueMin.MinNum {
		@Override public IMetricValue newInstance() { return new MinText(); }
		@Override public IMetricValue convert(MetricCollector collector) {
			CubeSearcher searcher = collector.getSearcher();
			FieldDefinition fieldDef = collector.getFieldDefinition();
			MetricValueText.Min txt = new MetricValueText.Min();
			if(metric != Long.MAX_VALUE) {
				ValueSearcher vs = searcher.getValueSearcher(fieldDef.getTableName(), fieldDef.getName()); 
				txt.value = vs.getValue((int)metric).toString();
			}
			return txt;
		}
	}
	
	public static class MaxText extends MetricValueMax.MaxNum {
		@Override public IMetricValue newInstance() { return new MaxText(); }
		@Override public IMetricValue convert(MetricCollector collector) {
			CubeSearcher searcher = collector.getSearcher();
			FieldDefinition fieldDef = collector.getFieldDefinition();
			MetricValueText.Max txt = new MetricValueText.Max();
			if(metric != Long.MIN_VALUE) {
				ValueSearcher vs = searcher.getValueSearcher(fieldDef.getTableName(), fieldDef.getName()); 
				txt.value = vs.getValue((int)metric).toString();
			}
			return txt;
		}
	}

	public static class MinLink extends MetricValueMin.MinNum {
		@Override public IMetricValue newInstance() { return new MinLink(); }
		@Override public IMetricValue convert(MetricCollector collector) {
			CubeSearcher searcher = collector.getSearcher();
			FieldDefinition fieldDef = collector.getFieldDefinition();
			MetricValueText.Min txt = new MetricValueText.Min();
			if(metric != Long.MAX_VALUE) {
				IdSearcher ids = searcher.getIdSearcher(fieldDef.getLinkExtent());
				txt.value = ids.getId((int)metric).toString();
			}
			return txt;
		}
	}
	
	public static class MaxLink extends MetricValueMax.MaxNum {
		@Override public IMetricValue newInstance() { return new MaxLink(); }
		@Override public IMetricValue convert(MetricCollector collector) {
			CubeSearcher searcher = collector.getSearcher();
			FieldDefinition fieldDef = collector.getFieldDefinition();
			MetricValueText.Max txt = new MetricValueText.Max();
			if(metric != Long.MIN_VALUE) {
				IdSearcher ids = searcher.getIdSearcher(fieldDef.getLinkExtent());
				txt.value = ids.getId((int)metric).toString();
			}
			return txt;
		}
	}
	
}
