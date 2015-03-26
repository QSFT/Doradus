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

import java.util.HashSet;
import java.util.Set;

import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.olap.collections.BdLongSet;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.olap.store.IdSearcher;
import com.dell.doradus.olap.store.ValueSearcher;

public class MetricValueDistinct implements IMetricValue {
	protected BdLongSet m_LongValues = new BdLongSet(2);
	protected Set<BSTR> m_Values = new HashSet<BSTR>();

	public MetricValueDistinct() {
		m_LongValues.enableClearBuffer();
	}
	
	public BdLongSet getLongValues() { return m_LongValues; }
	public Set<BSTR> getValues() { return m_Values; }
	public long getValue() { return m_Values.size() == 0 ? m_LongValues.size() : m_Values.size(); }
	
	@Override public int compareTo(IMetricValue o) {
		MetricValueDistinct other = (MetricValueDistinct)o;
		return Double.compare(getValue(), other.getValue());
	}

	@Override public String toString() { return "" + getValue(); }
	
	@Override public void reset() { m_Values.clear(); m_LongValues.clear(); }

	@Override public void add(IMetricValue value) {
		MetricValueDistinct other = (MetricValueDistinct)value;
		m_Values.addAll(other.m_Values);
		m_LongValues.addAll(other.m_LongValues);
	}

	@Override public void add(long value) {
		m_LongValues.add(value);
	}

	@Override public boolean isDegenerate() { return false; } 

	@Override public IMetricValue newInstance() { return new MetricValueDistinct(); }
	@Override public IMetricValue convert(MetricCollector collector) { return this; }
	
	public static class Text extends MetricValueDistinct {
		@Override public IMetricValue newInstance() { return new Text(); }
		@Override public IMetricValue convert(MetricCollector collector) {
			CubeSearcher searcher = collector.getSearcher();
			FieldDefinition fieldDef = collector.getFieldDefinition();
			BdLongSet longValues = getLongValues();
			Set<BSTR> values = getValues();
			if(longValues.size() == 0) return this;
			longValues.sort();
			ValueSearcher vs = searcher.getValueSearcher(fieldDef.getTableName(), fieldDef.getName());
			for(int i = 0; i < longValues.size(); i++) {
				long l = longValues.get(i);
				if(l >= 0) {
					BSTR text = new BSTR(vs.getValue((int)l));
					values.add(text);
				}
			}
			longValues.clear();
			return this;
		}
	}
	
	public static class Id extends MetricValueDistinct {
		@Override public IMetricValue newInstance() { return new Id(); }
		@Override public IMetricValue convert(MetricCollector collector) {
			CubeSearcher searcher = collector.getSearcher();
			FieldDefinition fieldDef = collector.getFieldDefinition();
			BdLongSet longValues = getLongValues();
			Set<BSTR> values = getValues();
			if(longValues.size() == 0) return this;
			longValues.sort();
			IdSearcher ids = searcher.getIdSearcher(fieldDef.getLinkExtent());
			for(int i = 0; i < longValues.size(); i++) {
				long l = longValues.get(i);
				if(l >= 0) {
					BSTR text = new BSTR(ids.getId((int)l));
					values.add(text);
				}
			}
			longValues.clear();
			return this;
		}
	}
	
}
