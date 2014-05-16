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

import java.util.Collections;
import java.util.List;

import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.olap.XType;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.olap.store.IdSearcher;
import com.dell.doradus.olap.store.NumSearcherMV;
import com.dell.doradus.olap.store.ValueSearcher;

public abstract class EndFieldCollector implements IFieldCollector {
	protected MetricCollectorSet m_collectorSet;
	
	public abstract int getSize();
	public abstract int getIndex(int doc);
	public abstract String getFieldName(int field);
	public abstract Object getFieldId(int field, String name);

	public boolean returnEmptyGroups() { return false; }
	
	@Override public void reset(MetricCollectorSet collectorSet) {
		int size = getSize();
		m_collectorSet = collectorSet;
		m_collectorSet.setSize(size);
	}
	
	@Override public void add(int doc, int field, MetricValueSet valueSet) {
		int index = getIndex(field);
		if(index < 0) return;
		m_collectorSet.add(doc, index, valueSet);
	}

	@Override public AggregationResult getResult() {
		AggregationResult result = new AggregationResult();
		result.documentsCount = m_collectorSet.documentsCount;
		
		if(m_collectorSet.summaryGroup != null) {
			result.summary = new AggregationResult.AggregationGroup();
			result.summary.name = null;
			result.summary.id = null;
			result.summary.metricSet = m_collectorSet.summaryGroup;
			m_collectorSet.convert(result.summary.metricSet);
		}
		if(m_collectorSet.nullGroup != null) {
			AggregationResult.AggregationGroup group = new AggregationResult.AggregationGroup();
			group.name = null;
			group.id = null;
			group.metricSet = m_collectorSet.nullGroup;
			m_collectorSet.convert(group.metricSet);
			result.groups.add(group);
		}
		for(int i = 0; i < m_collectorSet.documents.length; i++) {
			if(!returnEmptyGroups() && m_collectorSet.documents[i] < 0) continue;
			result.groupsCount++;
			AggregationResult.AggregationGroup group = new AggregationResult.AggregationGroup();
			group.name = getFieldName(i);
			group.id = getFieldId(i, group.name);
			group.metricSet = m_collectorSet.get(i);
			m_collectorSet.convert(group.metricSet);
			result.groups.add(group);
		}
		Collections.sort(result.groups);
		return result;
	}
	
	public static class EmptyCollector extends EndFieldCollector {
		
		public EmptyCollector() { }
		
		@Override public int getSize() { return 1; }
		
		@Override public int getIndex(int doc) { return 0; }

		@Override public String getFieldName(int field) { return "*"; }
		
		@Override public String getFieldId(int field, String name) { return "*"; }
	}
	
	public static class BooleanCollector extends EndFieldCollector {
		private NumSearcherMV m_searcher;
		
		public BooleanCollector(CubeSearcher searcher, FieldDefinition fieldDef) {
			m_searcher = searcher.getNumSearcher(fieldDef.getTableName(), fieldDef.getName());
		}
		
		@Override public int getSize() { return 2; }
		
		@Override public int getIndex(int doc) { return m_searcher.isNull(doc) ? -1 : (int)m_searcher.get(doc, 0); }

		@Override public String getFieldName(int field) { return XType.toString(field != 0); }
		
		@Override public String getFieldId(int field, String name) { return field != 0 ? "1" : "0"; }
		
	}

	public static class NumCollector extends EndFieldCollector {
		private NumSearcherMV m_searcher;
		private long m_max;
		private long m_min;
		
		public NumCollector(CubeSearcher searcher, FieldDefinition fieldDef) {
			m_searcher = searcher.getNumSearcher(fieldDef.getTableName(), fieldDef.getName());
			m_max = m_searcher.max();
			m_min = m_searcher.min();
			if(m_max - m_min > 10000000) throw new IllegalArgumentException("Numeric range too big; use BATCH(...) instead");
		}
		
		@Override public int getSize() { return (int)(m_max - m_min) + 1 ; }
		
		@Override public int getIndex(int doc) { return m_searcher.isNull(doc) ? -1 : (int)(m_searcher.get(doc, 0) - m_min); }

		@Override public String getFieldName(int field) { return "" + (field + m_min); }
		
		@Override public Long getFieldId(int field, String name) { return new Long(field + m_min); }
	}

	public static class NumDoubleCollector extends NumCollector {
		public NumDoubleCollector(CubeSearcher searcher, FieldDefinition fieldDef) { super(searcher, fieldDef); }
		
		@Override public String getFieldName(int field) {
			String fn = super.getFieldName(field);
			if(fn == null) return null;
			else return XType.toString(Double.longBitsToDouble(Long.parseLong(fn)));
		}
	}

	public static class NumFloatCollector extends NumCollector {
		public NumFloatCollector(CubeSearcher searcher, FieldDefinition fieldDef) { super(searcher, fieldDef); }
		
		@Override public String getFieldName(int field) {
			String fn = super.getFieldName(field);
			if(fn == null) return null;
			else return XType.toString(Float.intBitsToFloat((int)Long.parseLong(fn)));
		}
	}
	
	public static class NumBatchCollector extends EndFieldCollector {
		private NumSearcherMV m_searcher;
		private long[] m_batches;
		
		public NumBatchCollector(CubeSearcher searcher, FieldDefinition fieldDef, List<? extends Object> batches) {
			m_searcher = searcher.getNumSearcher(fieldDef.getTableName(), fieldDef.getName());
			m_batches = new long[batches.size()];
			for(int i = 0; i < m_batches.length; i++) {
				m_batches[i] = Long.parseLong(batches.get(i).toString());
			}
		}
		
		@Override public int getSize() { return m_batches.length + 1; }
		
		@Override public int getIndex(int doc) { 
			if(m_searcher.isNull(doc)) return -1; 
			long v = m_searcher.get(doc, 0);
			int pos = 0;
			while(pos < m_batches.length && m_batches[pos] <= v) pos++;
			return pos;
		}

		@Override public String getFieldName(int field) {
			String v = null;
			if(field == 0) v = "< " + m_batches[0];
			else if(field == m_batches.length) v = ">= " + m_batches[field - 1];
			else v = m_batches[field - 1] + " - " + m_batches[field]; 
			return v;
		}
		
		@Override public Long getFieldId(int field, String name) {
			return new Long(field);
		}
		
		@Override public boolean returnEmptyGroups() { return true; }
	}

	public static class NumDoubleBatchCollector extends EndFieldCollector {
		private NumSearcherMV m_searcher;
		private double[] m_batches;
		
		public NumDoubleBatchCollector(CubeSearcher searcher, FieldDefinition fieldDef, List<? extends Object> batches) {
			m_searcher = searcher.getNumSearcher(fieldDef.getTableName(), fieldDef.getName());
			m_batches = new double[batches.size()];
			for(int i = 0; i < m_batches.length; i++) {
				m_batches[i] = Double.parseDouble(batches.get(i).toString());
			}
		}
		
		@Override public int getSize() { return m_batches.length + 1; }
		
		@Override public int getIndex(int doc) { 
			if(m_searcher.isNull(doc)) return -1; 
			long lv = m_searcher.get(doc, 0);
			double v = Double.longBitsToDouble(lv);
			int pos = 0;
			while(pos < m_batches.length && m_batches[pos] <= v) pos++;
			return pos;
		}

		@Override public String getFieldName(int field) {
			String v = null;
			if(field == 0) v = "< " + XType.toString(m_batches[0]);
			else if(field == m_batches.length) v = ">= " + XType.toString(m_batches[field - 1]);
			else v = XType.toString(m_batches[field - 1]) + " - " + XType.toString(m_batches[field]); 
			return v;
		}
		
		@Override public Long getFieldId(int field, String name) {
			return new Long(field);
		}
		
		@Override public boolean returnEmptyGroups() { return true; }
	}

	public static class NumFloatBatchCollector extends EndFieldCollector {
		private NumSearcherMV m_searcher;
		private float[] m_batches;
		
		public NumFloatBatchCollector(CubeSearcher searcher, FieldDefinition fieldDef, List<? extends Object> batches) {
			m_searcher = searcher.getNumSearcher(fieldDef.getTableName(), fieldDef.getName());
			m_batches = new float[batches.size()];
			for(int i = 0; i < m_batches.length; i++) {
				m_batches[i] = Float.parseFloat(batches.get(i).toString());
			}
		}
		
		@Override public int getSize() { return m_batches.length + 1; }
		
		@Override public int getIndex(int doc) { 
			if(m_searcher.isNull(doc)) return -1; 
			long lv = m_searcher.get(doc, 0);
			float v = Float.intBitsToFloat((int)lv);
			int pos = 0;
			while(pos < m_batches.length && m_batches[pos] <= v) pos++;
			return pos;
		}

		@Override public String getFieldName(int field) {
			String v = null;
			if(field == 0) v = "< " + XType.toString(m_batches[0]);
			else if(field == m_batches.length) v = ">= " + XType.toString(m_batches[field - 1]);
			else v = XType.toString(m_batches[field - 1]) + " - " + XType.toString(m_batches[field]); 
			return v;
		}
		
		@Override public Long getFieldId(int field, String name) {
			return new Long(field);
		}
		
		@Override public boolean returnEmptyGroups() { return true; }
	}
	
	public static class TextCollector extends EndFieldCollector {
		private ValueSearcher m_valueSearcher;
		
		public TextCollector(CubeSearcher searcher, FieldDefinition fieldDef) {
			m_valueSearcher = searcher.getValueSearcher(fieldDef.getTableName(), fieldDef.getName());
		}
		
		@Override public int getSize() { return m_valueSearcher.size(); }
		
		@Override public int getIndex(int doc) { return doc; }

		@Override public String getFieldName(int field) {
			return m_valueSearcher.getValue(field).toString();
		}
		
		@Override public String getFieldId(int field, String name) {
			return name.toLowerCase();
		}
	}
	
	public static class IdCollector extends EndFieldCollector {
		private IdSearcher m_searcher;
		
		public IdCollector(CubeSearcher searcher, TableDefinition tableDef) {
			m_searcher = searcher.getIdSearcher(tableDef.getTableName());
		}
		
		@Override public int getSize() { return m_searcher.size(); }
		
		@Override public int getIndex(int doc) { return doc; }

		@Override public String getFieldName(int field) {
			return m_searcher.getId(field).toString();
		}
		
		@Override public String getFieldId(int field, String name) {
			return name;
		}
	}
	
}
