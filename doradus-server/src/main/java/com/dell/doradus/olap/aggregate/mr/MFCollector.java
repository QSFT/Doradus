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

package com.dell.doradus.olap.aggregate.mr;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;

import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.FieldType;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.olap.XType;
import com.dell.doradus.olap.collections.BdLongSet;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.olap.search.Result;
import com.dell.doradus.olap.search.ResultBuilder;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.olap.store.FieldSearcher;
import com.dell.doradus.olap.store.IdSearcher;
import com.dell.doradus.olap.store.IntIterator;
import com.dell.doradus.olap.store.NumSearcher;
import com.dell.doradus.olap.store.NumSearcherMV;
import com.dell.doradus.olap.store.ValueSearcher;
import com.dell.doradus.olap.xlink.DirectXLinkCollector;
import com.dell.doradus.olap.xlink.InverseXLinkCollector;
import com.dell.doradus.olap.xlink.XGroups;
import com.dell.doradus.search.aggregate.AggregationGroup;
import com.dell.doradus.search.aggregate.AggregationGroup.SubField;
import com.dell.doradus.search.aggregate.AggregationGroupItem;
import com.dell.doradus.search.query.Query;

public abstract class MFCollector {
	public CubeSearcher searcher;
	
	public MFCollector(CubeSearcher searcher) { this.searcher = searcher; }
	
	public void collectEmptyGroups(BdLongSet values) { }
	public abstract void collect(long doc, BdLongSet values);
	public abstract MGName getField(long value);
	public abstract boolean requiresOrdering(); 

	public static MFCollector create(CubeSearcher searcher, AggregationGroup group) {
		return create(searcher, group, 0, group.items.size());
	}
	public static MFCollector create(CubeSearcher searcher, AggregationGroup group, int start, int end) {
		if(group.batchexAliases != null) {
			return new BatchexCollector(searcher, group.tableDef, group.batchexAliases, group.batchexFilters);
		}
		AggregationGroupItem last = group.items.get(end - 1);
		FieldDefinition fieldDef = last.fieldDef;
		MFCollector collector = null;
		if(NumSearcher.isNumericType(fieldDef.getType())) collector = new EndNumField(searcher, fieldDef, group);
		else if(fieldDef.isLinkField()) {
			Result filter = null;
			if(last.query != null) filter = ResultBuilder.search(last.tableDef, last.query, searcher);
			collector = new IdField(searcher, fieldDef.getInverseTableDef());
			if(last.isTransitive) collector = new TransitiveLinkField(searcher, fieldDef, last.transitiveDepth, filter, collector);
			else collector = new LinkField(searcher, fieldDef, filter, collector);
		} else if(last.fieldDef.isXLinkDirect()) {
				collector = new DirectXLinkCollector(searcher, last.fieldDef, (XGroups)last.xlinkContext);
		} else if(fieldDef.isXLinkInverse()) {
			collector = new InverseXLinkCollector(searcher, fieldDef, (XGroups)last.xlinkContext);
		} else if(fieldDef.getType() == FieldType.TEXT) {
			collector = new EndTextField(searcher, fieldDef);
		} else if(last.isID) {
			collector = new IdField(searcher, last.tableDef);
		} else throw new IllegalArgumentException("Invalid field in aggregation group: " + last.name);
		
		for(int i = end - 2; i >= start; i--) {
			AggregationGroupItem item = group.items.get(i);
			Result filter = null;
			if(item.query != null) filter = ResultBuilder.search(item.tableDef, item.query, searcher);
			if(item.fieldDef.isXLinkDirect()) {
				collector = new DirectXLinkCollector(searcher, item.fieldDef, (XGroups)item.xlinkContext);
			} else if(item.fieldDef.isXLinkInverse()) {
				collector = new InverseXLinkCollector(searcher, item.fieldDef, (XGroups)item.xlinkContext);
			} else if(item.fieldDef.isLinkField()) {
				if(item.isTransitive) collector = new TransitiveLinkField(searcher, item.fieldDef, item.transitiveDepth, filter, collector);
				else collector = new LinkField(searcher, item.fieldDef, filter, collector);
			} else throw new IllegalArgumentException("Invalid field in aggregation group: " + item.name);
		}
		
		if(group.filter != null) {
			Result filter = ResultBuilder.search(group.tableDef, group.filter, searcher);
			collector = new MFCollector.FilteredCollector(filter, collector);
		}
		
		return collector;
	}

	public static class NullField extends MFCollector
	{
		public NullField(CubeSearcher searcher) { super(searcher); }
		@Override public void collect(long doc, BdLongSet values) { values.add(0); }
		@Override public MGName getField(long value) { return new MGName("*"); }
		@Override public boolean requiresOrdering() { return false; }
	}
	
	public static class BooleanField extends MFCollector
	{
		public BooleanField(CubeSearcher searcher) { super(searcher); }
		@Override public void collect(long doc, BdLongSet values) { values.add(doc); }
		@Override public MGName getField(long value) {
			return new MGName(value == 0 ? "false" : "true", new BSTR(value == 0 ? "0" : "1"));
		}
		@Override public boolean requiresOrdering() { return false; }
	}
	
	public static class LongField extends MFCollector
	{
		public LongField(CubeSearcher searcher) { super(searcher); }
		@Override public void collect(long doc, BdLongSet values) { values.add(doc); }
		@Override public MGName getField(long value) { return new MGName(value); }
		@Override public boolean requiresOrdering() { return false; }
	}

	public static class FloatField extends MFCollector
	{
		public FloatField(CubeSearcher searcher) { super(searcher); }
		// floatToIntBits flips sort order of negative numbers, so we fix it
		@Override public void collect(long doc, BdLongSet values) {
			if(doc < 0) doc = - (doc & Integer.MAX_VALUE);
			values.add(doc);
		}
		@Override public MGName getField(long value) {
			if(value < 0) value = (-value) | Integer.MIN_VALUE;
			float fval = Float.intBitsToFloat((int)value);
			return new MGName(XType.toString(fval), new BSTR(fval));
		}
		@Override public boolean requiresOrdering() { return false; }
	}

	public static class DoubleField extends MFCollector
	{
		public DoubleField(CubeSearcher searcher) { super(searcher); }
		// doubleToLongBits flips sort order of negative numbers, so we fix it
		@Override public void collect(long doc, BdLongSet values) {
			if(doc < 0) doc = - (doc & Long.MAX_VALUE);
			values.add(doc);
		}
		@Override public MGName getField(long value) {
			if(value < 0) value = (-value) | Long.MIN_VALUE;
			double dval = Double.longBitsToDouble(value);
			return new MGName(XType.toString(dval), new BSTR(dval));
		}
		@Override public boolean requiresOrdering() { return false; }
	}
	
	public class DateField extends MFCollector {
		private String m_truncate;
		private TimeZone m_zone;
		private long m_divisor;
		private GregorianCalendar m_Calendar;

		public DateField(CubeSearcher searcher, String truncate, String timeZone) {
			super(searcher);
			if(timeZone != null) m_zone = TimeZone.getTimeZone(timeZone);
			else m_zone = Utils.UTC_TIMEZONE;
			m_truncate = truncate;
			if(m_truncate == null) m_truncate = "SECOND"; 
			m_truncate = m_truncate.toUpperCase();
			if ("SECOND".equals(m_truncate)) m_divisor = 1000;
			else if ("MINUTE".equals(m_truncate)) m_divisor = 60 * 1000;
			else if ("HOUR".equals(m_truncate)) m_divisor = 3600 * 1000;
			else m_divisor = 1 * 3600 * 1000;
			m_Calendar = (GregorianCalendar)GregorianCalendar.getInstance(m_zone);
		}

		@Override public void collect(long doc, BdLongSet values) { values.add(doc / m_divisor); }
		@Override public boolean requiresOrdering() { return false; }

		@Override public MGName getField(long value) {
			m_Calendar.setTimeInMillis(value * m_divisor);
			if ("SECOND".equals(m_truncate) || "MINUTE".equals(m_truncate) || "HOUR".equals(m_truncate)) {
				return new MGName(Utils.formatDate(m_Calendar, Calendar.SECOND));
			}
			else if ("DAY".equals(m_truncate)) {
				return new MGName(Utils.formatDate(m_Calendar, Calendar.DATE));
			}
			else if("WEEK".equals(m_truncate)) {
				GregorianCalendar calendar = Utils.truncateToWeek(m_Calendar);
				return new MGName(Utils.formatDate(calendar, Calendar.DATE));
			}
			else if ("MONTH".equals(m_truncate)) {
				m_Calendar.set(Calendar.DAY_OF_MONTH, 1);
				return new MGName(Utils.formatDate(m_Calendar, Calendar.DATE));
			}
			else if ("QUARTER".equals(m_truncate)) {
				m_Calendar.set(Calendar.DAY_OF_MONTH, 1);
				m_Calendar.set(Calendar.MONTH, m_Calendar.get(Calendar.MONTH) / 3 * 3);
				return new MGName(Utils.formatDate(m_Calendar, Calendar.DATE));
			}
			else if ("YEAR".equals(m_truncate)) {
				return new MGName(Utils.formatDate(m_Calendar, Calendar.YEAR));
			}
			else throw new IllegalArgumentException("Unknown truncate function: " + m_truncate);
		}
	}
	
	public class DateSubField extends MFCollector {
		private SubField m_subfield;
		private Calendar m_calendar;
		private int m_calendarField;
		private String[] m_months = new String[] { "January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"};
		
		public DateSubField(CubeSearcher searcher, SubField subfield) {
			super(searcher);
			m_subfield = subfield;
			m_calendar = new GregorianCalendar(Utils.UTC_TIMEZONE);
			
			switch(subfield) {
				case MINUTE: m_calendarField = Calendar.MINUTE; break;
				case HOUR: m_calendarField = Calendar.HOUR_OF_DAY; break;
				case DAY: m_calendarField = Calendar.DAY_OF_MONTH; break;
				case MONTH: m_calendarField = Calendar.MONTH; break;
				case YEAR: m_calendarField = Calendar.YEAR; break;
			default: Utils.require(false, "Undefined subfield: " + subfield);
			}
		}

		@Override public void collect(long doc, BdLongSet values) {
			m_calendar.setTimeInMillis(doc);
			int pos = m_calendar.get(m_calendarField);
			values.add((long)pos);
		}

		@Override public MGName getField(long value) {
			if(m_subfield == SubField.MONTH) return new MGName(m_months[(int)value], new BSTR(value));
			else return new MGName(value);
		}
		
		@Override public boolean requiresOrdering() { return false; }
	}
	
	public static class NumBatchField extends MFCollector {
		private long[] m_batches;
		
		public NumBatchField(CubeSearcher searcher, List<? extends Object> batches) {
			super(searcher);
			m_batches = new long[batches.size()];
			for(int i = 0; i < m_batches.length; i++) {
				m_batches[i] = Long.parseLong(batches.get(i).toString());
			}
		}
		
		@Override public void collect(long doc, BdLongSet values) { 
			int pos = 0;
			while(pos < m_batches.length && m_batches[pos] <= doc) pos++;
			values.add((long)pos);
		}

		@Override public MGName getField(long value) {
			int field = (int)value;
			String v = null;
			if(field == 0) v = "< " + m_batches[0];
			else if(field == m_batches.length) v = ">= " + m_batches[field - 1];
			else v = m_batches[field - 1] + " - " + m_batches[field];
			return new MGName(v, new BSTR(value));
		}
		
		@Override public void collectEmptyGroups(BdLongSet values) {
			for(long l = 0; l <= m_batches.length; l++) {
				values.add(l);
			}
		}
		
		@Override public boolean requiresOrdering() { return false; }
	}
	
	public static class NumDoubleBatchField extends MFCollector {
		private double[] m_batches;
		
		public NumDoubleBatchField(CubeSearcher searcher, List<? extends Object> batches) {
			super(searcher);
			m_batches = new double[batches.size()];
			for(int i = 0; i < m_batches.length; i++) {
				m_batches[i] = Double.parseDouble(batches.get(i).toString());
			}
		}
		
		@Override public void collect(long doc, BdLongSet values) { 
			double d = Double.longBitsToDouble(doc);
			int pos = 0;
			while(pos < m_batches.length && m_batches[pos] <= d) pos++;
			values.add((long)pos);
		}

		@Override public MGName getField(long value) {
			int field = (int)value;
			String v = null;
			if(field == 0) v = "< " + XType.toString(m_batches[0]);
			else if(field == m_batches.length) v = ">= " + XType.toString(m_batches[field - 1]);
			else v = XType.toString(m_batches[field - 1]) + " - " + XType.toString(m_batches[field]);
			return new MGName(v, new BSTR(value));
		}
		
		@Override public void collectEmptyGroups(BdLongSet values) {
			for(long l = 0; l <= m_batches.length; l++) {
				values.add(l);
			}
		}
		
		@Override public boolean requiresOrdering() { return false; }
	}

	public static class NumFloatBatchField extends MFCollector {
		private float[] m_batches;
		
		public NumFloatBatchField(CubeSearcher searcher, List<? extends Object> batches) {
			super(searcher);
			m_batches = new float[batches.size()];
			for(int i = 0; i < m_batches.length; i++) {
				m_batches[i] = Float.parseFloat(batches.get(i).toString());
			}
		}
		
		@Override public void collect(long doc, BdLongSet values) { 
			double d = Float.intBitsToFloat((int)doc);
			int pos = 0;
			while(pos < m_batches.length && m_batches[pos] <= d) pos++;
			values.add((long)pos);
		}

		@Override public MGName getField(long value) {
			int field = (int)value;
			String v = null;
			if(field == 0) v = "< " + XType.toString(m_batches[0]);
			else if(field == m_batches.length) v = ">= " + XType.toString(m_batches[field - 1]);
			else v = XType.toString(m_batches[field - 1]) + " - " + XType.toString(m_batches[field]);
			return new MGName(v, new BSTR(value));
		}
		
		@Override public void collectEmptyGroups(BdLongSet values) {
			for(long l = 0; l <= m_batches.length; l++) {
				values.add(l);
			}
		}
		
		@Override public boolean requiresOrdering() { return false; }
	}
	
	public static class TextField extends MFCollector
	{
		private ValueSearcher m_valueSearcher;
		
		public TextField(CubeSearcher searcher, FieldDefinition fieldDef) {
			super(searcher);
			m_valueSearcher = searcher.getValueSearcher(fieldDef.getTableName(), fieldDef.getName());
		}
		@Override public void collect(long doc, BdLongSet values) { values.add(doc); }
		@Override public MGName getField(long value) {
			String name = m_valueSearcher.getValue((int)value).toString();
			return new MGName(name, new BSTR(name.toLowerCase()));
		}
		
		@Override public boolean requiresOrdering() { return true; }
	}

	public static class IdField extends MFCollector
	{
		private IdSearcher m_idSearcher;
		
		public IdField(CubeSearcher searcher, TableDefinition tableDef) {
			super(searcher);
			m_idSearcher = searcher.getIdSearcher(tableDef.getTableName());
		}
		@Override public void collect(long doc, BdLongSet values) { values.add(doc); }
		@Override public MGName getField(long value) { return new MGName(m_idSearcher.getId((int)value).toString()); }
		@Override public boolean requiresOrdering() { return true; }
	}
	
	public static class EndNumField extends MFCollector
	{
		private NumSearcherMV m_numSearcher;
		private MFCollector m_collector;
		
		public EndNumField(CubeSearcher searcher, FieldDefinition fieldDef, AggregationGroup group) {
			super(searcher);
			m_numSearcher = searcher.getNumSearcher(fieldDef.getTableName(), fieldDef.getName());
			switch(fieldDef.getType()) {
				case BOOLEAN:
					m_collector = new BooleanField(searcher);
					break;
				case INTEGER:
				case LONG:
					if(group.batch != null) m_collector = new NumBatchField(searcher, group.batch);
					else m_collector = new LongField(searcher);
					break;
				case FLOAT:
					if(group.batch != null) m_collector = new NumFloatBatchField(searcher, group.batch);
					else m_collector = new FloatField(searcher);
					break;
				case DOUBLE:
					if(group.batch != null) m_collector = new NumDoubleBatchField(searcher, group.batch);
					else m_collector = new DoubleField(searcher);
					break;
				case TIMESTAMP:
					if(group.subField != null) m_collector = new DateSubField(searcher, group.subField);
					else m_collector = new DateField(searcher, group.truncate, group.timeZone);
					break;
				default: throw new IllegalArgumentException("Unsupported type: " + fieldDef.getType().toString());
			}
		}
		
		@Override public void collect(long doc, BdLongSet values) {
			int fcount = m_numSearcher.size((int)doc);
			for(int index = 0; index < fcount; index++) {
				m_collector.collect(m_numSearcher.get((int)doc, index), values);
			}
		}
		
		@Override public void collectEmptyGroups(BdLongSet values) { m_collector.collectEmptyGroups(values); }
		@Override public MGName getField(long value) { return m_collector.getField(value); }
		@Override public boolean requiresOrdering() { return m_collector.requiresOrdering(); }
	}
	
	public static class EndTextField extends MFCollector
	{
		private FieldSearcher m_fieldSearcher;
		private MFCollector m_collector;
		private IntIterator m_iter = new IntIterator();
		
		public EndTextField(CubeSearcher searcher, FieldDefinition fieldDef) {
			super(searcher);
			m_fieldSearcher = searcher.getFieldSearcher(fieldDef.getTableName(), fieldDef.getName());
			m_collector = new TextField(searcher, fieldDef);
		}
		
		@Override public void collect(long doc, BdLongSet values) {
			m_fieldSearcher.fields((int)doc, m_iter);
			for(int i = 0; i < m_iter.count(); i++) {
				m_collector.collect(m_iter.get(i), values);
			}
		}
		
		@Override public void collectEmptyGroups(BdLongSet values) { m_collector.collectEmptyGroups(values); }
		
		@Override public MGName getField(long value) { return m_collector.getField(value); }
		@Override public boolean requiresOrdering() { return m_collector.requiresOrdering(); }
	}
	
	public static class LinkField extends MFCollector
	{
		private Result m_filter;
		private FieldSearcher m_fieldSearcher;
		private MFCollector m_collector;
		private IntIterator m_iter = new IntIterator();
		
		public LinkField(CubeSearcher searcher, FieldDefinition fieldDef, Result filter, MFCollector inner) {
			super(searcher);
			m_filter = filter;
			m_fieldSearcher = searcher.getFieldSearcher(fieldDef.getTableName(), fieldDef.getName());
			m_collector = inner;
		}
		
		@Override public void collect(long doc, BdLongSet values) {
			m_fieldSearcher.fields((int)doc, m_iter);
			for(int i = 0; i < m_iter.count(); i++) {
				int d = m_iter.get(i);
				if(m_filter != null && !m_filter.get(d)) continue;
				m_collector.collect(m_iter.get(i), values);
			}
		}
		
		@Override public void collectEmptyGroups(BdLongSet values) { m_collector.collectEmptyGroups(values); }
		@Override public MGName getField(long value) { return m_collector.getField(value); }
		@Override public boolean requiresOrdering() { return m_collector.requiresOrdering(); }
	}

	
	public static class TransitiveLinkField extends MFCollector
	{
		private Result m_filter;
		private FieldSearcher m_fieldSearcher;
		private MFCollector m_collector;
		private IntIterator m_iter = new IntIterator();
		private BdLongSet m_set;
		private int m_depth;
		
		
		public TransitiveLinkField(CubeSearcher searcher, FieldDefinition fieldDef, int depth, Result filter, MFCollector inner) {
			super(searcher);
			m_filter = filter;
			m_fieldSearcher = searcher.getFieldSearcher(fieldDef.getTableName(), fieldDef.getName());
			m_collector = inner;
			
			m_set = new BdLongSet(1024);
			m_set.enableClearBuffer();
			m_depth = Math.min(depth, 1024);
			if(m_depth == 0) m_depth = 1024;
		}
		
		@Override public void collect(long doc, BdLongSet values) {
			m_set.clear();
			m_set.add(doc);
			int last_size = 0;
			
			for(int depth = 0; depth < m_depth; depth++) {
				int current_size = m_set.size(); 
				if(current_size == last_size) break;
				for(int i = last_size; i < current_size; i++) {
					doc = m_set.get(i);
					m_fieldSearcher.fields((int)doc, m_iter);
					for(int j = 0; j < m_iter.count(); j++) {
						int d = m_iter.get(j);
						m_set.add(d);
					}
				}
				last_size = current_size;
			}
			
			for(int i = 1; i < m_set.size(); i++) {
				long d = m_set.get(i);
				if(m_filter != null && !m_filter.get((int)d)) continue;
				m_collector.collect(d, values);
			}
			
			m_set.clear();
		}
		
		@Override public void collectEmptyGroups(BdLongSet values) { m_collector.collectEmptyGroups(values); }
		@Override public MGName getField(long value) { return m_collector.getField(value); }
		@Override public boolean requiresOrdering() { return m_collector.requiresOrdering(); }
	}


	public static class BatchexCollector extends MFCollector {
		private String[] m_aliases;
		private Result[] m_filters;
		
		public BatchexCollector(CubeSearcher searcher, TableDefinition tableDef, List<String> aliases, List<Query> queries) {
			super(searcher);
			m_aliases = new String[queries.size()];
			m_filters = new Result[queries.size()];
			for(int i = 0; i < queries.size(); i++) {
				m_aliases[i] = aliases.get(i);
				m_filters[i] = ResultBuilder.search(tableDef, queries.get(i), searcher);
			}
		}
		
		@Override public void collect(long doc, BdLongSet values) {
			for(int i=0; i<m_aliases.length; i++) {
				if(m_filters[i].get((int)doc)) values.add(i);
			}
		}

		@Override public MGName getField(long value) {
			return new MGName(m_aliases[(int)value], new BSTR(value));
		}
		
		@Override public void collectEmptyGroups(BdLongSet values) {
			for(long l = 0; l < m_aliases.length; l++) {
				values.add(l);
			}
		}
		
		@Override public boolean requiresOrdering() { return false; }
	}
	
	
	public static class FilteredCollector extends MFCollector
	{
		private Result m_filter;
		private MFCollector m_collector;
		
		public FilteredCollector(Result filter, MFCollector inner) {
			super(inner.searcher);
			m_filter = filter;
			m_collector = inner;
		}
		
		@Override public void collect(long doc, BdLongSet values) {
			if(doc < 0 || m_filter.get((int)doc)) m_collector.collect(doc, values);
		}
		
		@Override public void collectEmptyGroups(BdLongSet values) { m_collector.collectEmptyGroups(values); }
		@Override public MGName getField(long value) { return m_collector.getField(value); }
		@Override public boolean requiresOrdering() { return m_collector.requiresOrdering(); }
	}
	
}
