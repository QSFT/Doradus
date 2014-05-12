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

package com.dell.doradus.olap.store;

import java.util.Date;

import com.dell.doradus.common.FieldType;
import com.dell.doradus.olap.XType;
import com.dell.doradus.olap.io.VDirectory;
import com.dell.doradus.olap.io.VInputStream;
import com.dell.doradus.olap.search.Result;
import com.dell.doradus.search.analyzer.DateTrie;

public class NumSearcher {
	private NumArray m_values;
	private BitVector m_mask;
	private long m_min = Long.MAX_VALUE;
	private long m_min_pos = Long.MAX_VALUE;
	private long m_max = Long.MIN_VALUE;
	
	public NumSearcher(VDirectory dir, String table, String field) {
		if(!dir.fileExists(table + "." + field + ".dat")) return;
		VInputStream stream = dir.open(table + "." + field + ".dat");
		int size = stream.readVInt();
		int bits = stream.readByte();
		m_values = new NumArray(size, bits);
		m_values.load(stream);
		for(int i = 0; i < m_values.size(); i++) {
			long val = m_values.get(i);
			if(m_min > val) m_min = val;
			if(m_max < val) m_max = val;
			if(val > 0 && m_min_pos > val) m_min_pos = val;
		}
		
		if(dir.fileExists(table + "." + field + ".num.mask")) {
			m_mask = new BitVector(m_values.size());
			VInputStream mask_stream = dir.open(table + "." + field + ".num.mask");
			mask_stream.read(m_mask.getBuffer(), 0, m_mask.getBuffer().length);  
		}
	}

	public boolean isDefined(int doc) { return m_values == null ? false : m_mask == null ? true : m_mask.get(doc); }
	public boolean isNull(int doc) { return !isDefined(doc); }
	public int size() { return m_values == null ? 0 : m_values.size(); }
	public long get(int doc) { return isNull(doc) ? 0 : m_values.get(doc); }
	//public boolean getBoolean(int doc) { return get(doc) == 1; }
	//public Date getDate(int doc) { return isNull(doc) ? null : new Date(get(doc)); }
	public long min() { return m_min; }
	public long max() { return m_max; }
	public long minPos() { return m_min_pos; }
	
	public void fill(long value, Result r) {
		if(m_values == null) return;
		for(int i = 0; i < m_values.size(); i++) {
			if(isNull(i)) continue;
			if(m_values.get(i) != value) continue; 
			r.set(i);
		}
	}

	public void fillNull(Result r) {
		r.clear();
		if(m_values == null) { r.not(); return; }
		if(m_mask == null) { return; }
		for(int i = 0; i < m_values.size(); i++) {
			if(isNull(i)) r.set(i);
		}
	}
	
	public void fill(long start, long finish, Result r) {
		if(m_values == null) return;
		for(int i = 0; i < m_values.size(); i++) {
			if(isNull(i)) continue;
			if(m_values.get(i) < start) continue; 
			if(m_values.get(i) >= finish) continue; 
			r.set(i);
		}
	}
	
	public static boolean isNumericType(FieldType type) {
		return 	type == FieldType.BOOLEAN ||
				type == FieldType.INTEGER ||
				type == FieldType.LONG ||
				type == FieldType.TIMESTAMP ||
				type == FieldType.DOUBLE ||
				type == FieldType.FLOAT;
	}
	
	public static long parse(String value, FieldType type) {
		if(type == FieldType.BOOLEAN) return value.equalsIgnoreCase("true") ? 1 : 0;
		else if(type == FieldType.INTEGER || type == FieldType.LONG) return Long.parseLong(value);
		else if(type == FieldType.TIMESTAMP) return new DateTrie().parse(value).getTime(); 
		else if(type == FieldType.FLOAT) return Float.floatToRawIntBits(Float.parseFloat(value));
		else if(type == FieldType.DOUBLE) return Double.doubleToRawLongBits(Double.parseDouble(value));
		else throw new RuntimeException("Invalid numeric type: " + type);
	}

	public static String format(long value, FieldType type) {
		if(type == FieldType.BOOLEAN) return value == 1 ? "True" : "False";
		else if(type == FieldType.INTEGER || type == FieldType.LONG) return "" + value;
		else if(type == FieldType.TIMESTAMP) return value == 0 ? null : XType.toString(new Date(value));
		else if(type == FieldType.FLOAT) return XType.toString((double)Float.intBitsToFloat((int)value));
		else if(type == FieldType.DOUBLE) return XType.toString(Double.longBitsToDouble(value));
		else throw new RuntimeException("Invalid numeric type: " + type);
	}
	
	public long cacheSize()
	{
		return 16L + (m_values == null ? 0 : m_values.cacheSize()) + (m_mask == null ? 0 : m_mask.getBuffer().length);
	}
	
}
