package com.dell.doradus.olap.search;

import com.dell.doradus.olap.collections.BdLongSet;

public class SortValue implements Comparable<SortValue> {
	private boolean m_accending;
	private long[] m_values;
	private int m_count;
	
	public SortValue(boolean accending) {
		m_accending = accending;
		m_values = new long[10];
		m_count = 0;
	}
	
	public void set(BdLongSet values) {
		values.sort();
		m_count = Math.min(m_values.length, values.size());
		if(m_accending) {
			for(int i = 0; i < m_count; i++) {
				m_values[i] = values.get(i);
			}
		} else {
			for(int i = 0; i < m_count; i++) {
				m_values[i] = values.get(values.size() - i - 1);
			}
		}
	}

	@Override public int compareTo(SortValue other) {
		if(m_accending != other.m_accending) throw new RuntimeException("Invalid sort order");
		int minCount = Math.min(m_count, other.m_count);
		int c = 0;
		for(int i = 0; i < minCount; i++) {
			c = m_values[i] > other.m_values[i] ? 1 : m_values[i] < other.m_values[i] ? -1 : 0;
			if(c != 0) return m_accending ? c : -c;
		}
		c = m_count > other.m_count ? 1 : m_count < other.m_count ? -1 : 0;
		return m_accending ? c : -c;
	}
	
	
}
