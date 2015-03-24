package com.dell.doradus.olap.search;

import com.dell.doradus.olap.collections.BdLongSet;
import com.dell.doradus.search.aggregate.SortOrder;

public class SortKey implements Comparable<SortKey> {
	private int m_doc;
	private SortValue[] m_values;
	
	public SortKey(SortOrder[] orders) {
		m_values = new SortValue[orders.length];
		for(int i = 0; i < orders.length; i++) {
			m_values[i] = new SortValue(orders[i].ascending);
		}
	}
	
	public void set(int doc, BdLongSet[] sets) {
		m_doc = doc;
		for(int i = 0; i < m_values.length; i++) {
			m_values[i].set(sets[i]);
		}
	}
	
	public int doc() { return m_doc; }
	
	@Override public int compareTo(SortKey other) {
		int c = 0;
		for(int i = 0; i < m_values.length; i++) {
			c = m_values[i].compareTo(other.m_values[i]);
			if(c != 0) return c;
		}
		c = m_doc > other.m_doc ? 1 : m_doc < other.m_doc ? -1 : 0;
		return c;
	}

}
