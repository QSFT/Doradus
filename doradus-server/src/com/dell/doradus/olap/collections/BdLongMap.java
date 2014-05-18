package com.dell.doradus.olap.collections;

import java.util.ArrayList;
import java.util.List;

public class BdLongMap<V> {
	private BdLongSet m_set;
	private List<V> m_values;
	
	public BdLongMap(int capacity) {
		m_set = new BdLongSet(capacity);
		m_values = new ArrayList<V>(capacity);
	}

	//public BdLongMap(BdLongSet set) {
	//	m_set = set;
	//	m_set.restoreAfterSort();
	//	m_values = new ArrayList<V>(set.size());
	//}
	
	public int size() { return m_set.size(); }
	public V get(long key) { 
		int index = m_set.indexOf(key);
		if(index < 0) return null;
		return m_values.get(index);
	}
	
	public void put(long key, V value) {
		int index = m_set.add(key);
		if(index == m_values.size()) m_values.add(value);
		else m_values.set(index, value);
	}
	
}
