package com.dell.doradus.olap.aggregate.mr;

import java.util.ArrayList;
import java.util.List;

import com.dell.doradus.olap.aggregate.MetricCollectorSet;
import com.dell.doradus.olap.aggregate.MetricValueSet;
import com.dell.doradus.olap.collections.BdLongMap;
import com.dell.doradus.olap.collections.BdLongSet;

public class AggregationCollectorRaw {
	private MetricCollectorSet m_mcs;
	private int m_documentsCount;
	private int m_lastAddedDoc = -1;
	private Group m_group;

	public AggregationCollectorRaw(MetricCollectorSet mcs) {
		m_mcs = mcs;
		m_group = new Group(Long.MIN_VALUE, mcs.get());
	}
	
	public int documentsCount() { return m_documentsCount; }
	
	public Group getGroup() { return m_group; }
	
	public MetricCollectorSet getMetricCollectorSet() { return m_mcs; }
	
	public void add(int doc, BdLongSet[] keys, MetricValueSet metric) {
		if(doc >= 0) {
			if(doc != m_lastAddedDoc) {
				m_documentsCount++;
				m_group.m_value.add(metric);
				m_lastAddedDoc = doc;
			}
		}
		m_group.add(doc, keys, 0, metric);
	}
	
	public List<BdLongMap<MGName>> createNamesMap(MFCollectorSet mfc) {
		int len = mfc.size();
		//1. Fill set of MGValues
		BdLongSet[] valuesSet = new BdLongSet[len];
		for(int i = 0; i < len; i++) {
			if(!mfc.collectors[i].requiresOrdering()) continue;
			valuesSet[i] = new BdLongSet(1024);
		}
		
		m_group.fillValueSet(valuesSet, 0);
		
		//2. Map value to name
		List<BdLongMap<MGName>> mapSet = new ArrayList<BdLongMap<MGName>>(valuesSet.length);
		for(int i = 0; i < len; i++) {
			BdLongSet vset = valuesSet[i];
			if(vset == null) {
				mapSet.add(null);
				continue;
			}
			vset.sort();
			BdLongMap<MGName> map = new BdLongMap<MGName>(vset.size());
			mapSet.add(map);
			for(int j = 0; j < vset.size(); j++) {
				long val = vset.get(j);
				if(val == Long.MIN_VALUE) continue;
				map.put(val, mfc.collectors[i].getField(val));
			}
		}
		return mapSet;
	}
	
	public class Group implements Comparable<Group> {
		private long m_key;
		private MetricValueSet m_value;
		private BdLongMap<Group> m_groups;
		private int m_lastDoc;
		
		Group(long key, MetricValueSet value) {
			m_key = key;
			m_value = value;
			m_lastDoc = -1;
		}

		public long getKey() { return m_key; }
		public MetricValueSet getMetric() { return m_value; }
		public BdLongMap<Group> groups() { return m_groups; }
		public void add(MetricValueSet metric) { m_value.add(metric); }
		
		@Override public int compareTo(Group o) {
			if(m_key > o.m_key) return 1;
			else if(m_key < o.m_key) return -1;
			else return 0;
		}
		
		@Override public int hashCode() {
			return (int)(m_key ^ (m_key>>>32));
		}
		
		@Override public boolean equals(Object obj) {
			Group g = (Group)obj;
			return m_key == g.m_key; 
		};
		
		@Override public String toString() {
			return "" + m_key + "=" + m_value.toString();
		}
		
		void add(int doc, BdLongSet[] keys, int index, MetricValueSet metric) {
			if(index == keys.length) return;
			BdLongSet keySet = keys[index];
			if(m_groups == null) m_groups = new BdLongMap<Group>(index == 0 ? 1024 : 4);
			for(int i = 0; i < keySet.size(); i++) {
				long key = keySet.get(i);
				Group group = m_groups.get(key);
				if(group == null) {
					MetricValueSet m = m_mcs.get();
					group = new Group(key, m);
					m_groups.put(key, group);
				}
				if(doc < 0 || doc != group.m_lastDoc) {
					group.m_value.add(metric);
					group.m_lastDoc = doc;
				}
				if(index + 1 < keys.length) group.add(doc, keys, index + 1, metric);
			}
			if(keySet.size() == 0) {
				long key = Long.MIN_VALUE;
				Group group = m_groups.get(key);
				if(group == null) {
					MetricValueSet m = m_mcs.get();
					group = new Group(key, m);
					m_groups.put(key, group);
				}
				if(doc < 0 || doc != group.m_lastDoc) {
					group.m_value.add(metric);
					group.m_lastDoc = doc;
				}
				if(index + 1 < keys.length) group.add(doc, keys, index + 1, metric);
			}
		}
		
		void fillValueSet(BdLongSet[] keys, int index) {
			if(m_groups == null) return;
			BdLongSet keySet = keys[index];
			
			for(int i = 0; i < m_groups.size(); i++) {
				Group child = m_groups.getAt(i);
				long key = child.getKey();
				if(key != Long.MIN_VALUE && keySet != null) keySet.add(key);
				child.fillValueSet(keys, index + 1);
			}
		}
		
	}

}
