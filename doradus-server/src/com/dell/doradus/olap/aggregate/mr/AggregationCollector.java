package com.dell.doradus.olap.aggregate.mr;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dell.doradus.olap.aggregate.MetricCollectorSet;
import com.dell.doradus.olap.aggregate.MetricValueSet;
import com.dell.doradus.olap.collections.BdLongMap;
import com.dell.doradus.search.aggregate.AggregationGroup;
import com.dell.doradus.search.util.HeapSort;

public class AggregationCollector {
	private static List<Group> EMPTY_GROUPS = new ArrayList<Group>(0);
	private MetricCollectorSet m_mcs;
	private int m_documentsCount;
	private Group m_group;
	private AggregationTokenizer m_tokenizer;
	private AggregationChangeCasing m_changeCasing;
	private AggregationIncludeExclude m_includeExclude;

	public AggregationCollector(int documents) { m_documentsCount = documents; }
	
	public AggregationCollector(MFCollectorSet mfc, AggregationCollectorRaw rawCollector, List<AggregationGroup> groups) {
		m_tokenizer = new AggregationTokenizer(groups);
		m_changeCasing = new AggregationChangeCasing(groups);
		m_includeExclude = new AggregationIncludeExclude(groups);
		m_mcs = rawCollector.getMetricCollectorSet();
		m_documentsCount = rawCollector.documentsCount();
		if(m_documentsCount == 0) return;
		List<BdLongMap<MGName>> namesMap = rawCollector.createNamesMap(mfc);
		m_group = new Group(MGName.NullGroup, rawCollector.getGroup().getMetric());
		m_group.add(mfc, namesMap, rawCollector.getGroup(), 0);
		m_group.resort(0);
	}
	
	public int documentsCount() { return m_documentsCount; }
	public Group getGroup() { return m_group; }
	
	public void merge(AggregationCollector collector) {
		m_documentsCount += collector.m_documentsCount;
		if(m_group == null) m_group = collector.m_group;
		else if(collector.m_group != null) m_group.merge(collector.m_group);
	}
	
	public class Group implements Comparable<Group> {
		private MGName m_key;
		private MetricValueSet m_value;
		private List<Group> m_groups;
		
		public Collection<Group> groups() { return m_groups == null ? EMPTY_GROUPS : m_groups; }

		Group(MGName name, MetricValueSet metric) {
			m_key = name;
			m_value = metric;
			m_mcs.convert(metric);
		}
		
		void add(MFCollectorSet mfc, List<BdLongMap<MGName>> namesMap, AggregationCollectorRaw.Group rawGroup, int level) {
			if(rawGroup.groups() == null) return;
			m_groups = new ArrayList<Group>(rawGroup.groups().size());
			for(int i = 0; i < rawGroup.groups().size(); i++) {
				AggregationCollectorRaw.Group rawChild = rawGroup.groups().getAt(i);
				MGName key = MGName.NullGroup;
				if(rawChild.getKey() != Long.MIN_VALUE) {
					key = namesMap.get(level) == null ?
							mfc.collectors[level].getField(rawChild.getKey()) :
							namesMap.get(level).get(rawChild.getKey());
				}
				if(!m_includeExclude.accept(key, level)) continue;
				Group child = new Group(key, rawChild.getMetric());
				m_groups.add(child);
				child.add(mfc, namesMap, rawChild, level + 1);
			}
			if(m_groups.size() == 0) m_groups = null;
		}
		
		@Override public int compareTo(Group o) {
			return m_key.compareTo(o.m_key);
		}
		
		@Override public int hashCode() {
			return m_key.hashCode();
		}
		
		@Override public boolean equals(Object obj) {
			Group o = (Group)obj;
			return m_key.equals(o.m_key); 
		}
		
		@Override public String toString() {
			return m_key.toString() + "=" + m_value.toString();
		}
		
		public void add(MetricValueSet metric) { m_value.add(metric); }
		
		public MGName getKey() { return m_key; }
		public MetricValueSet getValue() { return m_value; }
		
		public void merge(Group group) {
			if(group == null) return;
			m_value.add(group.m_value);
			if(group.m_groups == null) return;
			
			if(m_groups == null) {
				m_groups = group.m_groups;
				return;
			}
			
			HeapSort<Group> sgroups = new HeapSort<Group>();
			sgroups.add(m_groups);
			sgroups.add(group.m_groups);

			List<Group> newgroups = new ArrayList<Group>();
			
			Group current = null;
			for(Group next: sgroups) {
				if(current == null) current = next;
				else if(current.equals(next)) {
					current.merge(next);
					newgroups.add(current);
					current = null;
				} else {
					newgroups.add(current);
					current = next;
				}
			}
			if(current != null) newgroups.add(current);
			m_groups = newgroups;
		}

		void resort(int level) {
			if(m_groups == null) return;
			for(Group grp: m_groups) {
				grp.resort(level + 1);
			}
			
			if(m_tokenizer.needsTokenizing(level)) {
				Map<MGName, Group> map = new HashMap<MGName, Group>();
				for(Group grp: m_groups) {
					if(grp.m_key == MGName.NullGroup) {
						map.put(MGName.NullGroup, grp);
						continue;
					}
					for(MGName term: m_tokenizer.tokenize(level, grp.m_key)) {
						Group g = map.get(term);
						if(g == null) {
							MetricValueSet v = m_mcs.get();
							g = new Group(term, v);
							map.put(term, g);
						}
						g.m_value.add(grp.m_value);
					}
				}
				m_groups = new ArrayList<Group>(map.values());
			}
			
			if(m_changeCasing.needsChangeCasing(level)) {
				for(Group grp: m_groups) {
					m_changeCasing.changeCase(level, grp.m_key);
				}
			}
			
			Collections.sort(m_groups);
			
			boolean needsMerging = false;
			Group last = null;
			for(Group group: m_groups) {
				if(last != null && last.equals(group)) {
					needsMerging = true;
					break;
				}
				last = group;
			}
			if(!needsMerging) return;
			List<Group> newgroups = new ArrayList<Group>();
			last = null;
			for(Group group: m_groups) {
				if(last != null && last.equals(group)) last.merge(group);
				else {
					if(last != null) newgroups.add(last);
					last = group;
				}
			}
			if(last != null) newgroups.add(last);
			m_groups = newgroups;
		}
		
	}

}
