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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.dell.doradus.common.Utils;
import com.dell.doradus.olap.aggregate.AggregationGroupChangeCasing;
import com.dell.doradus.olap.aggregate.AggregationGroupTokenizer;
import com.dell.doradus.olap.aggregate.AggregationResult;
import com.dell.doradus.olap.aggregate.MetricCollectorSet;
import com.dell.doradus.olap.aggregate.MetricValueSet;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.search.aggregate.AggregationGroup;

public class MGBuilder {
	private MetricCollectorSet m_metricSet;
	private Map<MGValue, MetricValueSet> m_groups = new HashMap<MGValue, MetricValueSet>();
	private int m_documents;
	
	public MGBuilder(CubeSearcher searcher, MetricCollectorSet metricSet, int groups) {
		m_metricSet = metricSet;
	}
	
	public void add(int doc, List<Set<Long>> sets, MetricValueSet valueSet) {
		MGValue value = new MGValue(sets.size());
		addValue(value, sets, valueSet, 0);
		if(doc >= 0) m_documents++;
	}
	
	private void addValue(MGValue value, List<Set<Long>> sets, MetricValueSet valueSet, int index) {
		if(index == sets.size()) {
			MetricValueSet oldSet = m_groups.get(value);
			if(oldSet != null) {
				oldSet.add(valueSet);
				return;
			}
			
			MGValue newval = new MGValue(value);
			MetricValueSet newSet = m_metricSet.get(-1);
			newSet.add(valueSet);
			m_groups.put(newval, newSet);
			return;
		}
		
		if(sets.get(index).isEmpty()) {
			value.Values[index] = Long.MIN_VALUE;
			addValue(value, sets, valueSet, index + 1);
			return;
		}
		
		for(Long l : sets.get(index)) {
			value.Values[index] = l;
			addValue(value, sets, valueSet, index + 1);
		}
	}
	
	public AggregationResult createResult(List<AggregationGroup> ag, MetricCollectorSet collectorSet, MFCollectorSet mfc) {
		if(ag == null) ag = new ArrayList<AggregationGroup>(0);
		int len = mfc.collectors.length;
		//1. Fill set of MGValues
		List<Set<Long>> valuesSet = new ArrayList<Set<Long>>(len);
		for(int i = 0; i < len; i++) valuesSet.add(new HashSet<Long>());
		for(MGValue mg : m_groups.keySet()) {
			for(int i = 0; i < len; i++) {
				valuesSet.get(i).add(mg.Values[i]);
			}
		}
		
		//2. Map value to name
		List<Map<Long, MGName>> mapSet = new ArrayList<Map<Long, MGName>>();
		for(int i = 0; i < len; i++) {
			Map<Long, MGName> map = new HashMap<Long, MGName>();
			mapSet.add(map);
			List<Long> list = new ArrayList<Long>(valuesSet.get(i));
			Collections.sort(list);
			for(Long l : list) if(l != Long.MIN_VALUE) map.put(l, mfc.collectors[i].getField(l));
		}
		
		//2.1 Exclude list
		List<Set<String>> exList = new ArrayList<Set<String>>();
		for(int i=0; i<ag.size(); i++) {
			List<String> exclude = ag.get(i).exclude;
			if(exclude == null) exList.add(new HashSet<String>(0));
			else exList.add(new HashSet<String>(exclude));
		}
		//2.2 Include list
		List<Set<String>> inList = new ArrayList<Set<String>>();
		for(int i=0; i<ag.size(); i++) {
			List<String> include = ag.get(i).include;
			if(include == null) inList.add(null);
			else inList.add(new HashSet<String>(include));
		}
		if(ag.size() == 0) {
			exList.add(new HashSet<String>(0));
			inList.add(null);
		}

		//3. Create result
		AggregationResult result = new AggregationResult();
		result.documentsCount = m_documents;
		AggregationResult.AggregationGroup summary = new AggregationResult.AggregationGroup();
		List<MGValue> values = new ArrayList<MGValue>(m_groups.keySet());
		Collections.sort(values);
		summary.metricSet = collectorSet.get(-1);
		collectorSet.convert(summary.metricSet);
		result.summary = summary;
		for(MGValue mg : values) {
			MetricValueSet value = m_groups.get(mg);
			collectorSet.convert(value);
			AggregationResult r = result;
			for(int i = 0; i < mg.Values.length; i++) {
				AggregationResult.AggregationGroup g = new AggregationResult.AggregationGroup();
				Long l = mg.Values[i];
				if(l != Long.MIN_VALUE) {
					MGName mgName = mapSet.get(i).get(mg.Values[i]);
					g.id = mgName.id;
					g.name = mgName.name;
				}
				if(exList.get(i).contains(g.name)) break;
				if(inList.get(i) != null && !inList.get(i).contains(g.name)) break;
				if(r.groups.size() > 0 && r.groups.get(r.groups.size() - 1).equals(g)) {
					g = r.groups.get(r.groups.size() - 1);
				} else if(r.groups.size() > 0 && r.groups.get(r.groups.size() - 1).compareTo(g) > 0) {
					int idx = r.groups.lastIndexOf(g);
					if(idx >= 0) g = r.groups.get(idx);
					else {
						g.metricSet = collectorSet.get(-1);
						collectorSet.convert(g.metricSet);
						r.groups.add(g);
						Collections.sort(r.groups);
					}
				} else {
					g.metricSet = collectorSet.get(-1);
					collectorSet.convert(g.metricSet);
					r.groups.add(g);
				}
				g.metricSet.add(value);
				if(i < mg.Values.length - 1) {
					if(g.innerResult != null) r = g.innerResult;
					else {
						r = new AggregationResult();
						g.innerResult = r;
					}
				} else summary.metricSet.add(value);
			}
		}

		for(int i=0; i<ag.size(); i++) {
			if(ag.get(i).stopWords != null) {
				AggregationGroupTokenizer.tokenizeValues(i, result, collectorSet);
			}
		}

		for(int i=0; i<ag.size(); i++) {
			if(ag.get(i).tocase != null) {
				String tocase =  ag.get(i).tocase;
				Utils.require("UPPER".equals(tocase) || "LOWER".equals(tocase), "only UPPER and LOWER casing are supported");
				AggregationGroupChangeCasing.changeCasing(i, result, "UPPER".equals(tocase));
			}
		}
		
		return result;
	}
	
}



