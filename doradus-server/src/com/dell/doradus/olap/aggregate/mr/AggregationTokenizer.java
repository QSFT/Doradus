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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.dell.doradus.fieldanalyzer.SimpleTextAnalyzer;
import com.dell.doradus.search.aggregate.AggregationGroup;

public class AggregationTokenizer {
	private List<Set<String>> m_stopWords;
	
	public AggregationTokenizer(List<AggregationGroup> groups) {
		m_stopWords = new ArrayList<Set<String>>(groups.size());
		for(AggregationGroup group: groups) {
			List<String> stopWords = group.stopWords;
			if(stopWords != null) m_stopWords.add(new HashSet<String>(stopWords));
			else m_stopWords.add(null);
		}
	}
	
	public boolean needsTokenizing(int level) { return m_stopWords.get(level) != null; }
	
	
	public Collection<MGName> tokenize(int level, MGName name) {
		String[] tokens = SimpleTextAnalyzer.instance().tokenize(name.name);
		List<MGName> names = new ArrayList<MGName>(tokens.length);
		for(String token: tokens) {
			if(m_stopWords.contains(token)) continue;
			names.add(new MGName(token));
		}
		return names;
	}

}
