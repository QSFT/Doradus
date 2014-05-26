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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.dell.doradus.search.aggregate.AggregationGroup;

public class AggregationIncludeExclude {
	private List<Set<String>> m_exclude = new ArrayList<Set<String>>();
	private List<Set<String>> m_include = new ArrayList<Set<String>>();
	
	public AggregationIncludeExclude(List<AggregationGroup> groups) {
		for(int i = 0; i < groups.size(); i++) {
			List<String> exclude = groups.get(i).exclude;
			if(exclude == null) m_exclude.add(null);
			else {
				HashSet<String> hs = new HashSet<String>(exclude.size());
				for(String str: exclude) hs.add(str == null ? null : str.toLowerCase());
				m_exclude.add(hs);
			}
			
			List<String> include = groups.get(i).include;
			if(include == null) m_include.add(null);
			else {
				HashSet<String> hs = new HashSet<String>(include.size());
				for(String str: include) hs.add(str == null ? null : str.toLowerCase());
				m_include.add(hs);
			}
		}
	}
	
	public boolean accept(MGName name, int level) {
		String text = name.name == null ? null : name.name.toLowerCase();
		Set<String> excl = m_exclude.get(level);
		if(excl != null && excl.contains(text)) return false;
		Set<String> incl = m_include.get(level);
		if(incl != null && (!incl.contains(text))) return false;
		return true;
	}
	
}
