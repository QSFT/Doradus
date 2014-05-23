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
				for(int j = 0; j < exclude.size(); j++) exclude.set(j, exclude.get(j).toLowerCase());
				m_exclude.add(new HashSet<String>(exclude));
			}
			
			List<String> include = groups.get(i).include;
			if(include == null) m_include.add(null);
			else {
				for(int j = 0; j < include.size(); j++) include.set(j, include.get(j).toLowerCase());
				m_include.add(new HashSet<String>(include));
			}
		}
	}
	
	public boolean accept(MGName name, int level) {
		Set<String> excl = m_exclude.get(level);
		if(excl != null && name.name != null && excl.contains(name.name.toLowerCase())) return false;
		Set<String> incl = m_include.get(level);
		if(incl != null && (name.name == null || !incl.contains(name.name.toLowerCase()))) return false;
		return true;
	}
	
}
