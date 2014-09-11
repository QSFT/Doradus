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

import com.dell.doradus.common.Utils;
import com.dell.doradus.search.aggregate.AggregationGroup;
import com.dell.doradus.search.aggregate.Matcher;

public class AggregationIncludeExclude {
	private List<Matcher> m_exclude = new ArrayList<Matcher>();
	private List<Matcher> m_include = new ArrayList<Matcher>();
	
	public AggregationIncludeExclude(List<AggregationGroup> groups) {
		for(int i = 0; i < groups.size(); i++) {
			List<String> exclude = groups.get(i).exclude;
			if(exclude == null) m_exclude.add(null);
			else m_exclude.add(new Matcher(exclude));
			
			List<String> include = groups.get(i).include;
			if(include == null) m_include.add(null);
			else m_include.add(new Matcher(include));
		}
	}
	
	public boolean accept(MGName name, int level) {
		String text = name.name == null ? null : name.name.toLowerCase();
		Matcher excl = m_exclude.get(level);
		if(excl != null && excl.match(text)) return false;
		Matcher incl = m_include.get(level);
		if(incl != null && (!incl.match(text))) return false;
		
		
		return true;
	}
	
}
