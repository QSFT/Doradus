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

import java.util.List;

import com.dell.doradus.common.Utils;
import com.dell.doradus.search.aggregate.AggregationGroup;

public class AggregationChangeCasing {
	private boolean[] m_bToUpper;
	private boolean[] m_bToLower;
	
	public AggregationChangeCasing(List<AggregationGroup> groups) {
		m_bToUpper = new boolean[groups.size()];
		m_bToLower = new boolean[groups.size()];
		for(int i = 0; i < groups.size(); i++) {
			AggregationGroup group = groups.get(i);
			String tocase =  group.tocase;
			if(tocase == null) continue;
			if("UPPER".equals(tocase)) m_bToUpper[i] = true;
			else if("LOWER".equals(tocase)) m_bToLower[i] = true;
			else Utils.require(false, "only UPPER and LOWER casing are supported"); 
		}
	}
	
	public boolean needsChangeCasing(int level) { return m_bToUpper[level] || m_bToLower[level]; }
	
	public void changeCase(int level, MGName name) {
		if(m_bToUpper[level]) name.name = name.name.toUpperCase();
		else if (m_bToLower[level]) name.name = name.name.toLowerCase();
	}
	
}
