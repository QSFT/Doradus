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

package com.dell.doradus.search.aggregate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.dell.doradus.common.Utils;

public class Matcher {
	private Set<String> m_values;
	private List<String> m_templates;
	
	public Matcher(Collection<String> values) {
		if(values == null) return;
		m_values = new HashSet<String>();
		m_templates = new ArrayList<String>();
		
		for(String value: values) {
			if(value == null) m_values.add(null);
			else if(value.indexOf('*') >= 0 || value.indexOf('?') >= 0) m_templates.add(value);
			else m_values.add(value.toLowerCase());
		}
		
		if(m_values.size() == 0) m_values = null;
		if(m_templates.size() == 0) m_templates = null;
	}
	
	public boolean match(String value)
	{
		if(value == null) return m_values != null && m_values.contains(null);
		if(m_values != null && m_values.contains(value)) return true;
		if(m_templates == null) return false;
		for(String template: m_templates) {
			if(Utils.matchesPattern(value, template)) return true;
		}
		return false;
	}
}
