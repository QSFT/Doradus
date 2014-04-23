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

package com.dell.doradus.search.filter;

import java.util.List;
import java.util.Set;

import com.dell.doradus.common.CommonDefs;
import com.dell.doradus.common.Utils;
import com.dell.doradus.search.aggregate.Entity;
import com.dell.doradus.search.analyzer.SimpleText;

public class FilterContains implements Filter {
    private String m_field;
    private String m_value;
    
    public FilterContains(String field, String value) {
        m_field = field;
        m_value = value;
    }

    
    public static boolean compare(String fieldValue, String value) {
		if(fieldValue == null) return false;
		else if(value == null || value.equals("*"))return true;
		else if(fieldValue.indexOf(CommonDefs.MV_SCALAR_SEP_CHAR) >= 0) {
			for(String subvalue: Utils.split(fieldValue, CommonDefs.MV_SCALAR_SEP_CHAR)) {
				if(compare(subvalue, value)) return true;
			}
			return false;
		}
		
		List<String> fieldTokens = new SimpleText().tokenize(fieldValue);
		List<String> valueTokens = new SimpleText().tokenizeWithWildcards(value);
		
		if(fieldTokens.size() == 0) return false;
		if(valueTokens.size() == 0) return true;
		if(valueTokens.size() > fieldTokens.size()) return false;
		
		for(int i=0; i<=fieldTokens.size() - valueTokens.size(); i++) {
			boolean match = true;
			for(int j=0; j<valueTokens.size(); j++) {
				if(!Utils.matchesPattern(fieldTokens.get(i+j), valueTokens.get(j))) {
					match = false;
					break;
				}
			}
			if(match) return true;
		}
		return false;
    }
    

    @Override public boolean check(Entity entity) {
    	String fieldValue = entity.get(m_field);
    	return FilterContains.compare(fieldValue, m_value);
    }

    @Override public void addFields(Set<String> fields) {
        fields.add(m_field);
    }
    
}
