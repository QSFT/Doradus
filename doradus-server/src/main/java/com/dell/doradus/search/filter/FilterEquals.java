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

import java.util.Set;

import com.dell.doradus.common.CommonDefs;
import com.dell.doradus.common.Utils;
import com.dell.doradus.search.aggregate.Entity;

public class FilterEquals implements Filter {
    private String m_field;
    private String m_value;
    
    public FilterEquals(String field, String value) {
        m_field = field;
        m_value = value;
    }
    
    public static boolean compare(String fieldValue, String value) {
    	if(fieldValue == null && value == null) return true;
    	else if(fieldValue == null || value == null) return false;
		else if(value.equals("*"))return true;
		else if(fieldValue.indexOf(CommonDefs.MV_SCALAR_SEP_CHAR) >= 0) {
			for(String subvalue: Utils.split(fieldValue, CommonDefs.MV_SCALAR_SEP_CHAR)) {
				if(compare(subvalue, value)) return true;
			}
			return false;
		}
		else return Utils.matchesPattern(fieldValue, value);
    }
    

    @Override public boolean check(Entity entity) {
        String fieldValue = entity.get(m_field);
        return FilterEquals.compare(fieldValue, m_value);
    }

    @Override public void addFields(Set<String> fields) {
        fields.add(m_field);
    }
    
}
