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
import com.dell.doradus.search.query.RangeQuery;

public class FilterFieldCountRange implements Filter {
	private String m_field;
	private int m_min;
	private int m_max;
    
    public FilterFieldCountRange(String field, RangeQuery query) {
    	m_field = field;
		m_min = Integer.MIN_VALUE;
		m_max = Integer.MAX_VALUE;
		if(query.min != null) {
			m_min = Integer.parseInt(query.min.toString());
			if(!query.minInclusive) m_min++;
		}
		if(query.max != null) {
			m_max = Integer.parseInt(query.max.toString());
			if(query.maxInclusive) m_max++;
		}
    }

    @Override public boolean check(Entity entity) {
        String value = entity.get(m_field);
        
        int count = 0;
        if(value != null) {
        	count =  value.indexOf(CommonDefs.MV_SCALAR_SEP_CHAR) == -1 ?
        			1 : Utils.split(value, CommonDefs.MV_SCALAR_SEP_CHAR).size();
        }
       	return count >= m_min && count < m_max;
    }


	@Override public void addFields(Set<String> fields) { }

}
