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

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Set;
import java.util.TimeZone;

import com.dell.doradus.common.Utils;
import com.dell.doradus.search.aggregate.Entity;
import com.dell.doradus.search.analyzer.DateTrie;

public class FilterDatePart implements Filter {
	private int m_part;
	private String m_field;
	private String m_value;
    private DateTrie m_trie = new DateTrie();
    
    public FilterDatePart(int part, String field, String value) {
        m_part = part;
        m_field = field;
        m_value = value;
    }
    
    @Override public boolean check(Entity entity) {
    	String fieldValue = entity.get(m_field);
    	if(fieldValue == null || fieldValue.length() == 0) return false;
		if(m_value == null || m_value.equals("*"))return true;
		Date date = m_trie.parse(fieldValue);
		Calendar cal = GregorianCalendar.getInstance(TimeZone.getTimeZone("GMT"));
		cal.setTime(date);
		int part = cal.get(m_part);
		//Months in Calendar start with 0 instead of 1
		if(m_part == Calendar.MONTH) part++;
		
		String fieldPart = "" + part;
		return Utils.matchesPattern(fieldPart, m_value);
    }

    @Override public void addFields(Set<String> fields) {
        fields.add(m_field);
    }
    
}
