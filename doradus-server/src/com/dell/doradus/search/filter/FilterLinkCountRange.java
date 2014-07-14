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

import com.dell.doradus.search.Searcher;
import com.dell.doradus.search.aggregate.Entity;
import com.dell.doradus.search.aggregate.EntitySequence;
import com.dell.doradus.search.query.RangeQuery;

public class FilterLinkCountRange implements Filter {
	private String m_link;
	private int m_min;
	private int m_max;
	private Filter m_inner;
    
    public FilterLinkCountRange(String link, RangeQuery query, Filter inner) {
		m_link = link;
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
		m_inner = inner;
    }

    @Override public boolean check(Entity entity) {
        EntitySequence links = entity.getLinkedEntities(m_link, Searcher.EMPTY_ARRAY);
        
        int count = 0;
        for(Entity e: links) {
        	if(e == null)break; // to suppress "unused" warning
			if(m_inner != null) {
				if(!m_inner.check(e)) continue;
			}
        	count++;
        	if(m_max <= count) return false;
        }
        return count >= m_min && count < m_max;
    }


	@Override public void addFields(Set<String> fields) { }

}
