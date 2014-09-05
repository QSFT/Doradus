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

public class FilterLinkExists implements Filter {
	private String m_link;
    
    public FilterLinkExists(String link) {
		m_link = link;
    }
    
    @Override public boolean check(Entity entity) {
        EntitySequence links = entity.getLinkedEntities(m_link, Searcher.EMPTY_ARRAY);
        return links.iterator().hasNext();
    }

	@Override public void addFields(Set<String> fields) { }

}
