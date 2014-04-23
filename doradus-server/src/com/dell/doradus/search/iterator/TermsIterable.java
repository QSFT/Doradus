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

package com.dell.doradus.search.iterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.core.ObjectID;
import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.service.spider.SpiderHelper;
import com.dell.doradus.service.spider.SpiderService;

public class TermsIterable implements Iterable<ObjectID> {
	private TableDefinition m_table;
	private List<Integer> m_shards;
	private ObjectID m_continuation;
	private boolean m_inclusive;
	private List<String> m_terms = new ArrayList<String>();

    public TermsIterable(TableDefinition table, List<Integer> shards, ObjectID continuation, boolean inclusive) {
    	m_table = table;
    	m_shards = shards;
    	if(m_shards == null) {
    		m_shards = new ArrayList<Integer>(1);
    		m_shards.add(0);
    		if(table.isSharded()) {
    			Set<Integer> allShards = SpiderService.instance().getShards(table).keySet();
    			m_shards.addAll(allShards);
    		}
    	}
    	m_continuation = continuation;
    	m_inclusive = inclusive;
    }
    
    public void add(String term) {
    	m_terms.add(term);
    }
	
	@Override public Iterator<ObjectID> iterator() {
		int count = ServerConfig.getInstance().dbesoptions_linkBuffer; // 1000
		if(m_shards.size() == 0) return NoneIterator.instance;
		if(m_terms.size() == 0) return NoneIterator.instance;
		
		if(m_shards.size() * m_terms.size() == 1) {
			List<ObjectID> lst = SpiderHelper.getTermDocs(m_table, m_shards.get(0), m_terms.get(0), m_continuation, m_inclusive, count);
			return new TermIterable(m_table, m_shards.get(0), m_terms.get(0), count, lst).iterator();
		}
		
		OrIterable or = new OrIterable(m_shards.size() * m_terms.size());
		
		for(Integer shard : m_shards) {
			Map<String, List<ObjectID>> map = SpiderHelper.getTermDocs(m_table, shard, m_terms, m_continuation, m_inclusive, count);
			for(Map.Entry<String, List<ObjectID>> e : map.entrySet()) {
				if(e.getValue().size() == 0) continue;
				or.add(new TermIterable(m_table, shard, e.getKey(), count, e.getValue()));
			}
		}
		return or.iterator();
	}
	
}
