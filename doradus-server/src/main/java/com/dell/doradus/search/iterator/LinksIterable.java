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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.core.ObjectID;
import com.dell.doradus.core.ServerParams;
import com.dell.doradus.service.spider.SpiderHelper;
import com.dell.doradus.service.spider.SpiderService;

public class LinksIterable implements Iterable<ObjectID> {
	private FieldDefinition m_link;
	private List<Integer> m_shards;
	private ObjectID m_continuation;
	private boolean m_inclusive;
	private Iterable<ObjectID> m_keys; 

    public LinksIterable(FieldDefinition link, List<Integer> shards, ObjectID continuation, boolean inclusive, Iterable<ObjectID> keys) {
    	m_link = link;
    	m_shards = shards;
    	if(m_shards == null) {
    		m_shards = new ArrayList<Integer>(1);
    		m_shards.add(0);
    		if(link.isSharded()) m_shards.addAll(SpiderService.instance().getShards(link.getInverseTableDef()).keySet());
    	}
    	m_continuation = continuation;
    	m_inclusive = inclusive;
    	m_keys = keys;
    }
    
	@Override public Iterator<ObjectID> iterator() {
		if(m_shards.size() == 0) return NoneIterator.instance;
		int maxobjects = 64;
		int maxmaxobjects = 64 * 1024;
		int count = ServerParams.instance().getModuleParamInt("DoradusServer", "dbesoptions_linkBuffer", 1000);
		List<ObjectID> keys = new ArrayList<ObjectID>();
		for(ObjectID key : m_keys) {
			keys.add(key);
		}
		if(keys.size() == 0) return NoneIterator.instance;
		if(keys.size() * m_shards.size() == 1) {
			List<ObjectID> lst = SpiderHelper.getLinks(m_link, m_shards.get(0), keys.get(0), m_continuation, m_inclusive, count);
			return new LinkIterator(m_link, m_shards.get(0), keys.get(0), count, lst);
		}
		if(keys.size() <= maxobjects) {
			OrIterable or = new OrIterable(m_shards.size() * keys.size());
			for(Integer shard : m_shards) {
				Map<ObjectID, List<ObjectID>> map = SpiderHelper.getLinks(m_link, shard, keys, m_continuation, m_inclusive, count);
				for(Map.Entry<ObjectID, List<ObjectID>> e : map.entrySet()) {
					if(e.getValue().size() == 0) continue;
					or.add(new LinkIterable(m_link, shard, e.getKey(), count, e.getValue()));
				}
			}
			return or.iterator();
		}
		if(keys.size() <= maxmaxobjects) {
			OrIterable or = new OrIterable(keys.size());
			for(Integer shard : m_shards) {
				for(ObjectID key : keys) {
					List<ObjectID> lst = SpiderHelper.getLinks(m_link, shard, key, m_continuation, m_inclusive, count);
					or.add(new LinkIterable(m_link, shard, key, count, lst));
				}
			}
			return or.iterator();
		}

		Set<ObjectID> set = new HashSet<ObjectID>();
		for(Integer shard : m_shards) {
			for(ObjectID key : keys) {
				List<ObjectID> lst = SpiderHelper.getLinks(m_link, shard, key, m_continuation, m_inclusive, count);
				Iterable<ObjectID> iterator = new LinkIterable(m_link, shard, key, count, lst);
				for(ObjectID obj : iterator) set.add(obj);
			}
		}
		List<ObjectID> result = new ArrayList<ObjectID>(set.size());
		result.addAll(set);
		Collections.sort(result);
		return result.iterator();
	}
}
