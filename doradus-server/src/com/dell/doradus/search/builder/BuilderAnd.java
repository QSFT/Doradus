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

package com.dell.doradus.search.builder;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

import com.dell.doradus.search.FilteredIterable;
import com.dell.doradus.search.analyzer.DateTrie;
import com.dell.doradus.search.filter.Filter;
import com.dell.doradus.search.filter.FilterAnd;
import com.dell.doradus.search.iterator.AndIterable;
import com.dell.doradus.search.iterator.NoneIterable;
import com.dell.doradus.search.query.AndQuery;
import com.dell.doradus.search.query.Query;
import com.dell.doradus.search.query.RangeQuery;
import com.dell.doradus.service.spider.SpiderService;

public class BuilderAnd extends SearchBuilder {
    
	@Override public FilteredIterable search(Query query) {
		AndQuery qu = (AndQuery)query;
        if(qu.subqueries.size() == 0) return create(NoneIterable.instance, null);
        List<Integer> shards = getShards(qu);
		if(m_params.l2r) {
	        FilteredIterable seq = m_searcher.search(m_params, m_table, qu.subqueries.get(0), shards);
	        FilterAnd filter = new FilterAnd();
	        filter.add(seq.filter());
	        for(int i = 1; i < qu.subqueries.size(); i++) {
	            Filter f= m_searcher.filter(m_params, m_table, qu.subqueries.get(i));
	            filter.add(f);
	        }
	        return create(seq.sequence(), filter);
		} else {
			AndIterable iter = new AndIterable(qu.subqueries.size());
			FilterAnd filter = new FilterAnd();
			for(Query q: qu.subqueries) {
		        FilteredIterable seq = m_searcher.search(m_params, m_table, q, shards);
		        iter.add(seq.sequence());
		        filter.add(seq.filter());
			}
			return create(iter, filter);
		}
	}

	@Override public Filter filter(Query query) {
		AndQuery qu = (AndQuery)query;
		FilterAnd filter = new FilterAnd();
		for(Query q: qu.subqueries) {
			filter.add(m_searcher.filter(m_params, m_table, q));
		}
		return filter;
	}
   
	private List<Integer> getShards(AndQuery query) {
		if(!m_table.isSharded()) return null;
		for(Query q : query.subqueries) {
			if(!(q instanceof RangeQuery)) continue;
			List<Integer> shards = getShards((RangeQuery)q);
			if(shards != null) return shards;
		}
		return null;
	}
	
	private List<Integer> getShards(RangeQuery query) {
		if(!m_table.getShardingField().getName().equals(query.field)) return null;
		Set<Integer> shards = SpiderService.instance().getShards(m_table).keySet();
		if(shards.size() == 0) return null;
		int minShard = Integer.MIN_VALUE;
		int maxShard = Integer.MAX_VALUE;
		if(query.min != null) {
			Date date = new DateTrie().parse(query.min); 
			minShard = m_table.computeShardNumber(date);
		}
		if(query.max != null) {
			Date date = new DateTrie().parse(query.max); 
			maxShard = m_table.computeShardNumber(date);
		}
		List<Integer> result = new ArrayList<Integer>(shards.size());
		if(minShard <= 0 && maxShard >= 0) result.add(0);
		for(Integer shard : shards) {
			if(shard < minShard || shard > maxShard) continue;
			result.add(shard);
		}
		return result;
	}
	
}
