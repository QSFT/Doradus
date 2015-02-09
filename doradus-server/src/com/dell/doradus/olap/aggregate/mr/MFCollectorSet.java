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

package com.dell.doradus.olap.aggregate.mr;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.olap.collections.BdLongSet;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.search.aggregate.AggregationGroup;
import com.dell.doradus.search.aggregate.AggregationGroupItem;

public class MFCollectorSet {
    private static Logger LOG = LoggerFactory.getLogger("MFCollectorSet");
	
	public MFCollector[] collectors;
	
	public MFCollector commonPartCollector;
	
	public MFCollectorSet(CubeSearcher searcher, List<AggregationGroup> groups, boolean allowCommonPart) {
		int commonIndex = 0;
		// common parts in groups
		if(allowCommonPart) commonIndex = getCommonPart(groups);
		if(commonIndex > 0) commonPartCollector = MFCollector.create(searcher, groups.get(0), 0, commonIndex);
		collectors = new MFCollector[groups.size()];
		for(int i = 0; i < collectors.length; i++) {
			collectors[i] = MFCollector.create(searcher, groups.get(i), commonIndex, groups.get(i).items.size());
		}
	}
	
	public int size() { return collectors.length; }
	
	public void collect(long doc, BdLongSet[] values) {
		for(int i = 0; i < collectors.length; i++) {
			collectors[i].collect(doc, values[i]);
		}
	}
	

	// Support for common paths in groups
	private int getCommonPart(List<AggregationGroup> groups) {
		if(groups.size() < 2) return 0;
		
		for(int i = 0; i < groups.size(); i++) {
			if(groups.get(i).filter != null) return 0;
		}
		
		int itemsCount = groups.get(0).items.size() - 1;
		for(int i = 1; i < groups.size(); i++) {
			itemsCount = Math.min(itemsCount, groups.get(i).items.size() - 1);
		}
		if(itemsCount <= 0) return 0;
		
		int itemIndex = 0;
		for(; itemIndex < itemsCount; itemIndex++) {
			boolean eq = true;
			AggregationGroupItem item = groups.get(0).items.get(itemIndex);
			if(item.xlinkContext != null) break;
			for(int i = 1; i < groups.size(); i++) {
				AggregationGroupItem item2 = groups.get(i).items.get(itemIndex);
				if(!item.equals(item2) || item.xlinkContext != null) {
					eq = false;
					break;
				}
			}
			if(!eq) break;
		}
		if(itemIndex > 0) {
			LOG.info("Found common path for groups: " + itemIndex);
		}
		return itemIndex;
	}
	
}


