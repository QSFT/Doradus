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

import java.util.ArrayList;
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
		// common parts in groups
		if(allowCommonPart) {
			AggregationGroup commonGroup = getCommonPart(groups);
			if(commonGroup != null) {
				commonPartCollector = MFCollector.create(searcher, commonGroup);
				groups = removeCommonPart(groups, commonGroup);
			}
		}
		collectors = new MFCollector[groups.size()];
		for(int i = 0; i < collectors.length; i++) {
			collectors[i] = MFCollector.create(searcher, groups.get(i));
		}
	}
	
	public int size() { return collectors.length; }
	
	public void collect(long doc, BdLongSet[] values) {
		for(int i = 0; i < collectors.length; i++) {
			collectors[i].collect(doc, values[i]);
		}
	}
	

	// Support for common paths in groups
	private AggregationGroup getCommonPart(List<AggregationGroup> groups) {
		if(groups.size() < 2) return null;
		for(int i = 0; i < groups.size(); i++) {
			if(groups.get(i).filter != null) return null;
		}
		int itemsCount = groups.get(0).items.size() - 1;
		for(int i = 1; i < groups.size(); i++) {
			itemsCount = Math.min(itemsCount, groups.get(i).items.size() - 1);
		}
		if(itemsCount <= 0) return null;
		
		List<AggregationGroupItem> list = new ArrayList<AggregationGroupItem>();
		for(int itemIndex = 0; itemIndex < itemsCount; itemIndex++) {
			AggregationGroupItem item = groups.get(0).items.get(itemIndex);
			String itemInfo = item.name;
			if(item.query != null) itemInfo += "-" + item.query.toString();
			for(int i = 1; i<groups.size(); i++) {
				AggregationGroupItem item2 = groups.get(i).items.get(itemIndex);
				String itemInfo2 = item2.name;
				if(item2.query != null) itemInfo += "-" + item2.query.toString();
				
				if(!itemInfo.equals(itemInfo2)) {
					item = null;
					break;
				}
			}
			if(item == null) break;
			list.add(item);
		}
		
		AggregationGroup group = new AggregationGroup(groups.get(0).tableDef);
		group.items = list;
		LOG.info("Found common path for groups: " + group.items.get(0).name + "..., total " + group.items.size());
		return group;
	}
	
	private List<AggregationGroup> removeCommonPart(List<AggregationGroup> groups, AggregationGroup commonPart) {
		int parts = commonPart.items.size();
		List<AggregationGroup> newgroups = new ArrayList<AggregationGroup>(groups.size());
		for(int i = 0; i < groups.size(); i++) {
			AggregationGroup group = groups.get(i);
			List<AggregationGroupItem> newitems = group.items.subList(parts, group.items.size());
			AggregationGroup newgroup = new AggregationGroup(newitems.get(0).tableDef);
			newgroup.items = newitems;
			newgroup.batch = group.batch;
			newgroup.exclude = group.exclude;
			newgroup.include = group.include;
			newgroup.name = group.name;
			newgroup.selection = group.selection;
			newgroup.selectionValue = group.selectionValue;
			newgroup.stopWords = group.stopWords;
			newgroup.subField = group.subField;
			newgroup.timeZone = group.timeZone;
			newgroup.tocase = group.tocase;
			newgroup.truncate = group.truncate;
			newgroups.add(newgroup);
		}
		return newgroups;
	}
	
}


