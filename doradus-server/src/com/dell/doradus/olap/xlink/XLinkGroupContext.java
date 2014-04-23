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

package com.dell.doradus.olap.xlink;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.olap.aggregate.mr.MFCollector;
import com.dell.doradus.olap.aggregate.mr.MGName;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.olap.search.Result;
import com.dell.doradus.olap.search.ResultBuilder;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.olap.store.FieldSearcher;
import com.dell.doradus.olap.store.IdSearcher;
import com.dell.doradus.olap.store.IntIterator;
import com.dell.doradus.olap.store.ValueSearcher;
import com.dell.doradus.search.aggregate.AggregationGroup;
import com.dell.doradus.search.aggregate.AggregationGroupItem;
import com.dell.doradus.search.query.AllQuery;
import com.dell.doradus.search.query.Query;

// class representing structures needed during the search/aggregate, if external links are present 
public class XLinkGroupContext {
	
	public static class XGroups {
		public int maxValues;
		public Map<BSTR, Set<Long>> groupsMap = new HashMap<BSTR, Set<Long>>();
		public List<MGName> groupNames = new ArrayList<MGName>();
	}
	
	public XLinkContext context;
	
	public XLinkGroupContext(XLinkContext context) {
		this.context = context;
	}

	public static boolean hasXLink(AggregationGroup group) {
		List<AggregationGroupItem> items = group.items;
		for(int i = 0; i < items.size(); i++) {
			if(items.get(i).fieldDef.isXLinkField()) return true;
		}
		return false;
	}
	
	public void setupXLinkGroup(AggregationGroup group) {
		List<AggregationGroupItem> items = group.items;
		for(int i = items.size() - 1; i >= 0; i--) {
			AggregationGroupItem item = items.get(i);
			if(item.query != null) context.setupXLinkQuery(item.tableDef, item.query);
			if(!item.fieldDef.isXLinkField()) continue;
			group.items = new ArrayList<AggregationGroupItem>();
			for(int j = i + 1; j < items.size(); j++) {
				group.items.add(items.get(j));
			}
			XGroups xgroups = new XGroups();
			if(item.fieldDef.isXLinkDirect()) setupDirect(xgroups, item.fieldDef, group, item.query);
			else setupInverse(xgroups, item.fieldDef, group, item.query);
			item.xlinkContext = xgroups;
			// restore the group
			group.items = items;
		}
		
	}
	
	private void setupDirect(XGroups xgroups, FieldDefinition fieldDef, AggregationGroup group, Query filter) {
		if(filter == null) filter = new AllQuery();
		TableDefinition invTable = fieldDef.getInverseTableDef();
		Map<MGName, Long> names = new HashMap<MGName, Long>();
		for(String xshard : context.xshards) {
			CubeSearcher searcher = context.olap.getSearcher(context.application, xshard);
			Result bvQuery = ResultBuilder.search(invTable, filter, searcher);
			MFCollector collector = group.items.size() == 0 ?
					new MFCollector.IdField(searcher, invTable) :
					MFCollector.create(searcher, group);
			IdSearcher ids = searcher.getIdSearcher(invTable.getTableName());
			int docsCount = ids.size();
			List<Set<Long>> docToFields = new ArrayList<Set<Long>>(docsCount);
			Set<Long> allFields = new HashSet<Long>();
			
			Set<Long> values = new HashSet<Long>();
			Set<Long> emptySet = new HashSet<Long>(0);
			for(int doc = 0; doc < docsCount; doc++) {
				docToFields.add(emptySet);
				if(!bvQuery.get(doc)) continue;
				collector.collect(doc, values);
				if(values.size() > 0) {
					allFields.addAll(values);
					HashSet<Long> vals = new HashSet<Long>(values.size());
					vals.addAll(values);
					docToFields.set(doc, vals);
					values.clear();
				}
			}
			
			List<Long> fieldsList = new ArrayList<Long>(allFields);
			Collections.sort(fieldsList);

			Map<Long, Long> remap = new HashMap<Long, Long>();
			
			for(Long num : fieldsList) {
				MGName fieldName = collector.getField(num);
				Long mappedNum = names.get(fieldName);
				if(mappedNum == null) {
					mappedNum = (long)names.size();
					names.put(fieldName, mappedNum);
					xgroups.groupNames.add(fieldName);
				}
				remap.put(num, mappedNum);
			}
			
			allFields = null;
			fieldsList = null;
			
			for(int i = 0; i < docsCount; i++) {
				BSTR id = ids.getId(i);
				Set<Long> orig = docToFields.get(i);
				docToFields.set(i, null);
				if(orig.size() > 0) {
					Set<Long> remapped = new HashSet<Long>(orig.size());
					for(Long num : orig) remapped.add(remap.get(num));
					xgroups.groupsMap.put(new BSTR(id), remapped);
				}
			}
		}
	}

	private void setupInverse(XGroups xgroups, FieldDefinition fieldDef, AggregationGroup group, Query filter) {
		if(filter == null) filter = new AllQuery();
		TableDefinition invTable = fieldDef.getInverseTableDef();
		FieldDefinition inv = fieldDef.getInverseLinkDef();
		Map<MGName, Long> names = new HashMap<MGName, Long>();
		for(String xshard : context.xshards) {
			CubeSearcher searcher = context.olap.getSearcher(context.application, xshard);
			Result bvQuery = ResultBuilder.search(invTable, filter, searcher);
			
			MFCollector collector = group.items.size() == 0 ?
					new MFCollector.IdField(searcher, invTable) :
					MFCollector.create(searcher, group);
			FieldSearcher fs = searcher.getFieldSearcher(inv.getTableName(), inv.getXLinkJunction());
			IntIterator iter = new IntIterator();
			int docsCount = fs.size();
			List<Set<Long>> docToFields = new ArrayList<Set<Long>>(fs.fields());
			Set<Long> allFields = new HashSet<Long>();
			Set<Long> values = new HashSet<Long>();
			for(int doc = 0; doc < docsCount; doc++) {
				if(!bvQuery.get(doc)) continue;
				collector.collect(doc, values);
				allFields.addAll(values);
				fs.fields(doc, iter);
				for(int i = 0; i < iter.count(); i++) {
					int val = iter.get(i);
					while(docToFields.size() <= val) docToFields.add(null);
					Set<Long> vals = docToFields.get(val);
					if(vals == null) {
						vals = new HashSet<Long>(values.size());
						docToFields.set(val, vals);
					}
					vals.addAll(values);
				}
				values.clear();
			}
			while(docToFields.size() < fs.fields()) docToFields.add(new HashSet<Long>(0));
			
			List<Long> fieldsList = new ArrayList<Long>(allFields);
			Collections.sort(fieldsList);
			
			Map<Long, Long> remap = new HashMap<Long, Long>();
			
			for(Long num : fieldsList) {
				MGName fieldName = collector.getField(num);
				Long mappedNum = names.get(fieldName);
				if(mappedNum == null) {
					mappedNum = (long)names.size();
					names.put(fieldName, mappedNum);
					xgroups.groupNames.add(fieldName);
				}
				remap.put(num, mappedNum);
			}
			
			allFields = null;
			fieldsList = null;
			ValueSearcher vs = searcher.getValueSearcher(invTable.getTableName(), inv.getXLinkJunction());
			for(int i = 0; i < vs.size(); i++) {
				BSTR id = vs.getValue(i);
				Set<Long> orig = docToFields.get(i);
				docToFields.set(i, null);
				if(orig != null && orig.size() > 0) {
					Set<Long> remapped = new HashSet<Long>(orig.size());
					for(Long num : orig) remapped.add(remap.get(num));
					Set<Long> currentSet = xgroups.groupsMap.get(id);
					if(currentSet == null) xgroups.groupsMap.put(new BSTR(id), remapped);
					else currentSet.addAll(remapped);
				}
			}
		}
	}
	
}
