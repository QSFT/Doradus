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
import java.util.List;
import java.util.Set;

import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.olap.aggregate.mr.MFCollector;
import com.dell.doradus.olap.aggregate.mr.MGName;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.olap.store.IdSearcher;
import com.dell.doradus.olap.xlink.XLinkGroupContext.XGroups;

public class InverseXLinkCollector extends MFCollector
{
	private XGroups groups;
	private List<Set<Long>> fieldSets = new ArrayList<Set<Long>>();
	
	public InverseXLinkCollector(CubeSearcher searcher, FieldDefinition fieldDef, XGroups groups) {
		super(searcher);
		this.groups = groups;
		//FieldDefinition inv = fieldDef.getInverseLinkDef();
		//IdSearcher ids = searcher.getIdSearcher(inv.getTableName());
		IdSearcher ids = searcher.getIdSearcher(fieldDef.getTableName());
		for(int i = 0; i < ids.size(); i++) {
			BSTR id = ids.getId(i);
			Set<Long> set = groups.groupsMap.get(id);
			fieldSets.add(set);
		}
	}
	
	@Override public void collect(long doc, Set<Long> values) {
		Set<Long> set = fieldSets.get((int)doc);
		if(set != null) values.addAll(set);
	}
	
	@Override public void collectEmptyGroups(Set<Long> values) { }
	
	@Override public MGName getField(long value) { return groups.groupNames.get((int)value); }
}
