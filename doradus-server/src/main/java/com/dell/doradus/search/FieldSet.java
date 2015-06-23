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

package com.dell.doradus.search;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.dell.doradus.common.CommonDefs;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.search.query.Query;

public class FieldSet {
	/**
	 * limits the number of 
	 */
	public int limit = -1;
	public String alias = null;
	public Query filter = null;
	public TableDefinition tableDef;
	public ArrayList<String> ScalarFields = new ArrayList<String>();
	public Map<String, String> ScalarFieldAliases = new HashMap<String, String>();
	private TreeMap<String, List<FieldSet>> LinkFields = new TreeMap<String, List<FieldSet>>();
	
	public FieldSet(TableDefinition tableDef) {
		this.tableDef = tableDef;
	}
	
	public FieldSet(TableDefinition tableDef, String text) {
		text = tableDef.replaceAliaces(text);
		this.tableDef = tableDef;
		Set(text);
		Fixup();
		mergeLinks();
	}
	
	public void addLink(String linkName, FieldSet fieldSet) {
		List<FieldSet> fieldSetList = LinkFields.get(linkName);
		if(fieldSetList == null) {
			fieldSetList = new ArrayList<FieldSet>();
			LinkFields.put(linkName, fieldSetList);
		}
		fieldSetList.add(fieldSet);
	}
	
	public Collection<String> getLinks() { return LinkFields.keySet(); }
	
	public List<FieldSet> getLinks(String linkName) {
		return LinkFields.get(linkName);
	}
	
	public boolean IsOnlyID() {
		return LinkFields.size() == 0 && ScalarFields.size() == 1 && ScalarFields.get(0).equals(CommonDefs.ID_FIELD);
	}
	
	public void Set(String fields)
	{
		if(fields == null || fields.equals("") || fields.equals("*")) {
			ScalarFields.add("*");
			return;
		}
        FieldSetTokenizer tok = new FieldSetTokenizer(this, fields);
		tok.ProcessField();
	}
	
	public void Fixup() {
		if(ScalarFields.contains("*") && ScalarFields.size() > 1) {
			ScalarFields.clear();
			ScalarFields.add("*");
		}
		if(ScalarFields.contains("_all")) {
			ScalarFields.clear();
			ScalarFields.add("*");
			LinkFields.clear();
		    for(FieldDefinition fieldDef: tableDef.getFieldDefinitions()) {
		        if(!fieldDef.isLinkField()) continue;
		        addLink(fieldDef.getName(), new FieldSet(tableDef.getLinkExtentTableDef(fieldDef), "*"));
		    }
		}

        //NT 2012-08-06: return _ID values for all links
        if(ScalarFields.contains("_local")) {
            ScalarFields.clear();
            ScalarFields.add("*");
            LinkFields.clear();
            for(FieldDefinition fieldDef: tableDef.getFieldDefinitions()) {
                if(!fieldDef.isLinkField()) continue;
                addLink(fieldDef.getName(), new FieldSet(tableDef.getLinkExtentTableDef(fieldDef), CommonDefs.ID_FIELD));
            }
        }
        
		for(List<FieldSet> linkSetList: LinkFields.values()) {
			for(FieldSet linkSet: linkSetList) {
				linkSet.Fixup();
			}
		}

        for(String linkName: LinkFields.keySet()) {
            FieldDefinition linkField = tableDef.getFieldDef(linkName);
            if(!linkField.isXLinkField()) continue;
            String junction = linkField.getXLinkJunction();
            if(junction == null || "_ID".equals(junction)) continue;
            ScalarFields.add(junction);
        }
		
		ScalarFields.remove("_ID");
	}
	
	public void expand() {
		if(ScalarFields.contains("*")) {
			ScalarFields.clear();
			for(FieldDefinition fd : tableDef.getFieldDefinitions()) {
				if(!fd.isScalarField()) continue;
				ScalarFields.add(fd.getName());
			}
		}
		for(List<FieldSet> linkSetList: LinkFields.values()) {
			for(FieldSet linkSet: linkSetList) {
				linkSet.expand();
			}
		}
	}
	
	private void mergeLinks() {
		List<String> allLinks = new ArrayList<String>(getLinks());
		for(String linkName: allLinks) {
			List<FieldSet> linkSet = getLinks(linkName);
			if(linkSet.size() <= 1) continue;
			List<FieldSet> newLinkSet = new ArrayList<FieldSet>();
			for(FieldSet link: linkSet) {
				int curSize = newLinkSet.size();
				boolean merged = false;
				for(int i = 0; i < curSize; i++) {
					FieldSet existingLink = newLinkSet.get(i);
					String filter1 = link.filter == null ? null : link.filter.toString();
					String filter2 = existingLink.filter == null ? null : existingLink.filter.toString();
					if((filter1 == null && filter2 == null) || (filter1 != null && filter1.equals(filter2))) {
						existingLink.merge(link);
						merged = true;
						break;
					}
				}
				if(!merged) {
					link.mergeLinks();
					newLinkSet.add(link);
				}
			}
			LinkFields.put(linkName, newLinkSet);
 		}
		
	}
	
	private void merge(FieldSet other) {
		Utils.require(limit == -1 || other.limit == -1 || limit == other.limit, "Inconsistent field sizes");
		if(limit == -1) limit = other.limit;
		ScalarFields.addAll(other.ScalarFields);
		for(String link: other.getLinks()) {
			for(FieldSet linkSet: other.getLinks(link)) {
				addLink(link, linkSet);
			}
 		}
		mergeLinks();
	}
}
