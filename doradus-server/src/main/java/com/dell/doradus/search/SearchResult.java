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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.dell.doradus.common.CommonDefs;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.FieldType;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;
import com.dell.doradus.core.ObjectID;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.search.aggregate.AggregationGroupItem;
import com.dell.doradus.search.aggregate.SortOrder;

public class SearchResult implements Comparable<SearchResult> {
    public ObjectID id;
	public FieldSet fieldSet;
	public SortOrder[] orders;
	public TreeMap<String, String> scalars = new TreeMap<String, String>();
	public TreeMap<String, List<SearchResultList>> links = new TreeMap<String, List<SearchResultList>>();
	private List<List<BSTR>> sortKeys = null;
	public boolean hideID;
	
    public SearchResult() { }

	public String id() {
		String ret = (id != null) ? id.toString() : scalars.get(CommonDefs.ID_FIELD); 
		return ret;
	}
	
    public UNode toDoc() {
        UNode docNode = UNode.createMapNode("doc");
        for(Map.Entry<String, String> scalar : scalars.entrySet()) {
            FieldDefinition fieldDef = fieldSet.tableDef.getFieldDef(scalar.getKey());
            if (fieldDef != null && fieldDef.isCollection()) {
                UNode fieldNode = docNode.addArrayNode(scalar.getKey(), "field");
                for (String value : Utils.splitSorted(scalar.getValue(), CommonDefs.MV_SCALAR_SEP_CHAR)) {
                    fieldNode.addValueNode("value", value);
                }
            } else {
                if(hideID && CommonDefs.ID_FIELD.equals(scalar.getKey())) continue;
                docNode.addValueNode(scalar.getKey(), scalar.getValue(), "field");
            }
        }
        for(Map.Entry<String, List<SearchResultList>> link : links.entrySet()) {
        	List<SearchResultList> linkList = link.getValue();
        	List<FieldSet> fsList = fieldSet.getLinks(link.getKey());
        	for(int i = 0; i < linkList.size(); i++) {
        		SearchResultList l = linkList.get(i);
        		FieldSet fs = fsList.get(i);
        		String linkKey = link.getKey();
        		if(fs.filter != null) {
        			linkKey += ".WHERE(" + fs.filter + ")";
        		}
        		if(fs.alias != null) {
        			linkKey = fs.alias;
        		}
	            UNode linkNode = docNode.addArrayNode(linkKey, "field");
	            if(l.results.size() > 0) {
	                for (SearchResult sr : l.results) {
	                    linkNode.addChildNode(sr.toDoc());
	                }
	            }
        	}
        }
        return docNode;
    }

	private void loadSortKeys() {
		// cache sort key so we won't reload it each time
		if(sortKeys != null) return;
		if(orders == null || orders.length == 0) return;
		sortKeys = new ArrayList<List<BSTR>>(orders.length);
		for(SortOrder order: orders) {
			Set<BSTR> sortValues = new HashSet<BSTR>();
			loadSortKey(order.items, 0, sortValues);
			List<BSTR> sortArray = new ArrayList<BSTR>(sortValues);
			Collections.sort(sortArray);
			sortKeys.add(sortArray);
		}
	}
    
	private void loadSortKey(List<AggregationGroupItem> items, int index, Set<BSTR> sortedSet) {
		AggregationGroupItem item = items.get(index);
		
		if(index == items.size() - 1) {
			if(item.fieldDef.isLinkField()) {
				List<SearchResultList> list = links.get(item.fieldDef.getName());
				SearchResultList ch = list == null || list.size() == 0 ? null : list.get(0);
				if(list.size() > 1) {
					String queryStr = item.query == null ? "" : item.query.toString();
					for(SearchResultList filtered: list) {
						String filteredStr = filtered.fieldSet.filter == null ? "" : filtered.fieldSet.filter.toString();
						if(queryStr.equals(filteredStr)) {
							ch = filtered;
							break;
						}
					}
				}
				if(ch != null) {
					for(SearchResult sr: ch.results) sortedSet.add(new BSTR(sr.id()));
				}
			}
			else {
				String s = scalars.get(item.fieldDef.getName());
				List<String> values = new ArrayList<String>();
				if(item.fieldDef.isCollection()) {
					values.addAll(Utils.split(s, CommonDefs.MV_SCALAR_SEP_CHAR));
				} else values.add(s);
				FieldType type = item.fieldDef.getType();
				if(type == FieldType.INTEGER || type == FieldType.LONG) {
					for(String str: values) {
						if(str == null) sortedSet.add(new BSTR());
						else {
							long l = Long.parseLong(str);
							sortedSet.add(new BSTR(l));
						}
					}
				}
				else if(type == FieldType.DOUBLE || type == FieldType.FLOAT) {
					for(String str: values) {
						if(str == null) sortedSet.add(new BSTR());
						else {
							double d = Double.parseDouble(str);
							//long l = Double.doubleToRawLongBits(d);
							sortedSet.add(new BSTR(d));
						}
					}
				}
				else {
					for(String str: values) {
						sortedSet.add(new BSTR(str == null ? "" : str.toLowerCase()));
					}
				}
			}
		} else {
			Utils.require(item.fieldDef.isLinkField(), "in sort order " + item.fieldDef.getName() + " should be a link field");
			List<SearchResultList> list = links.get(item.fieldDef.getName());
			SearchResultList ch = list == null || list.size() == 0 ? null : list.get(0);
			if(list.size() > 1) {
				String queryStr = item.query == null ? "" : item.query.toString();
				for(SearchResultList filtered: list) {
					String filteredStr = filtered.fieldSet.filter == null ? "" : filtered.fieldSet.filter.toString();
					if(queryStr.equals(filteredStr)) {
						ch = filtered;
						break;
					}
				}
			}
			if(ch != null) {
				for(SearchResult sr: ch.results) { sr.loadSortKey(items, index + 1, sortedSet); }
			}
		}
	}
    
	@Override public int compareTo(SearchResult o) {
		if(orders == null) {
			return id().compareTo(o.id());
		}
		
		loadSortKeys();
		o.loadSortKeys();
		
		for(int s = 0; s < sortKeys.size(); s++) {
			List<BSTR> x = sortKeys.get(s);
			List<BSTR> y = o.sortKeys.get(s);
			SortOrder order = orders[s];
			int minK = Math.min(x.size(), y.size());
			int c = 0;
			if(order == null || order.ascending) {
				for(int i = 0; i < minK; i++) {
					c = x.get(i).compareTo(y.get(i));
					if(c != 0) return c;
				}
				c = Integer.compare(x.size(), y.size());
				if(c != 0) return c;
			} else {
				for(int i = minK - 1; i >= 0; i--) {
					c = x.get(i).compareTo(y.get(i));
					if(c != 0) return -c;
				}
				c = Integer.compare(x.size(), y.size());
				if(c != 0) return -c;
			}
		}
		
		return id().compareTo(o.id());
	}

	@Override public String toString() {
		return toDoc().toXML(true);
	}
	
}


