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

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.dell.doradus.common.CommonDefs;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.FieldType;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;
import com.dell.doradus.core.ObjectID;
import com.dell.doradus.search.aggregate.AggregationGroupItem;
import com.dell.doradus.search.aggregate.SortOrder;

public class SearchResult implements Comparable<SearchResult> {
    public ObjectID id;
	public FieldSet fieldSet;
	public SortOrder order;
	public TreeMap<String, String> scalars = new TreeMap<String, String>();
	public TreeMap<String, List<SearchResultList>> links = new TreeMap<String, List<SearchResultList>>();
	
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
    
	@Override public int compareTo(SearchResult o) {
		if(order == null) {
			return id().compareTo(o.id());
		}
		else {
			if(order.items.size() != 1) throw new IllegalArgumentException("Paths are not supported in the sort order");
			AggregationGroupItem item = order.items.get(0);
			if(item.fieldDef.isLinkField()) {
				List<SearchResultList> list1 = links.get(item.fieldDef.getName());
				List<SearchResultList> list2 = o.links.get(item.fieldDef.getName());
				SearchResultList ch1 = list1 == null || list1.size() == 0 ? null : list1.get(0);
				SearchResultList ch2 = list2 == null || list2.size() == 0 ? null : list2.get(0);
				if(ch1.results.size() == 0 && ch2.results.size() == 0) return 0;
				int d = 0;
				if(ch1.results.size() == 0) d = -1;
				else if(ch2.results.size() == 0) d = 1;
				else d = ch1.results.get(0).compareTo(ch2.results.get(0));
				if(!order.ascending) d = -d;
				return d;
			}
			else {
				String s1 = scalars.get(item.fieldDef.getName());
				String s2 = o.scalars.get(item.fieldDef.getName());
				FieldType type = item.fieldDef.getType();
				if(s1 == null && s2 == null) return 0;
				int d = 0;
				if(s1 == null) d = -1;
				else if(s2 == null) d = 1;
				else if(type == FieldType.TEXT || type == FieldType.BOOLEAN || type == FieldType.TIMESTAMP) {
					d = s1.compareToIgnoreCase(s2);
				}
				else if(type == FieldType.INTEGER || type == FieldType.LONG) {
					long l1 = Long.parseLong(s1);
					long l2 = Long.parseLong(s2);
					if(l1 < l2) d = -1; else if (l1 > l2) d = 1; else d = 0;
				}
				if(!order.ascending) d = -d;
				return d;
			}
		}
	}

	@Override public String toString() {
		return toDoc().toXML(true);
	}
}


