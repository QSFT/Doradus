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

package com.dell.doradus.olap.merge;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.dell.doradus.olap.search.GroupCount;
import com.dell.doradus.olap.search.GroupResult;
import com.dell.doradus.search.FieldSet;
import com.dell.doradus.search.SearchResult;
import com.dell.doradus.search.SearchResultList;
import com.dell.doradus.search.util.HeapSort;

public class MergeResult {
	public static GroupResult mergeGroups(List<GroupResult> results, int top) {
		if(results.size() == 1) return results.get(0);
		final GroupResult groupResult = new GroupResult();
		groupResult.isSortByCount = top > 0;
		HeapSort<GroupCount> heapSort = new HeapSort<GroupCount>();
		for(GroupResult r : results) {
			groupResult.totalCount += r.totalCount;
			Collections.sort(r.groups);
			heapSort.add(r.groups);
		}

		GroupCount last = null;
		for(GroupCount c : heapSort) {
			if(last != null && last.name.equalsIgnoreCase(c.name)) last.count += c.count;
			else {
				groupResult.groups.add(c);
				last = c;
			}
		}
		
		groupResult.groupsCount = groupResult.groups.size();
		
		if(groupResult.isSortByCount) {
			Collections.sort(groupResult.groups, new Comparator<GroupCount>() {
				@Override public int compare(GroupCount x, GroupCount y) { return y.count - x.count; }
			});
		}
		
		if(top > 0 && groupResult.groups.size() >= top) {
			groupResult.groups = groupResult.groups.subList(0, top);
		}
		
		return groupResult;
	}
	
	
	public static SearchResultList merge(List<SearchResultList> results, FieldSet fieldSet) {
		if(results.size() == 1) return results.get(0);
		SearchResultList searchResultList = new SearchResultList();
		searchResultList.documentsCount = 0;
		HeapSort<SearchResult> heapSort = new HeapSort<SearchResult>();
		for(SearchResultList s : results) {
			heapSort.add(s.results);
			searchResultList.documentsCount += s.documentsCount;
		}

		SearchResult last = null;
		List<SearchResult> list = new ArrayList<SearchResult>();
		for(SearchResult s : heapSort) {
			if(last == null || last != null && last.id().equals(s.id()) ) {
				list.add(s);
				last = s;
			}
			else {
				if(searchResultList.results.size() >= fieldSet.limit) break;
				
				//As per BIM's request, turn off results merging
				//SearchResult merged = mergeResult(list, fieldSet);
				//return last entry, i.e. with the latest shard
				SearchResult merged = list.get(list.size() - 1);
				
				searchResultList.results.add(merged);
				last = s;
				list.clear();
				list.add(s);
			}
			
		}
		if(list.size() > 0 && searchResultList.results.size() < fieldSet.limit) {
			//As per BIM's request, turn off results merging
			//SearchResult merged = mergeResult(list, fieldSet);
			SearchResult merged = list.get(0);
			searchResultList.results.add(merged);
		}
		
		List<SearchResult> res = searchResultList.results;
		if(res.size() < searchResultList.documentsCount && res.size() > 0) {
			searchResultList.continuation_token = res.get(res.size() - 1).id();
		}
		
		return searchResultList;
	}
	
	/*
	private static SearchResult mergeResult(List<SearchResult> results, FieldSet fieldSet) {
		if(results.size() == 1) return results.get(0);
		SearchResult result = new SearchResult();
		result.id = results.get(0).id;
		result.fieldSet = results.get(0).fieldSet;
		//1. merge scalar fields
		Map<String, Set<String>> fields = new HashMap<String, Set<String>>();
		for(SearchResult r : results) {
			for(Map.Entry<String, String> scalar : r.scalars.entrySet()) {
				Set<String> vals = fields.get(scalar.getKey());
				if(vals == null) {
					vals = new HashSet<String>();
					fields.put(scalar.getKey(), vals);
				}
				for(String v : Utils.split(scalar.getValue(), CommonDefs.MV_SCALAR_SEP_CHAR)) {
					vals.add(v);
				}
			}
		}
		for(Map.Entry<String, Set<String>> fv : fields.entrySet()) {
			String field = fv.getKey();
			List<String> vals = new ArrayList<String>();
			vals.addAll(fv.getValue());
			Collections.sort(vals);
			String value = Utils.concatenate(vals, CommonDefs.MV_SCALAR_SEP_CHAR);
			result.scalars.put(field, value);
		}
		//2. merge links
		for(String link : result.fieldSet.LinkFields.keySet()) {
			List<SearchResultList> children = new ArrayList<SearchResultList>();
			for(SearchResult r : results) children.add(r.links.get(link));
			result.links.put(link, merge(children, fieldSet.LinkFields.get(link)));
		}
		return result;
	}
	*/
}


