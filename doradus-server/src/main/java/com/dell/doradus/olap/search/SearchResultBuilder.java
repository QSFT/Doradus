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

package com.dell.doradus.olap.search;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.FieldType;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.olap.store.FieldSearcher;
import com.dell.doradus.olap.store.IdSearcher;
import com.dell.doradus.olap.store.IntIterator;
import com.dell.doradus.olap.store.NumSearcherMV;
import com.dell.doradus.olap.store.ValueSearcher;
import com.dell.doradus.search.FieldSet;
import com.dell.doradus.search.SearchResult;
import com.dell.doradus.search.SearchResultList;
import com.dell.doradus.search.aggregate.SortOrder;

public class SearchResultBuilder {
	
	public static SearchResultList build(CubeSearcher searcher, Result documents, FieldSet fieldSet, int size, SortOrder[] orders) {
	    
	    if(documents.countSet() == 0) {
	        SearchResultList empty = new SearchResultList();
	        empty.documentsCount = 0;
	        return empty;
	    }
	    
		if(size == 0) size = Integer.MAX_VALUE;
		fieldSet.limit = size;
		FieldSetCreator fieldSetCreator = new FieldSetCreator(searcher, fieldSet, orders);
		FVS fvs = new FVS();
		IntIterator documents_iter = SearchResultComparer.sort(searcher, documents, orders, size);
		IntIterator iter = new IntIterator();
		fill(searcher, documents_iter, fvs, fieldSetCreator, iter);
		fvs.resolve(searcher);
		SearchResultList list = new SearchResultList();
		list.documentsCount = documents.countSet();
		for(int doc = 0; doc < documents_iter.count(); doc++) {
			int d = documents_iter.get(doc);
			SearchResult child = build(searcher, d, fvs, fieldSetCreator, iter);
			child.orders = orders;
			list.results.add(child);
		}
		Collections.sort(list.results);
		if(list.results.size() > size) {
			List<SearchResult> subList = list.results.subList(0, size);
			list.results = new ArrayList<SearchResult>(subList); 
		}
		if(list.results.size() < list.documentsCount && list.results.size() > 0) {
			list.continuation_token = list.results.get(list.results.size() - 1).id();
		}
		return list;
	}
	
	private static void fill(CubeSearcher searcher, IntIterator documents, FVS fvs, FieldSetCreator fieldSetCreator, IntIterator iter) {
		String table = fieldSetCreator.tableDef.getTableName();
		fvs.add(table, "_ID", fieldSetCreator.filter, documents, fieldSetCreator.limit);
		for(String field : fieldSetCreator.scalarFields) {
			FieldDefinition fieldDef = fieldSetCreator.tableDef.getFieldDef(field);
			if(fieldDef == null) throw new IllegalArgumentException("Unknown field: " + field);
			FieldType type = fieldDef.getType();
			if(type == FieldType.TEXT || type == FieldType.BINARY) {
				FieldSearcher field_searcher = searcher.getFieldSearcher(table, field);
				int num = 0;
				for(int doc = 0; doc < documents.count(); doc++) {
					if(num >= fieldSetCreator.limit) break;
					int d = documents.get(doc);
					if(fieldSetCreator.filter != null && !fieldSetCreator.filter.get(d)) continue;
					field_searcher.fields(d, iter);
					fvs.add(table, field, null, iter, Integer.MAX_VALUE);
					num++;
				}
			}
		}
		for(Map.Entry<String, List<FieldSetCreator>> e : fieldSetCreator.links.entrySet()) {
			String link = e.getKey();
			List<FieldSetCreator> linkedSetList = e.getValue();
			for(FieldSetCreator linkedSet: linkedSetList) {
				FieldSearcher field_searcher = searcher.getFieldSearcher(table, link);
				IntIterator iter2 = new IntIterator();
				int num = 0;
				for(int doc = 0; doc < documents.count(); doc++) {
					if(num >= fieldSetCreator.limit) break;
					int d = documents.get(doc);
					if(fieldSetCreator.filter != null && !fieldSetCreator.filter.get(d)) continue;
					field_searcher.fields(d, iter);
					fill(searcher, iter, fvs, linkedSet, iter2);
					num++;
				}
			}
		}
	}

	private static SearchResult build(CubeSearcher searcher, int document, FVS fvs, FieldSetCreator fieldSetCreator, IntIterator iter) {
		SearchResult sr = new SearchResult();
		sr.fieldSet = fieldSetCreator.fieldSet;
		String table = fieldSetCreator.tableDef.getTableName();
		sr.scalars.put("_ID", fvs.get(table, "_ID", document));
		for(String field : fieldSetCreator.scalarFields) {
			FieldDefinition fieldDef = fieldSetCreator.tableDef.getFieldDef(field);
			if(fieldDef == null) throw new IllegalArgumentException("Unknown field: " + field);
			FieldType type = fieldDef.getType();
			if(type == FieldType.TEXT || type == FieldType.BINARY) {
				FieldSearcher field_searcher = searcher.getFieldSearcher(table, field);
				field_searcher.fields(document, iter);
				if(iter.count() == 0) continue;
				String value = "";
				for(int i = 0; i < iter.count(); i++) {
					if(value.length() > 0) value += "\uFFFE";
					value += fvs.get(table, field, iter.get(i));
				}
				sr.scalars.put(field, value);
			} else if(NumSearcherMV.isNumericType(type)) {
				NumSearcherMV num_searcher = searcher.getNumSearcher(table, field);
				if(num_searcher.isNull(document)) continue;
				String value = "";
				int size = num_searcher.size(document);
				if(size == 0) continue;
				for(int i = 0; i < size; i++) {
					if(value.length() > 0) value += "\uFFFE";
					value += NumSearcherMV.format(num_searcher.get(document, i), type);
				}
				sr.scalars.put(field, value);
			} else throw new IllegalArgumentException("Invalid type: " + type + " for field " + field);
		}
		for(Map.Entry<String, List<FieldSetCreator>> e : fieldSetCreator.links.entrySet()) {
			String link = e.getKey();
			List<FieldSetCreator> linkedSetList = e.getValue();
			List<SearchResultList> childrenList = new ArrayList<SearchResultList>();
			sr.links.put(link, childrenList);
			for(FieldSetCreator linkedSet: linkedSetList) {
				FieldSearcher field_searcher = searcher.getFieldSearcher(table, link);
				IntIterator iter2 = new IntIterator();
				field_searcher.fields(document, iter);
				SearchResultList childList = new SearchResultList();
				int num = 0;
				for(int doc = 0; doc < iter.count(); doc++) {
					if(num >= linkedSet.limit) break;
					int d = iter.get(doc);
					if(linkedSet.filter != null && !linkedSet.filter.get(d)) continue;
					SearchResult child = build(searcher, iter.get(doc), fvs, linkedSet, iter2);
					childList.results.add(child);
					num++;
				}
				childrenList.add(childList);
			}
		}
		return sr;
	}
	
	static class FVS {
		public Map<String, FV> fvs = new HashMap<String, FV>();

		public String get(String table, String field, int term) {
			return fvs.get(table + "." + field).get(term);
		}
		
		public void add(String table, String field, Result filter, IntIterator terms, int size) {
			String key = table + "." + field;
			FV fv = fvs.get(key);
			if(fv == null) {
				fv = new FV(table, field);
				fvs.put(key, fv);
			}
			int num = 0;
			if(size < 0) size = Integer.MAX_VALUE;
			for(int i = 0; i < terms.count(); i++) {
				if(num >= size) break;
				int t = terms.get(i);
				if(filter != null && !filter.get(t)) continue;
				fv.add(t);
				num++;
			}
		}
		
		public void resolve(CubeSearcher searcher) {
			for(FV fv : fvs.values()) fv.resolve(searcher);
		}
		
	}
	
	static class FV {
		public String table;
		public String field;
		public Map<Integer, String> values = new HashMap<Integer, String>();
		
		public FV(String table, String field) {
			this.table = table;
			this.field = field;
		}
		
		public void add(int i) { values.put(i, null); }
		public String get(int i) { 
			String val = values.get(i);
			return val;
		}
		
		public void resolve(CubeSearcher searcher) {
			if(field == "_ID") {
				IdSearcher id_searcher = searcher.getIdSearcher(table);
				List<Integer> li = new ArrayList<Integer>();
				li.addAll(values.keySet());
				Collections.sort(li);
				for(int i : li) {
					values.put(i, id_searcher.getId(i).toString());
				}
			} else {
				ValueSearcher value_searcher = searcher.getValueSearcher(table, field);
				List<Integer> li = new ArrayList<Integer>();
				li.addAll(values.keySet());
				Collections.sort(li);
				for(int i : li) {
					values.put(i, value_searcher.getValue(i).toString());
				}
			}
		}
		
	}
}
