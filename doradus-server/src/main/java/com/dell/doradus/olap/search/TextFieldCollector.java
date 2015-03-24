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

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.olap.store.FieldSearcher;
import com.dell.doradus.olap.store.IntIterator;
import com.dell.doradus.olap.store.ValueSearcher;
import com.dell.doradus.search.util.HeapList;

public class TextFieldCollector extends AggregationCollector {
	private CubeSearcher m_searcher;
	private FieldSearcher m_field_searcher;
	private FieldDefinition m_field;
	private IntIterator m_iter;
	
	private int[] m_counts;
	private int[] m_lastDocs;

	public TextFieldCollector(CubeSearcher searcher, FieldDefinition field) {
		m_searcher = searcher;
		m_field = field;
		m_field_searcher = searcher.getFieldSearcher(field.getTableName(), field.getName());
		m_iter = new IntIterator();
		m_counts = new int[m_field_searcher.fields()];
		m_lastDocs = new int[m_field_searcher.fields()];
		for(int i = 0; i < m_lastDocs.length; i++) m_lastDocs[i] = -1;
		
	}
	
	@Override public void collect(int doc, int value) {
		m_field_searcher.fields(value, m_iter);
		for(int i=0; i<m_iter.count(); i++) {
			value = m_iter.get(i);
			if(m_lastDocs[value] == doc) return;
			m_lastDocs[value] = doc;
			m_counts[value]++;
		}
	}

	@Override public GroupResult getResult(int top) {
		GroupResult groupResult = new GroupResult();
		groupResult.isSortByCount = top > 0;
		ValueSearcher value_searcher = m_searcher.getValueSearcher(m_field.getTableName(), m_field.getName());
		if(!groupResult.isSortByCount) {
			for(int i = 0; i < m_counts.length; i++) {
				if(m_counts[i] == 0) continue;
				groupResult.groupsCount++;
				GroupCount gc = new GroupCount(value_searcher.getValue(i).toString(), m_counts[i]);
				groupResult.groups.add(gc);
			}
		}
		else {
			HeapList<NumCount> hs = new HeapList<NumCount>(top);
			for(int i = 0; i < m_counts.length; i++) {
				if(m_counts[i] == 0) continue;
				groupResult.groupsCount++;
				hs.Add(new NumCount(i, m_counts[i]));
			}
			NumCount[] counts = hs.GetValues(NumCount.class);
			Arrays.sort(counts, new Comparator<NumCount>(){
				@Override public int compare(NumCount x, NumCount y) {
					return x.num - y.num;
				}});
			for(NumCount nc : counts) {
				GroupCount gc = new GroupCount(value_searcher.getValue(nc.num).toString(), nc.count);
				groupResult.groups.add(gc);
			}
			Collections.sort(groupResult.groups, new Comparator<GroupCount>(){
				@Override public int compare(GroupCount x, GroupCount y) {
					int c = y.count - x.count;
					if(c != 0) return c;
					return x.name.compareToIgnoreCase(y.name);
				}});
		}
		return groupResult;
	}
}

