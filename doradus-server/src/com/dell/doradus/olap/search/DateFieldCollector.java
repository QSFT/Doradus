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

import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.olap.XType;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.olap.store.NumSearcherMV;
import com.dell.doradus.search.util.HeapList;

public class DateFieldCollector extends AggregationCollector {
	//private static final long HOUR_MS = 3600 * 1000;
	private NumSearcherMV m_num_searcher;
	private String m_truncate;
	private int[] m_counts;
	private int[] m_lastDocs;
	private long m_minDate;
	private long m_maxDate;
	private long m_shift;
	private long m_divisor;

	public DateFieldCollector(CubeSearcher searcher, FieldDefinition field, String truncate, String timeZone) {
		if(truncate == null) throw new IllegalArgumentException("cannot aggregate on timestamps; use TRUNCATE instead");
		m_num_searcher = searcher.getNumSearcher(field.getTableName(), field.getName());
		m_truncate = truncate.toUpperCase();
		if(timeZone != null) {
			TimeZone zone = TimeZone.getTimeZone(timeZone);
			m_shift = zone.getRawOffset();
		}
		
		if ("SECOND".equals(m_truncate)) m_divisor = 1000;
		else if ("MINUTE".equals(m_truncate)) m_divisor = 60 * 1000;
		else if ("HOUR".equals(m_truncate)) m_divisor = 3600 * 1000;
		else m_divisor = 24 * 3600 * 1000;
		
		m_minDate = (m_num_searcher.minPos() + m_shift) / m_divisor;
		m_maxDate = (m_num_searcher.max() + m_shift) / m_divisor;
		if(m_maxDate - m_minDate > 100000) {
			throw new IllegalArgumentException("Too large date range!");
		}
		int size = (int)(m_maxDate - m_minDate + 1);
		if(m_minDate > m_maxDate) size = 0;
		m_counts = new int[size];
		m_lastDocs = new int[size];
		for(int i = 0; i < m_lastDocs.length; i++) m_lastDocs[i] = -1;
		
	}
	
	@Override public void collect(int doc, int value) {
		int fcount = m_num_searcher.size(value);
		for(int index = 0; index < fcount; index++) {
			long v = m_num_searcher.get(value, index);
			if(v == 0) return;
			v = (v + m_shift) / m_divisor - m_minDate;
			int pos = (int) v;
			if(m_lastDocs[pos] == doc) return;
			m_lastDocs[pos] = doc;
			m_counts[pos]++;
		}
	}

	@Override public GroupResult getResult(int top) {
		GroupResult groupResult = new GroupResult();
		groupResult.isSortByCount = top > 0;
		if ("SECOND".equals(m_truncate) ||
			"MINUTE".equals(m_truncate) ||
			"HOUR".equals(m_truncate) ||
			"DAY".equals(m_truncate)) {
			if(!groupResult.isSortByCount) {
				for(int i = 0; i < m_counts.length; i++) {
					if(m_counts[i] == 0) continue;
					groupResult.groupsCount++;
					String v = XType.toString(new Date((i + m_minDate) * m_divisor));
					GroupCount gc = new GroupCount(v, m_counts[i]);
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
				for(NumCount nc : hs.GetValues(NumCount.class)) {
					String v = XType.toString(new Date((nc.num + m_minDate) * m_divisor));
					GroupCount gc = new GroupCount(v, nc.count);
					groupResult.groups.add(gc);
				}
			}
			return groupResult;
		} 
		else {
			GregorianCalendar cal = new GregorianCalendar(Utils.UTC_TIMEZONE);
			GroupCount next = null;
			long nextTime = -1;
			for(int i = 0; i < m_counts.length; i++) {
				if(m_counts[i] == 0) continue;
				long time = (i + m_minDate) * m_divisor;
				if(time > nextTime) {
					if(next != null) groupResult.groups.add(next);
					long startTime = -1;
					cal.setTimeInMillis(time);
					if ("WEEK".equals(m_truncate)) {
					    cal = Utils.truncateToWeek(cal);
					    startTime = cal.getTimeInMillis();
					    cal.add(Calendar.DAY_OF_MONTH, 7);
					} else if ("MONTH".equals(m_truncate)) {
						cal.set(Calendar.DAY_OF_MONTH, 1);
						startTime = cal.getTimeInMillis();
						cal.add(Calendar.MONTH, 1);
					} else if ("QUARTER".equals(m_truncate)) {
					    cal.set(Calendar.DAY_OF_MONTH, 1);
					    cal.set(Calendar.MONTH, (cal.get(Calendar.MONTH) / 3) * 3);
					    startTime = cal.getTimeInMillis();
					    cal.add(Calendar.MONTH, 3);
					} else if ("YEAR".equals(m_truncate)) {
						cal.set(Calendar.DAY_OF_YEAR, 1);
						startTime = cal.getTimeInMillis();
						cal.add(Calendar.YEAR, 1);
					}
					nextTime = cal.getTimeInMillis();
					String v = XType.toString(new Date(startTime));
					next = new GroupCount(v, m_counts[i]);
				} else next.count += m_counts[i];
			}
			if(next != null) groupResult.groups.add(next);
			groupResult.groupsCount = groupResult.groups.size();
			if(groupResult.isSortByCount) {
				Collections.sort(groupResult.groups, new Comparator<GroupCount>() {
					@Override public int compare(GroupCount x, GroupCount y) {
						return y.count - x.count; }});
				if(groupResult.groups.size() > top) {
					groupResult.groups = groupResult.groups.subList(0, top);
				}
			}
			return groupResult;
		}
		
		
		
		
	}
}

