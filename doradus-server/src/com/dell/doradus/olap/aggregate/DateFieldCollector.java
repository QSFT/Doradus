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

package com.dell.doradus.olap.aggregate;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.olap.XType;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.olap.store.NumSearcher;

public class DateFieldCollector extends EndFieldCollector {
	private NumSearcher m_searcher;
	private String m_truncate;
	private long m_minDate;
	private long m_maxDate;
	private long m_shift;
	private long m_divisor;

	public DateFieldCollector(CubeSearcher searcher, FieldDefinition field, String truncate, String timeZone) {
		if(truncate == null) throw new IllegalArgumentException("cannot aggregate on timestamps; use TRUNCATE instead");
		m_searcher = searcher.getNumSearcher(field.getTableName(), field.getName());
		m_truncate = truncate.toUpperCase();
		if(timeZone != null) {
			TimeZone zone = TimeZone.getTimeZone(timeZone);
			m_shift = zone.getRawOffset();
		}
		if ("SECOND".equals(m_truncate)) m_divisor = 1000;
		else if ("MINUTE".equals(m_truncate)) m_divisor = 60 * 1000;
		else if ("HOUR".equals(m_truncate)) m_divisor = 3600 * 1000;
		else m_divisor = 24 * 3600 * 1000;
		
		m_minDate = (m_searcher.minPos() + m_shift) / m_divisor;
		m_maxDate = (m_searcher.max() + m_shift) / m_divisor;
		if(m_maxDate - m_minDate > 100000) {
			throw new IllegalArgumentException("Too large date range!");
		}
	}

	@Override public int getSize() {
		int size = (int)(m_maxDate - m_minDate + 1);
		if(m_minDate > m_maxDate) size = 0;
		return size;
	}

	@Override public int getIndex(int doc) {
		if(m_searcher.isNull(doc)) return -1; 
		long v = m_searcher.get(doc);
		if(v == 0) return -1;
		v = (v + m_shift) / m_divisor - m_minDate;
		int pos = (int) v;
		return pos;
	}

	@Override public String getFieldName(int field) {
		return XType.toString(new Date((field + m_minDate) * m_divisor));
	}

	@Override public String getFieldId(int field, String name) {
		return name;
	}
	
	
	@Override public AggregationResult getResult() {
		AggregationResult result = super.getResult();
		if ("SECOND".equals(m_truncate) || "MINUTE".equals(m_truncate) || "HOUR".equals(m_truncate) || "DAY".equals(m_truncate)) {
			return result;
		}
		else {
			if("WEEK".equals(m_truncate)) {
				GregorianCalendar cal = new GregorianCalendar(Utils.UTC_TIMEZONE);
				for(AggregationResult.AggregationGroup group : result.groups) {
					if(group.name == null) continue;
					cal.setTime(Utils.dateFromString(group.name));
					cal = Utils.truncateToWeek(cal);
					group.name = XType.toString(cal.getTime());
					group.id = group.name;
				}
			} else if ("MONTH".equals(m_truncate)) {
				for(AggregationResult.AggregationGroup group : result.groups) {
					if(group.name == null) continue;
					group.name = group.name.substring(0, 7) + "-01 00:00:00";
					group.id = group.name;
				}
			} else if ("QUARTER".equals(m_truncate)) {
				GregorianCalendar cal = new GregorianCalendar(Utils.UTC_TIMEZONE);
				for(AggregationResult.AggregationGroup group : result.groups) {
					if(group.name == null) continue;
					group.name = group.name.substring(0, 7) + "-01 00:00:00";
					cal.setTime(Utils.dateFromString(group.name));
				    cal.set(Calendar.MONTH, (cal.get(Calendar.MONTH) / 3) * 3);
					group.name = XType.toString(cal.getTime());
					group.id = group.name;
				}
			} else if ("YEAR".equals(m_truncate)) {
				for(AggregationResult.AggregationGroup group : result.groups) {
					if(group.name == null) continue;
					group.name = group.name.substring(0, 4) + "-01-01 00:00:00";
					group.id = group.name;
				}
			} else throw new IllegalArgumentException("Unknown truncate function: " + m_truncate);

			AggregationResult newResult = new AggregationResult();
			newResult.documentsCount = result.documentsCount;
			newResult.summary = result.summary;
			AggregationResult.AggregationGroup current = null;
			for(AggregationResult.AggregationGroup group : result.groups) {
				if(current == null) current = group;
				else if(current.equals(group)) current.merge(group);
				else {
					newResult.groups.add(current);
					current = group;
				}
			}
			if(current != null) newResult.groups.add(current);
			newResult.groupsCount = newResult.groups.size();
			return newResult;
		}
	}

}

