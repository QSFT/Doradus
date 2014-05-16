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
import java.util.GregorianCalendar;

import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.olap.store.NumSearcherMV;
import com.dell.doradus.search.aggregate.AggregationGroup.SubField;

public class DateSubfieldCollector extends EndFieldCollector {
	private NumSearcherMV m_searcher;
	private SubField m_subfield;
	private Calendar m_calendar;
	private int m_calendarField;
	private int m_size;
	private String[] m_months = new String[] { "January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"};

	public DateSubfieldCollector(CubeSearcher searcher, FieldDefinition field, SubField subfield) {
		m_searcher = searcher.getNumSearcher(field.getTableName(), field.getName());
		m_subfield = subfield;
		m_calendar = new GregorianCalendar(Utils.UTC_TIMEZONE);
		
		switch(subfield) {
			case MINUTE: m_calendarField = Calendar.MINUTE; m_size = 60; break;
			case HOUR: m_calendarField = Calendar.HOUR_OF_DAY; m_size = 24; break;
			case DAY: m_calendarField = Calendar.DAY_OF_MONTH; m_size = 32; break;
			case MONTH: m_calendarField = Calendar.MONTH; m_size = 12; break;
			case YEAR: m_calendarField = Calendar.YEAR; m_size = 10000; break;
		default: Utils.require(false, "Undefined subfield: " + subfield);
		}
	}

	@Override public int getSize() { return m_size; }

	@Override public int getIndex(int doc) {
		if(m_searcher.isNull(doc)) return -1; 
		long v = m_searcher.get(doc, 0);
		if(v == 0) return -1;
		m_calendar.setTimeInMillis(v);
		int pos = m_calendar.get(m_calendarField);
		return pos;
	}

	@Override public String getFieldName(int field) {
		if(m_subfield == SubField.MONTH) return m_months[field];
		else return "" + field;
	}

	@Override public Long getFieldId(int field, String name) {
		return new Long(field);
	}

}

