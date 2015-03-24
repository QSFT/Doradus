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

package com.dell.doradus.search.analyzer;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;

import com.dell.doradus.common.Utils;

/**
 * Helps tokenize and search on Date trie values  
 *
 */
public class DateTrie {
	//Lower bound (inclusive) of the range query min <= x < max.
	//If the query is x < max then you should set min to be the minimal possible value that x can take.
	//It is better though not required to chose it to have 0 seconds, minutes, hours, etc., whenever possible.
	//The less the range min-max is, the less are clauses in the query, so try to make it as small as possible
	//even better to take these values from the database if possible to build the query.
	//for example, if today is 2012, query x in LastYear can be represented with min = 2012-01-01 00:00:00,
	//and max = 2013-01-01 00:00:00
	public Date min;
	//Upper bound (exclusive) of the range query min <= x < max.
	public Date max;
	
	private static TimeZone m_utc = TimeZone.getTimeZone("GMT");
	

	public DateTrie() {
		Calendar cal = GregorianCalendar.getInstance(m_utc);
		cal.set(0, 1, 1);
		min = cal.getTime();
		cal.set(9999, 1, 1);
		max = cal.getTime();
	}

	public DateTrie(Date min, Date max) {
		this.min = min;
		this.max = max;
	}
	
	public Date parse(String date) {
	    try {
	        return Utils.dateFromString(date);
	    } catch (IllegalArgumentException e) {
	        throw new IllegalArgumentException("Cannot parse date: '" + date + "'. Only the following formats are accepted: yyyy; yyy-MM; yyyy-MM-dd; yyyy-MM-dd HH; yyyy-MM-dd HH:mm; yyyy-MM-dd HH:mm:ss; yyyy-MM-dd HH:mm:ss.SSS");
	    }
	}
	
	public String format(Date date) {
	    return Utils.formatDateUTC(date);
	}
	
	public List<String> tokenize(Date date) {
		List<String> tokens = new ArrayList<String>();
		addSecond(tokens, date);
		addMinute(tokens, date);
		addHour(tokens, date);
		addDay(tokens, date);
		addMonth(tokens, date);
		addYear(tokens, date);
		return tokens;
	}
	
	public List<String> getSearchTerms() {
		List<String> terms = new ArrayList<String>();
		Calendar cal = GregorianCalendar.getInstance(m_utc);
		Calendar cal2 = GregorianCalendar.getInstance(m_utc);
		cal.setTime(min);
		
		cal2.setTime(cal.getTime());
		cal2.add(Calendar.SECOND, 1);
		while(cal.get(Calendar.SECOND) != 0 && cal2.getTime().compareTo(max) <= 0) {
			addSecond(terms, cal.getTime());
			cal.setTime(cal2.getTime());
			cal2.add(Calendar.SECOND, 1);
		}
		cal2.setTime(cal.getTime());
		cal2.add(Calendar.MINUTE, 1);
		while(cal.get(Calendar.MINUTE) != 0 && cal2.getTime().compareTo(max) <= 0) {
			addMinute(terms, cal.getTime());
			cal.setTime(cal2.getTime());
			cal2.add(Calendar.MINUTE, 1);
		}
		cal2.setTime(cal.getTime());
		cal2.add(Calendar.HOUR_OF_DAY, 1);
		while(cal.get(Calendar.HOUR_OF_DAY) != 0 && cal2.getTime().compareTo(max) <= 0) {
			addHour(terms, cal.getTime());
			cal.setTime(cal2.getTime());
			cal2.add(Calendar.HOUR_OF_DAY, 1);
		}
		cal2.setTime(cal.getTime());
		cal2.add(Calendar.DAY_OF_MONTH, 1);
		while(cal.get(Calendar.DAY_OF_MONTH) != 1 && cal2.getTime().compareTo(max) <= 0) {
			addDay(terms, cal.getTime());
			cal.setTime(cal2.getTime());
			cal2.add(Calendar.DAY_OF_MONTH, 1);
		}
		cal2.setTime(cal.getTime());
		cal2.add(Calendar.MONTH, 1);
		while(cal.get(Calendar.MONTH) != 0 && cal2.getTime().compareTo(max) <= 0) {
			addMonth(terms, cal.getTime());
			cal.setTime(cal2.getTime());
			cal2.add(Calendar.MONTH, 1);
		}
		cal2.setTime(cal.getTime());
		cal2.add(Calendar.YEAR, 1);
		while(cal2.getTime().compareTo(max) <= 0) {
			addYear(terms, cal.getTime());
			cal.setTime(cal2.getTime());
			cal2.add(Calendar.YEAR, 1);
		}
		cal2.setTime(cal.getTime());
		cal2.add(Calendar.MONTH, 1);
		while(cal2.getTime().compareTo(max) <= 0) {
			addMonth(terms, cal.getTime());
			cal.setTime(cal2.getTime());
			cal2.add(Calendar.MONTH, 1);
		}
		cal2.setTime(cal.getTime());
		cal2.add(Calendar.DAY_OF_MONTH, 1);
		while(cal2.getTime().compareTo(max) <= 0) {
			addDay(terms, cal.getTime());
			cal.setTime(cal2.getTime());
			cal2.add(Calendar.DAY_OF_MONTH, 1);
		}
		cal2.setTime(cal.getTime());
		cal2.add(Calendar.HOUR_OF_DAY, 1);
		while(cal2.getTime().compareTo(max) <= 0) {
			addHour(terms, cal.getTime());
			cal.setTime(cal2.getTime());
			cal2.add(Calendar.HOUR_OF_DAY, 1);
		}
		cal2.setTime(cal.getTime());
		cal2.add(Calendar.MINUTE, 1);
		while(cal2.getTime().compareTo(max) <= 0) {
			addMinute(terms, cal.getTime());
			cal.setTime(cal2.getTime());
			cal2.add(Calendar.MINUTE, 1);
		}
		cal2.setTime(cal.getTime());
		cal2.add(Calendar.SECOND, 1);
		while(cal2.getTime().compareTo(max) <= 0) {
			addSecond(terms, cal.getTime());
			cal.setTime(cal2.getTime());
			cal2.add(Calendar.SECOND, 1);
		}
		
		return terms;
	}
	
	
	private void addSecond(List<String> terms, Date d) {
	    terms.add(Utils.formatDateUTC(d, Calendar.SECOND));
	}
	private void addMinute(List<String> terms, Date d) {
		terms.add("minute/" + Utils.formatDateUTC(d, Calendar.MINUTE));
	}
	private void addHour(List<String> terms, Date d) {
		terms.add("hour/" + Utils.formatDateUTC(d, Calendar.HOUR));
	}
	private void addDay(List<String> terms, Date d) {
		terms.add("day/" + Utils.formatDateUTC(d, Calendar.DATE));
	}
	private void addMonth(List<String> terms, Date d) {
		terms.add("month/" + Utils.formatDateUTC(d, Calendar.MONTH));
	}
	private void addYear(List<String> terms, Date d) {
		terms.add("year/" + Utils.formatDateUTC(d, Calendar.YEAR));
	}
}
