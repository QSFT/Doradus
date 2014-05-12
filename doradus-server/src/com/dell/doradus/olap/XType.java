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

package com.dell.doradus.olap;

import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import com.dell.doradus.common.Utils;

public class XType {
	public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"; // e.g., 2010-07-15 21:32:01
	public static final String DATE_FORMAT_MS = "yyyy-MM-dd HH:mm:ss.SSS"; // e.g., 2010-07-15 21:32:01.123
	public static final TimeZone GMT = TimeZone.getTimeZone("GMT");

	public static Long getLong(String string) {
		return string == null ? null : Long.parseLong(string);
	}
	public static Integer getInt(String string) {
		return string == null ? null : Integer.parseInt(string);
	}
	public static Boolean getBoolean(String string) {
		return string == null ? null : "true".equalsIgnoreCase(string);
	}
	public static Date getDate(String string) {
		return Utils.dateFromString(string);
	}

	public static String toString(long value) {
		return "" + value;
	}
	public static String toString(int value) {
		return "" + value;
	}
	public static String toString(boolean value) {
		return "" + value;
	}
	
	public static String toString(double value) {
		long lval = Math.round(value);
		if(Math.abs(value - lval) < 0.001) return "" + lval;
		else return new DecimalFormat("#.#########").format(value);
	}
	
	public static String toString(Date value) {
		if(value == null) throw new IllegalArgumentException("date cannot be null");
	    SimpleDateFormat format = new SimpleDateFormat(value.getTime() % 1000 == 0 ? DATE_FORMAT : DATE_FORMAT_MS);
	    format.setTimeZone(GMT);
		return format.format(value);
	}

	public static String toString(Date value, String dateFormat) {
		if(value == null) throw new IllegalArgumentException("date cannot be null");
	    SimpleDateFormat format = new SimpleDateFormat(dateFormat);
	    format.setTimeZone(GMT);
		return format.format(value);
	}
	
	public static String sizeToString(long size) {
		double s = size / 1024.0 / 1024.0;
		return String.format("%.3f MB", s);
	}
	
	public static long stringToSize(String str) {
		if(str == null) return 0;
		str = str.substring(0, str.length() - 3);
		try {
			double d = NumberFormat.getInstance(Locale.ROOT).parse(str).doubleValue();
			//Double.parseDouble(str);
			return (long)(d * 1024 * 1024);
		} catch (ParseException e) { throw new IllegalArgumentException(e.getMessage(), e); } 
	}
}
