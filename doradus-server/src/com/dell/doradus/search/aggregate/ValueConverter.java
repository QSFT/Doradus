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

package com.dell.doradus.search.aggregate;

import java.lang.reflect.Method;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;

import com.dell.doradus.common.Utils;

public interface ValueConverter {
	String convert (String value);
}

/**
 * Doradus date values have the following format: "yyyy-MM-dd HH:mm:ss"*
 */
class DateConverter implements ValueConverter{

	private final static String truncatePattern = "yyyy-01-01 00:00:00";
	private int m_truncateIndex;

	DateConverter (int truncateIndex){
		m_truncateIndex = truncateIndex;
	}
	
	@Override
	public String convert(String value) {
		if (value.length() < m_truncateIndex){
			return value;
		}
		return value.substring(0,m_truncateIndex) + truncatePattern.substring(m_truncateIndex);
	}
	
	public static ValueConverter getConverter(String parameter){
		parameter = parameter.toUpperCase();
		if ("SECOND".equals(parameter)){
			return new DateConverter(19);
		}
		else if ("MINUTE".equals(parameter)){
			return new DateConverter(17);
		}
		else if ("HOUR".equals(parameter)){
			return new DateConverter(13);
		}
		else if ("DAY".equals(parameter)){
			return new DateConverter(11);
		}
		else if ("WEEK".equals(parameter)){
			return new WeekConverter();
		}
		else if ("MONTH".equals(parameter)){
			return new DateConverter(8);
		}
		else if ("QUARTER".equals(parameter)){
			return new QuarterConverter();
		}
		else if ("YEAR".equals(parameter)){
			return new DateConverter(5);
		}
		else throw new IllegalArgumentException("Illegagal TRUNCATE parameter: " + parameter);
	}	
}

class QuarterConverter implements ValueConverter{

	@Override
	public String convert(String value) {
		int month;
		try {
			month = Integer.parseInt(value.substring(5,7));
		}catch (Exception ex){
			return null;
		}
		if (month < 1 || month > 12){
			return null;
		}
		String quarter;
		if (month < 4){
			quarter = "-01-01 00:00:00";
		}else if (month < 7){
			quarter = "-04-01 00:00:00";
		}else if (month < 10){
			quarter = "-07-01 00:00:00";
		}else{
			quarter = "-10-01 00:00:00";
		}
		return value.substring(0,4) + quarter;
	}	
}

class WeekConverter implements ValueConverter{
	@Override
	public String convert(String value) {
		try {
			int year = Integer.parseInt(value.substring(0,4));
			int month = Integer.parseInt(value.substring(5,7));		
			int day = Integer.parseInt(value.substring(8,10));		
			GregorianCalendar c =  new GregorianCalendar(year,month-1,day);
			int weekday = c.get(Calendar.DAY_OF_WEEK);  
			int days = difference(weekday);
			if (days != 0){
				c.add(Calendar.DAY_OF_MONTH, days);
			}
			return String.format("%04d-%02d-%02d 00:00:00" , c.get(Calendar.YEAR),c.get(Calendar.MONTH)+1,c.get(Calendar.DAY_OF_MONTH));
		} catch (NumberFormatException e) {
			return null;
		}
	}
	
	private static int difference(int weekday){
		switch (weekday){
		case Calendar.TUESDAY: return -1;
		case Calendar.WEDNESDAY: return -2;
		case Calendar.THURSDAY: return -3;
		case Calendar.FRIDAY: return -4;
		case Calendar.SATURDAY: return -5;
		case Calendar.SUNDAY: return -6;
		default: return 0;
		}
	}
}

class TimeZoneConverter implements ValueConverter{
	final SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	final SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public TimeZoneConverter(String timeZoneID){
		inputFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		char ch = timeZoneID.charAt(0);
		if (ch == '+' || ch == '-'){
			timeZoneID =  "GMT"+timeZoneID;
		}else if (Character.getType(ch) == Character.DECIMAL_DIGIT_NUMBER){
			timeZoneID =  "GMT+"+timeZoneID;
		}		
		outputFormat.setTimeZone(TimeZone.getTimeZone(timeZoneID));
	}
	@Override
	public synchronized String convert(String value) {
		try {
			return outputFormat.format(inputFormat.parse(value));
		} catch (ParseException e) {
			return null;
		}
	}
}

abstract class BatchConverter<E extends Object> implements ValueConverter{
	List<E> m_backetValues;
	String[] m_backets;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static ValueConverter getConverter(List backetValues) {
		if (backetValues.get(0) instanceof Integer) {
			return new BatchConverter<Integer>(backetValues) {
				public Integer StringToValue(String value) {
					try {
						return Integer.parseInt(value);
					} catch (NumberFormatException ex) {
						throw new IllegalArgumentException("Invalid integer format");
					}
				}
				public int Compare(Integer value1, Integer value2) {
					return value1.compareTo(value2);
				}
			};
		} else if (backetValues.get(0) instanceof Long) {
			return new BatchConverter<Long>(backetValues) {
				public Long StringToValue(String value) {
					try {
						return Long.parseLong(value);
					} catch (NumberFormatException ex) {
						throw new IllegalArgumentException("Invalid long format");
					}
				}
				public int Compare(Long value1, Long value2) {
					return value1.compareTo(value2);
				}
			};
		} else if (backetValues.get(0) instanceof Float) {
			return new BatchConverter<Float>(backetValues) {
				public Float StringToValue(String value) {
					try {
						return Float.parseFloat(value);
					} catch (NumberFormatException ex) {
						throw new IllegalArgumentException("Invalid float format");
					}
				}
				public int Compare(Float value1, Float value2) {
					return value1.compareTo(value2);
				}
			};
		} else if (backetValues.get(0) instanceof Double) {
			return new BatchConverter<Double>(backetValues) {
				public Double StringToValue(String value) {
					try {
						return Double.parseDouble(value);
					} catch (NumberFormatException ex) {
						throw new IllegalArgumentException("Invalid double format");
					}
				}
				public int Compare(Double value1, Double value2) {
					return value1.compareTo(value2);
				}
			};
		} else if (backetValues.get(0) instanceof Date) {
			return new BatchConverter<Date>(backetValues) {
			   public String ValueToString(Date value) {
					return Utils.formatDateUTC(value.getTime());
				}
				public Date StringToValue(String value) {
					return Utils.dateFromString(value); // throw new IllegalArgumentException("Invalid date format");
				}
				public int Compare(Date value1, Date value2) {
					return value1.compareTo(value2);
				}
			};
		} else if (backetValues.get(0) instanceof String) {
			return new BatchConverter<String>(backetValues) {
				public String StringToValue(String value) {
					return value;
				}
				public int Compare(String value1, String value2) {
					return value1.compareTo(value2);
				}
			};
		} else {
			return null;
		}
	}

	BatchConverter(List<E> backetValues){
		m_backetValues = backetValues;
		m_backets = new String[m_backetValues.size() + 1];
		m_backets[0] = "<" + ValueToString(m_backetValues.get(0));
		for (int i = 0; i < m_backetValues.size() - 1; i++) {
			m_backets[i + 1] = ValueToString(m_backetValues.get(i)) + "-" + ValueToString(m_backetValues.get(i + 1));
		}
		m_backets[m_backets.length - 1] = ">=" + ValueToString(m_backetValues.get(m_backetValues.size() - 1));
	}
	
	public String ValueToString(E value) {
		return value.toString();
	}
	
	abstract public E StringToValue(String value) throws IllegalArgumentException;
	
	abstract public int Compare(E value1, E value2);

	@Override
	public String convert(String value) {
		try {
			E typeValue = StringToValue(value);
			for (int i = 0; i < m_backetValues.size(); i++) {
				if (Compare(typeValue, m_backetValues.get(i)) < 0) {
					return m_backets[i];
				}
			}
			return m_backets[m_backets.length-1];
		}catch (IllegalArgumentException ex) {
			return null;
		}
	}	
}

class CaseConverter implements ValueConverter{
	Method m_toCaseMethod;
	
	CaseConverter(Method toCaseMethod){
		m_toCaseMethod = toCaseMethod;
	}
	
	@Override
	public String convert(String value) {
		try {
			return (String)m_toCaseMethod.invoke(null, value);
		} catch (Exception e) {
			return null;
		}
	}
	
	public static String toUPPERCase(String value) {
		return value.toUpperCase();
	}
	
	public static String toLOWERCase(String value) {
		return value.toLowerCase();
	}
	
	public static ValueConverter getConverter(String parameter){
		parameter = parameter.toUpperCase();
		String methodName = String.format("to%sCase", parameter); 
		try {
			Method method = CaseConverter.class.getMethod(methodName, new Class[]{String.class});
			return new CaseConverter(method);
		} catch (NoSuchMethodException e) {
			throw new IllegalArgumentException("Illegal CASE parameter: " + parameter);
		}
	}		
}

