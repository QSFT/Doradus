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

package com.dell.doradus.search.parser;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Hashtable;
import java.util.TimeZone;

public class TimeUtils {

    final static long ONE_SECOND = 1000;
    final static long ONE_MINUTE = ONE_SECOND * 60;
    final static long ONE_HOUR = ONE_MINUTE * 60;
    final static long ONE_DAY = ONE_HOUR * 24;
    final static long ONE_WEEK = ONE_DAY * 7;
    final static long ONE_MONTH = ONE_DAY * 30;
    final static long ONE_YEAR = ONE_DAY * 365;
    final static long ONE_QUARTER = ONE_MONTH * 3;
    static Hashtable<String, Integer> functions = new Hashtable<>();
    static Hashtable<String, String> timeZoneShortNames = new Hashtable<>();
    static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    static TimeZone utc = TimeZone.getTimeZone("UTC");
    static Calendar utcCalendar = Calendar.getInstance(utc);

    public static Calendar getCalendarByName(String timeZone) {
        timeZone = timeZone.trim();
        TimeZone zone = TimeZone.getTimeZone(timeZone);
        String id = zone.getID();
        if (id.compareTo(timeZone) != 0) {
            throw new IllegalArgumentException("Bad timezone name: '" + timeZone + "'");
        }
        return Calendar.getInstance(zone);
    }

    public static TimeZone getTimeZone(String timeZone) {
        timeZone = timeZone.trim();
        TimeZone zone = TimeZone.getTimeZone(timeZone);
        String id = zone.getID();
        if (id.compareTo(timeZone) != 0) {
            throw new IllegalArgumentException("Bad timezone name: '" + timeZone + "'");
        }
        return zone;
    }

    public static Calendar getCalendarByValue(String timeZone) {

        timeZone = timeZone.trim();
        char ch = timeZone.charAt(0);
        if (ch == '+' || ch == '-') {
            timeZone = "GMT" + timeZone;
        } else if (Character.getType(ch) == Character.DECIMAL_DIGIT_NUMBER) {
            timeZone = "GMT+" + timeZone;
        }

        String prefix = timeZone.substring(0, 4);
        String val = timeZone.substring(4);
        switch (val.length()) {
            case 1:
                timeZone = prefix + "0" + val + ":00";
                break;
            case 2:
                timeZone = prefix + val + ":00";
                break;
            case 3:
                timeZone = prefix + "0" + val.substring(0, 1) + ":" + val.substring(1);
                break;
            case 4:
                if (val.charAt(1) == ':')
                    timeZone = prefix + "0" + val;
                else {
                    timeZone = prefix + val.substring(0, 2) + ":" + val.substring(2);
                }
                break;
            case 5:
                break;
            default:
                throw new IllegalArgumentException("Internal error: bad timezone(1)");

        }
        return getCalendarByName(timeZone);
    }

    public static Calendar getNowValue(Calendar calendar, String units, Integer number) {

        Calendar c1 = (Calendar) calendar.clone();

        if (units != null) {
            switch (functions.get(units)) {
                case 20:  // minute
                    c1.add(Calendar.MINUTE, number);
                    break;
                case 21:  //hour
                    c1.add(Calendar.HOUR_OF_DAY, number);
                    break;
                case 22:  //day
                    c1.add(Calendar.DAY_OF_YEAR, number);
                    break;
                case 23:  //week
                    c1.add(Calendar.WEEK_OF_YEAR, number);
                    break;
                case 24:  //month
                    c1.add(Calendar.MONTH, number);
                    break;
                case 25:  // year
                    c1.add(Calendar.YEAR, number);
                    break;
                default:
                    throw new IllegalArgumentException("Internal error: Unknown units for period definition:" + units);
            }
        }
        return c1;
    }

    public static Calendar getPeriodStart(Calendar calendar, String units, String value) {

        int number = 1;
        if (value != null)
            try {
                number = Integer.parseInt(value);
                if (number < 1)
                    throw new IllegalArgumentException("Period.Last* value must be greater than 0: " + value);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Bad format for Period.Last* value: " + value);
            }

        Calendar c1 = (Calendar) calendar.clone();

        switch (functions.get(units)) {
            case 1:  //This minute
                c1.set(Calendar.SECOND, 0);
                c1.set(Calendar.MILLISECOND, 0);
                break;
            case 2:  //This hour
                c1.set(Calendar.MINUTE, 0);
                c1.set(Calendar.SECOND, 0);
                c1.set(Calendar.MILLISECOND, 0);
                break;
            case 3:  //today
                c1.clear(Calendar.AM_PM);
                c1.set(Calendar.HOUR_OF_DAY, 0);
                c1.set(Calendar.MINUTE, 0);
                c1.set(Calendar.SECOND, 0);
                c1.set(Calendar.MILLISECOND, 0);
                break;
            case 4:  //this week
                c1.set(Calendar.DAY_OF_WEEK, c1.getFirstDayOfWeek());
                c1.clear(Calendar.AM_PM);
                c1.set(Calendar.HOUR_OF_DAY, 0);
                c1.set(Calendar.MINUTE, 0);
                c1.set(Calendar.SECOND, 0);
                c1.set(Calendar.MILLISECOND, 0);
                break;
            case 5:  //this month
                c1.set(Calendar.DAY_OF_MONTH, 1);
                c1.clear(Calendar.AM_PM);
                c1.set(Calendar.HOUR_OF_DAY, 0);
                c1.set(Calendar.MINUTE, 0);
                c1.set(Calendar.SECOND, 0);
                c1.set(Calendar.MILLISECOND, 0);
                break;
            case 6:  //this year
                c1.clear(Calendar.AM_PM);
                c1.set(Calendar.HOUR_OF_DAY, 0);
                c1.set(Calendar.MINUTE, 0);
                c1.set(Calendar.SECOND, 0);
                c1.set(Calendar.MILLISECOND, 0);
                c1.set(Calendar.DAY_OF_YEAR, 1);
                break;

            case 10:  //last minute
                c1.add(Calendar.MINUTE, -1 * number);
                break;
            case 11:  //last hour
                c1.add(Calendar.HOUR_OF_DAY, -1 * number);
                break;
            case 12:  //last day
                c1.add(Calendar.HOUR, -24 * number);
                break;
            case 13:  //last week
                c1.add(Calendar.DAY_OF_YEAR, -7 * number);
                break;
            case 14:  //last month
                c1.add(Calendar.MONTH, -1 * number);
                break;
            case 15:  //last year
                c1.add(Calendar.YEAR, -1 * number);
                break;
            default:
                throw new IllegalArgumentException("Internal error: Unknown units for period definition:" + units);
        }

        return c1;
    }

    public static Calendar getPeriodEnd(Calendar calendar, String units) {

        Calendar c1 = (Calendar) calendar.clone();

        switch (functions.get(units)) {
            case 1:  //This minute
                c1.set(Calendar.SECOND, 0);
                c1.set(Calendar.MILLISECOND, 0);
                c1.add(Calendar.MINUTE, 1);

                break;
            case 2:  //This hour
                c1.set(Calendar.MINUTE, 0);
                c1.set(Calendar.SECOND, 0);
                c1.set(Calendar.MILLISECOND, 0);
                c1.add(Calendar.HOUR_OF_DAY, 1);
                break;
            case 3:  //today
                c1.clear(Calendar.AM_PM);
                c1.set(Calendar.HOUR_OF_DAY, 0);
                c1.set(Calendar.MINUTE, 0);
                c1.set(Calendar.SECOND, 0);
                c1.set(Calendar.MILLISECOND, 0);
                c1.add(Calendar.DAY_OF_YEAR, 1);
                break;
            case 4:  //this week
                c1.set(Calendar.DAY_OF_WEEK, c1.getFirstDayOfWeek());
                c1.clear(Calendar.AM_PM);
                c1.set(Calendar.HOUR_OF_DAY, 0);
                c1.set(Calendar.MINUTE, 0);
                c1.set(Calendar.SECOND, 0);
                c1.set(Calendar.MILLISECOND, 0);
                c1.add(Calendar.WEEK_OF_YEAR, 1);
                break;
            case 5:  //this month
                c1.set(Calendar.DAY_OF_MONTH, 1);
                c1.clear(Calendar.AM_PM);
                c1.set(Calendar.HOUR_OF_DAY, 0);
                c1.set(Calendar.MINUTE, 0);
                c1.set(Calendar.SECOND, 0);
                c1.set(Calendar.MILLISECOND, 0);
                c1.add(Calendar.MONTH, 1);
                break;
            case 6:  //this year
                c1.clear(Calendar.AM_PM);
                c1.set(Calendar.HOUR_OF_DAY, 0);
                c1.set(Calendar.MINUTE, 0);
                c1.set(Calendar.SECOND, 0);
                c1.set(Calendar.MILLISECOND, 0);
                c1.set(Calendar.DAY_OF_YEAR, 1);
                c1.add(Calendar.YEAR, 1);
                break;
            default:
                break;
        }
        return c1;
    }

    public static boolean isThisUnit(String units) {
        switch (functions.get(units)) {
            case 1:  //This minute
            case 2:  //This hour
            case 3:  //today
            case 4:  //this week
            case 5:  //this month
            case 6:  //this year
                return true;
            default:
                return false;
        }
    }

    public static String toUtcTime(Calendar calendar) {
        utcCalendar.setTimeInMillis(calendar.getTimeInMillis());
        //format.setCalendar(utcCalendar);
        return format.format(utcCalendar.getTime());
    }

    public static long getTimeDifference(String unit, Calendar start, Calendar end) {

        long diff =end.getTimeInMillis() - start.getTimeInMillis() ;
        switch (functions.get(unit)) {
            case 20:  // minute
                diff = diff / ONE_MINUTE;
                break;
            case 21:  //hour
                diff = diff / ONE_HOUR;
                break;
            case 22:  //day
                diff = diff / ONE_DAY;
                break;
            case 23:  //week
                diff = diff / ONE_WEEK;
                break;
            case 24:  //month
                diff = end.get(Calendar.YEAR) - start.get(Calendar.YEAR);
                diff = diff * 12 + end.get(Calendar.MONTH) - start.get(Calendar.MONTH);
                break;
            case 25:  //year
                diff = end.get(Calendar.YEAR) - start.get(Calendar.YEAR);
                break;
            case 26:  //quarter
                diff = end.get(Calendar.YEAR) - start.get(Calendar.YEAR);
                diff = diff * 12 + end.get(Calendar.MONTH) - start.get(Calendar.MONTH);
                diff = diff/3;
                break;
            case 27:  //second
                diff = diff / ONE_SECOND;
                break;
            default:
                throw new IllegalArgumentException("Unsupported date time unit:" + unit);
        }

        return diff;
    }

    static {
        format.setTimeZone(utc);
        functions.put(SemanticNames.ThisMinute, 1);
        functions.put(SemanticNames.ThisHour, 2);
        functions.put(SemanticNames.Today, 3);
        functions.put(SemanticNames.ThisWeek, 4);
        functions.put(SemanticNames.ThisMonth, 5);
        functions.put(SemanticNames.ThisYear, 6);

        functions.put(SemanticNames.LastMinute, 10);
        functions.put(SemanticNames.LastHour, 11);
        functions.put(SemanticNames.LastDay, 12);
        functions.put(SemanticNames.LastWeek, 13);
        functions.put(SemanticNames.LastMonth, 14);
        functions.put(SemanticNames.LastYear, 15);

        functions.put(SemanticNames.Minute, 20);
        functions.put(SemanticNames.Hour, 21);
        functions.put(SemanticNames.Day, 22);
        functions.put(SemanticNames.Week, 23);
        functions.put(SemanticNames.Month, 24);
        functions.put(SemanticNames.Year, 25);

        functions.put(SemanticNames.Minutes, 20);
        functions.put(SemanticNames.Hours, 21);
        functions.put(SemanticNames.Days, 22);
        functions.put(SemanticNames.Weeks, 23);
        functions.put(SemanticNames.Months, 24);
        functions.put(SemanticNames.Years, 25);

        functions.put(SemanticNames.Quarter, 26);
        functions.put(SemanticNames.Second, 27);

        //TODO : different timezones have the same shortName - how to select zone by shortName?
        /*
        String[] zoneIds = TimeZone.getAvailableIDs();
        Date date = new Date();

        for(String zoneId:zoneIds)
        {
            TimeZone timeZone = TimeZone.getTimeZone(zoneId) ;
            boolean daylight = timeZone.inDaylightTime(date);
            String shortName = timeZone.getDisplayName(daylight, TimeZone.SHORT, Locale.US);
            String shortName1 = timeZone.getDisplayName(!daylight, TimeZone.SHORT, Locale.US);
            String longName = timeZone.getDisplayName(daylight, TimeZone.LONG, Locale.US);
            timeZoneShortNames.put(shortName, zoneId);

        }
        */

    }

}
