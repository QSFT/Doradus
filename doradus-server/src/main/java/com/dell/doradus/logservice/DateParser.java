package com.dell.doradus.logservice;

import java.util.Calendar;
import java.util.GregorianCalendar;

import com.dell.doradus.common.Utils;
import com.dell.doradus.olap.io.BSTR;

/**
 * faster than Utils.parseDate but not thread-safe
 * Date formats:
 * yyyy-MM-dd HH:mm:ss.SSS
 * yyyy-MM-dd HH:mm:ss,SSS
 * yyyy-MM-dd HH:mm:ss
 * yyyy-MM-dd HH
 * yyyy-MM-dd
 * yyyy-MM
 * yyyy
 */
public class DateParser {
    
    private static final byte ZERO = (byte)'0';
    private static final byte NINE = (byte)'9';
    private static final byte DASH = (byte)'-';
    private static final byte SPACE = (byte)' ';
    private static final byte COLON = (byte)':';
    private static final byte POINT = (byte)'.';
    private static final byte COMMA = (byte)',';
    
    private GregorianCalendar m_calendar = new GregorianCalendar(Utils.UTC_TIMEZONE);
    private int m_lastYear = 0;
    private int m_lastMonth = 0;
    private int m_lastDay = 0;
    private long m_lastTimestamp = 0;
    private BSTR m_date;
    //private byte[] m_data;
    //private int m_length;
    
    public DateParser() {
        m_calendar.set(Calendar.HOUR_OF_DAY, 0);
        m_calendar.set(Calendar.MINUTE, 0);
        m_calendar.set(Calendar.SECOND, 0);
        m_calendar.set(Calendar.MILLISECOND, 0);
    }
    
    
    public long getTimestamp(BSTR date) {
        byte[] data = date.buffer;
        int length = date.length;
        m_date = date;
        //m_data = data;
        //m_length = length;

        int year = 0;
        int month = 1;
        int day = 1;
        int hour = 0;
        int minute = 0;
        int second = 0;
        int millis = 0;

        //yyyy-MM-dd HH:mm:ss.fff
        //0    5  8  11 14 17 20
        
        if(length > 19) {
            require(data[19] == COMMA || data[19] == POINT);
            if(length == 20) millis = 0;
            if(length == 21) millis = digit(data[20]);
            else if(length == 22) millis = digit(data[20]) * 10 + digit(data[21]);
            else millis = digit(data[20]) * 100 + digit(data[21]) * 10 + digit(data[22]);
        }
        if(length > 16) {
            require(length >= 19);
            require(data[16] == COLON);
            second = digit(data[17]) * 10 + digit(data[18]);
            require(second < 60);
        }
        if(length > 13) {
            require(length >= 16);
            require(data[16] == COLON);
            minute = digit(data[14]) * 10 + digit(data[15]);
            require(minute < 60);
        }
        if(length > 10) {
            require(length >= 13);
            require(data[10] == SPACE);
            hour = digit(data[11]) * 10 + digit(data[12]);
            require(hour < 24);
        }
        if(length > 7) {
            require(length >= 10);
            require(data[7] == DASH);
            day = digit(data[8]) * 10 + digit(data[9]);
            require(day >= 1 && day <= 31);
        }
        if(length > 4) {
            require(length >= 7);
            require(data[4] == DASH);
            month = digit(data[5]) * 10 + digit(data[6]);
            require(month >= 1 && month <= 12);
        }
        
        require(length >= 4);
        month = digit(data[5]) * 10 + digit(data[6]);
        require(month >= 1 && month <= 12);
        year = digit(data[0]) * 1000 + digit(data[1]) * 100 + digit(data[2]) * 10 + digit(data[3]);
        
        if(m_lastYear != year || m_lastDay != day || m_lastMonth != month) {
            m_lastYear = year;
            m_lastDay = day;
            m_lastMonth = month;
            m_calendar.set(Calendar.YEAR, year);
            m_calendar.set(Calendar.MONTH, month - 1);    // 0-relative
            m_calendar.set(Calendar.DAY_OF_MONTH, day);
            m_lastTimestamp = m_calendar.getTimeInMillis();
        }
        
        return m_lastTimestamp + ((hour * 60 + minute) * 60 + second) * 1000 + millis;
    }

    private int digit(byte digit) {
        if(digit < ZERO || digit > NINE) error();
        return digit - ZERO;
    }
    
    private void require(boolean condition) {
        if(!condition) error();
    }
    
    private void error() {
        throw new RuntimeException("Invalid date format: " + m_date.toString());
    }
    
}
