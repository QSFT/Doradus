package com.dell.doradus.logservice;

import com.dell.doradus.common.Utils;

public class DateFormatter {
    private static final long DAY_MS = 1000 * 3600 * 24;
    private long m_lastDay = 0;
    //yyyy-MM-dd HH:mm:ss.fff
    //0    5  8  11 14 17 20
    private char[] m_date = new char[23];
    
    public DateFormatter() {
        m_date = "0000-00-00 00:00:00.000".toCharArray();
    }
    
    public String format(long timestamp) {
        long time = timestamp % DAY_MS; 
        long day = timestamp - time;
        if(day != m_lastDay) {
            m_lastDay = day;
            String date = Utils.formatDate(timestamp);
            for(int i = 0; i < date.length(); i++) {
                m_date[i] = date.charAt(i);
            }
        }
        
        //milliseconds
        m_date[22] = (char)('0' + (time % 10));
        time /= 10;
        m_date[21] = (char)('0' + (time % 10));
        time /= 10;
        m_date[20] = (char)('0' + (time % 10));
        time /= 10;
        //seconds
        m_date[18] = (char)('0' + (time % 10));
        time /= 10;
        m_date[17] = (char)('0' + (time % 6));
        time /= 6;
        //minutes
        m_date[15] = (char)('0' + (time % 10));
        time /= 10;
        m_date[14] = (char)('0' + (time % 6));
        time /= 6;
        //hours
        m_date[12] = (char)('0' + (time % 10));
        time /= 10;
        m_date[11] = (char)('0' + (time));
        
        return new String(m_date);
    }
}
