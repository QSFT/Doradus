package com.dell.doradus.logservice.search.filter;

import java.util.Calendar;
import java.util.TimeZone;

import com.dell.doradus.common.Utils;
import com.dell.doradus.logservice.ChunkReader;
import com.dell.doradus.search.query.BinaryQuery;
import com.dell.doradus.search.query.DatePartBinaryQuery;

public class FilterDatePart implements IFilter {
    private int m_datePart;
    private int m_partValue;
    private Calendar m_cal;
    
    public FilterDatePart(DatePartBinaryQuery dpq) {
        m_datePart = dpq.part;
        BinaryQuery bq = dpq.innerQuery;
        String field = bq.field;
        String value = bq.value;
        Utils.require(BinaryQuery.EQUALS.equals(bq.operation), "Contains is not supported");
        Utils.require("Timestamp".equals(field), "Only timestamp field is supported");
        Utils.require(value.indexOf('*') < 0 && value.indexOf('?') < 0, "Wildcard search not supported for DatePartBinaryQuery");
        m_partValue = Integer.parseInt(value);
        //Months in Calendar start with 0 instead of 1
        if(m_datePart == Calendar.MONTH) m_partValue--;
        m_cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    }
    
    @Override public boolean check(ChunkReader reader, int doc) {
        m_cal.setTimeInMillis(reader.getTimestamp(doc));
        return m_cal.get(m_datePart) == m_partValue;
    }
    
}
