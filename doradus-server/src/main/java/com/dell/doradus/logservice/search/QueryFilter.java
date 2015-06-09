package com.dell.doradus.logservice.search;

import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;
import java.util.regex.Pattern;

import com.dell.doradus.common.Utils;
import com.dell.doradus.logservice.ChunkReader;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.search.query.AllQuery;
import com.dell.doradus.search.query.AndQuery;
import com.dell.doradus.search.query.BinaryQuery;
import com.dell.doradus.search.query.DatePartBinaryQuery;
import com.dell.doradus.search.query.NoneQuery;
import com.dell.doradus.search.query.NotQuery;
import com.dell.doradus.search.query.OrQuery;
import com.dell.doradus.search.query.Query;
import com.dell.doradus.search.query.RangeQuery;

public class QueryFilter {

    public static boolean filter(Query query, ChunkReader reader, int doc) {

        if(query instanceof AllQuery) return true;
        if(query instanceof NoneQuery) return false;
        
        if(query instanceof NotQuery) {
            return !filter(((NotQuery)query).innerQuery, reader, doc);
        }
        
        if(query instanceof AndQuery) {
            for(Query qu : ((AndQuery)query).subqueries) {
                if(!filter(qu, reader, doc)) return false;
            }
            return true;
        }

        if(query instanceof OrQuery) {
            for(Query qu : ((OrQuery)query).subqueries) {
                if(filter(qu, reader, doc)) return true;
            }
            return false;
        }
        
        
        if(query instanceof BinaryQuery) {
            BinaryQuery bq = (BinaryQuery)query;
            String field = bq.field;
            if("*".equals(field)) field = null;
            String value = bq.value;
            Utils.require(value != null, "is null not supported");
            value = value.toLowerCase(Locale.ROOT);
            if(field == null && "*".equals(value)) return true;
            Utils.require(!"Timestamp".equals(field), "Timestamp can be only in range query");
            boolean isPattern = value.indexOf('*') >= 0 || value.indexOf('?') >= 0;
            
            if(field == null) {
                if(BinaryQuery.EQUALS.equals(bq.operation)) {
                    for(int i = 0; i < reader.fieldsCount(); i++) {
                        String v = reader.getFieldValue(doc, i);
                        if(isPattern) {
                            if(Utils.matchesPattern(v, value)) return true;
                        }
                        else if(v.toLowerCase(Locale.ROOT).equals(value)) return true;
                    }
                    return false;
                }
                if(BinaryQuery.CONTAINS.equals(bq.operation)) {
                    for(int i = 0; i < reader.fieldsCount(); i++) {
                        String v = reader.getFieldValue(doc, i);
                        if(isPattern) {
                            if(Utils.matchesPattern(v, "*" + value + "*")) return true;
                        }
                        else if(v.toLowerCase(Locale.ROOT).contains(value)) return true;
                    }
                    return false;
                }
                if(BinaryQuery.REGEXP.equals(bq.operation)) {
                    for(int i = 0; i < reader.fieldsCount(); i++) {
                        String v = reader.getFieldValue(doc, i);
                        if(Pattern.matches(v, value)) return true;
                    }
                    return false;
                }
                throw new IllegalArgumentException("Only equals or contains or regexp are supported");
            }
            
            int fieldIndex = reader.getFieldIndex(new BSTR(field));
            if(fieldIndex < 0) return false;
            
            if(BinaryQuery.EQUALS.equals(bq.operation)) {
                String v = reader.getFieldValue(doc, fieldIndex);
                if(isPattern) {
                    return Utils.matchesPattern(v, value);
                }
                else return v.toLowerCase(Locale.ROOT).equals(value);
            }
            if(BinaryQuery.CONTAINS.equals(bq.operation)) {
                String v = reader.getFieldValue(doc, fieldIndex);
                if(isPattern) {
                    return Utils.matchesPattern(v, "*" + value + "*");
                }
                return v.toLowerCase(Locale.ROOT).contains(value);
            }
            if(BinaryQuery.REGEXP.equals(bq.operation)) {
                String v = reader.getFieldValue(doc, fieldIndex);
                return Pattern.matches(v, value);
            }
            throw new IllegalArgumentException("Only equals or contains or regexp are supported");
        }
        
        if(query instanceof RangeQuery) {
            RangeQuery rq = (RangeQuery)query;
            String field = rq.field;
            
            if("Timestamp".equals(field)) {
                long minTimestamp = Long.MIN_VALUE;
                long maxTimestamp = Long.MAX_VALUE;
                
                if(rq.min != null) {
                    minTimestamp = Utils.parseDate(rq.min).getTimeInMillis();
                    if(!rq.minInclusive) minTimestamp++;
                }
                if(rq.max != null) {
                    maxTimestamp = Utils.parseDate(rq.max).getTimeInMillis();
                    if(rq.maxInclusive) maxTimestamp++;
                }
                long timestamp = reader.getTimestamp(doc);
                return timestamp >= minTimestamp && timestamp < maxTimestamp;
            }

            
            int fieldIndex = reader.getFieldIndex(new BSTR(field));
            if(fieldIndex < 0) return false;
            String v = reader.getFieldValue(doc, fieldIndex);
            if(rq.min != null) {
                int c = v.compareTo(rq.min);
                if(rq.minInclusive && c < 0) return false;
                if(c <= 0) return false;
                return true;
            }
            if(rq.max != null) {
                int c = v.compareTo(rq.max);
                if(rq.maxInclusive && c > 0) return false;
                if(c >= 0) return false;
                return true;
            }
        }
        
        if(query instanceof DatePartBinaryQuery) {
            DatePartBinaryQuery dpq = (DatePartBinaryQuery)query;
            int datePart = dpq.part;
            BinaryQuery bq = dpq.innerQuery;
            String field = bq.field;
            String value = bq.value;
            Utils.require(BinaryQuery.EQUALS.equals(bq.operation), "Contains is not supported");
            Utils.require("Timestamp".equals(field), "Only timestamp field is supported");
            Utils.require(value.indexOf('*') < 0 && value.indexOf('?') < 0, "Wildcard search not supported for DatePartBinaryQuery");
            int partValue = Integer.parseInt(value);
            //Months in Calendar start with 0 instead of 1
            if(datePart == Calendar.MONTH) partValue--;
            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
            cal.setTimeInMillis(reader.getTimestamp(doc));
            return cal.get(datePart) == partValue;
        }
        
        throw new IllegalArgumentException("Query " + query.getClass().getSimpleName() + " not supported");
    }
}
