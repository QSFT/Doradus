package com.dell.doradus.logservice.search.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.common.Utils;
import com.dell.doradus.logservice.pattern.IncompatibleCaseException;
import com.dell.doradus.search.query.AllQuery;
import com.dell.doradus.search.query.AndQuery;
import com.dell.doradus.search.query.BinaryQuery;
import com.dell.doradus.search.query.DatePartBinaryQuery;
import com.dell.doradus.search.query.NoneQuery;
import com.dell.doradus.search.query.NotQuery;
import com.dell.doradus.search.query.OrQuery;
import com.dell.doradus.search.query.Query;
import com.dell.doradus.search.query.RangeQuery;

public class FilterBuilder {
    protected static final Logger LOG = LoggerFactory.getLogger(FilterBuilder.class.getSimpleName());

    
    public static IFilter build(Query query) {

        if(query == null) return new FilterAll();
        
        if(query instanceof AllQuery) return new FilterAll();
        
        if(query instanceof NoneQuery) return new FilterNone();
        
        if(query instanceof NotQuery) {
            return new FilterNot(build(((NotQuery)query).innerQuery));
        }
        
        if(query instanceof AndQuery) {
            FilterAnd filter = new FilterAnd();
            for(Query qu : ((AndQuery)query).subqueries) {
                filter.add(build(qu));
            }
            return filter;
        }

        if(query instanceof OrQuery) {
            FilterOr filter = new FilterOr();
            for(Query qu : ((OrQuery)query).subqueries) {
                filter.add(build(qu));
            }
            return filter;
        }
        
        
        if(query instanceof BinaryQuery) {
            BinaryQuery bq = (BinaryQuery)query;
            return build(bq.field, bq.value, bq.operation);
        }
        
        if(query instanceof RangeQuery) {
            RangeQuery rq = (RangeQuery)query;
            String field = rq.field;
            
            if("Timestamp".equals(field)) {
                return new FilterTimestampRange(rq);
            } else {
                return new FilterField(rq.field, new FilterFieldRange(rq));
            }
        }
        
        if(query instanceof DatePartBinaryQuery) {
            DatePartBinaryQuery dpq = (DatePartBinaryQuery)query;
            return new FilterDatePart(dpq);
        }
        
        throw new IllegalArgumentException("Query " + query.getClass().getSimpleName() + " not supported");
    }
    
    public static IFilter build(String field, String value, String operation) {
        if("*".equals(field)) field = null;
        Utils.require(value != null, "is null not supported");
        if(field == null && "*".equals(value)) return new FilterAll();
        Utils.require(!"Timestamp".equals(field), "Timestamp can be only in range query");
        
        boolean isAnyField = field == null || "*".equals(field);
        boolean isPattern = value.indexOf('*') >= 0;
        
        boolean isEquals = BinaryQuery.EQUALS.equals(operation);
        boolean isContains = BinaryQuery.CONTAINS.equals(operation);
        boolean isRegexp = BinaryQuery.REGEXP.equals(operation);
        
        IValuesFilter valuesFilter = null;
        try {
            if(isRegexp) {
                valuesFilter = new FilterRegex(value);
            } else if(isEquals) {
                valuesFilter = new FilterPattern(value);
            } else if(isContains) {
                if(isPattern) {
                    valuesFilter = new FilterPattern("*" + value + "*");
                } else {
                    valuesFilter = new FilterContains(value);
                }
            } else {
                throw new IllegalArgumentException("Unknown operation: " + operation);
            }
        } catch(IncompatibleCaseException e) {
            LOG.info("incompatible case: " + value);
            if(isEquals) valuesFilter = new FilterPattern(value);
            else valuesFilter = new FilterPatternSlow("*" + value + "*");
        }

        return isAnyField ? new FilterAnyField(valuesFilter) : new FilterField(field, valuesFilter);
    }
    
}
