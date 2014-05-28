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

package com.dell.doradus.search.builder;

import java.util.Date;
import java.util.List;

import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.fieldanalyzer.DateAnalyzer;
import com.dell.doradus.fieldanalyzer.FieldAnalyzer;
import com.dell.doradus.fieldanalyzer.IntegerAnalyzer;
import com.dell.doradus.fieldanalyzer.OpaqueTextAnalyzer;
import com.dell.doradus.fieldanalyzer.SimpleTextAnalyzer;
import com.dell.doradus.fieldanalyzer.TextAnalyzer;
import com.dell.doradus.search.FilteredIterable;
import com.dell.doradus.search.analyzer.DateTrie;
import com.dell.doradus.search.analyzer.SimpleText;
import com.dell.doradus.search.filter.Filter;
import com.dell.doradus.search.filter.FilterAllFieldsContains;
import com.dell.doradus.search.filter.FilterAllFieldsEquals;
import com.dell.doradus.search.filter.FilterContains;
import com.dell.doradus.search.filter.FilterEquals;
import com.dell.doradus.search.filter.FilterOr;
import com.dell.doradus.search.iterator.AndIterable;
import com.dell.doradus.search.iterator.OrIterable;
import com.dell.doradus.search.iterator.TermsIterable;
import com.dell.doradus.search.query.BinaryQuery;
import com.dell.doradus.search.query.Query;
import com.dell.doradus.service.spider.SpiderHelper;

public class BuilderBinary extends SearchBuilder {
    
	@Override public FilteredIterable search(Query query) {
		BinaryQuery qu = (BinaryQuery)query;
		String field = qu.field;
		String value = qu.value;
		if("*".equals(field) || "".equals(field)) field = null;
        if(field == null && "*".equals(value)) return null;
        //IS NULL
        if(field != null && value == null) return null;

        if(field == null) {
        	List<String> fields = SpiderHelper.getFields(m_table);
        	OrIterable or = new OrIterable(fields.size());
            if(BinaryQuery.EQUALS.equals(qu.operation)) {
            	for(String f: fields) {
            		FieldAnalyzer analyzer = analyzer(f);
            		if(!(analyzer instanceof SimpleTextAnalyzer || analyzer instanceof OpaqueTextAnalyzer)) {
            			analyzer = TextAnalyzer.instance();
            		}
            		FilteredIterable fi = getEquals(f, value, analyzer, qu);
            		if(fi == null) return create(all(), filter(query));
            		or.add(fi);
            	}
                return create(or, null);
            } else if(BinaryQuery.CONTAINS.equals(qu.operation)) {
            	for(String f: fields) {
            		FilteredIterable fi = getContains(f, value, TextAnalyzer.instance(), qu);
            		if(fi == null) return create(all(), filter(query));
            		or.add(fi);
            	}
                return create(or, null);
            } else throw new IllegalArgumentException("operation " + qu.operation + " not supported in l2r filter"); 
        }
        
		FieldDefinition f = m_table.getFieldDef(field);
		if(f != null && f.isGroupField()) return null;
        
		FieldAnalyzer analyzer = analyzer(field);
        
        if(BinaryQuery.EQUALS.equals(qu.operation)) {
    		FilteredIterable fi = getEquals(field, value, analyzer, qu);
    		if(fi == null) return create(all(), filter(query));
    		else return fi;
        } else if(BinaryQuery.CONTAINS.equals(qu.operation)) {
    		FilteredIterable fi = getContains(field, value, analyzer, qu);
    		if(fi == null) return create(all(), filter(query));
    		else return fi;
        } else throw new IllegalArgumentException("operation " + qu.operation + " not supported in l2r filter"); 
		
	}
	
	@Override public Filter filter(Query query) {
		BinaryQuery qu = (BinaryQuery)query;
		String field = qu.field;
		String value = qu.value;
		if("*".equals(field) || "".equals(field)) field = null;
        if(field == null && "*".equals(value)) return null;
    	if(field == null) {
            if(BinaryQuery.EQUALS.equals(qu.operation)) {
                return new FilterAllFieldsEquals(value);
            } else if(BinaryQuery.CONTAINS.equals(qu.operation)) {
                return new FilterAllFieldsContains(value);
            } else throw new IllegalArgumentException("operation " + qu.operation + " not supported in l2r filter"); 
    	}

		FieldDefinition f = m_table.getFieldDef(field);
		if(f != null && f.isGroupField()) {
			FilterOr filter = new FilterOr();
			for(FieldDefinition nested : f.getNestedFields()) {
				if(!nested.isScalarField()) continue;
				Filter inner = null;
		        if(BinaryQuery.EQUALS.equals(qu.operation)) {
		            inner = new FilterEquals(nested.getName(), value);
		        } else if(BinaryQuery.CONTAINS.equals(qu.operation)) {
		            inner = new FilterContains(nested.getName(), value);
		        } else throw new IllegalArgumentException("operation " + qu.operation + " not supported");
		        filter.add(inner);
			}
			return filter;
		}
    	
		FieldAnalyzer analyzer = analyzer(field);
    	
        if(BinaryQuery.EQUALS.equals(qu.operation)) {
        	if(value != null && analyzer instanceof DateAnalyzer) {
        		DateTrie dt = new DateTrie();
        		Date date = dt.parse(value);
        		if(date.getTime() % 1000 == 0) value = dt.format(date);
        	}
            return new FilterEquals(field, value);
        } else if(BinaryQuery.CONTAINS.equals(qu.operation)) {
        	if(analyzer instanceof DateAnalyzer) {
        		return new FilterEquals(field, value + "*");
        	}
            return new FilterContains(field, value);
        } else throw new IllegalArgumentException("operation " + qu.operation + " not supported in l2r filter"); 
	}
   
	private FilteredIterable getEquals(String field, String value, FieldAnalyzer analyzer, BinaryQuery query) {
		TermsIterable terms = new TermsIterable(m_table, m_shards, m_params.continuation, m_params.inclusive);
		if(analyzer instanceof DateAnalyzer) {
    		DateTrie dt = new DateTrie();
    		Date date = dt.parse(value);
    		terms.add(FieldAnalyzer.makeTermKey(field, dt.format(date)));
    		if(date.getTime() % 1000 != 0) {
    			return create(terms, new FilterEquals(field, value));
    		}
		} else if(analyzer instanceof IntegerAnalyzer) {
    		terms.add(FieldAnalyzer.makeTermKey(field, value));
		} else if(analyzer instanceof SimpleTextAnalyzer) {
			FilteredIterable c = getContains(field,  value, analyzer, query);
			return create(c.sequence(), new FilterEquals(field, value));
		} else {
			value = OpaqueTextAnalyzer.getOpaqueTerm(value);
			if(analyzer instanceof TextAnalyzer) value = "'" + value + "'";
    		int idx = value.indexOf('?');
    		int idx1 = value.indexOf('*');
    		if(idx < 0 || (idx > idx1 && idx1 >= 0))idx = idx1;
    		if(idx >= 0) {
	    		String prefix = value.substring(0, idx);
	    		List<String> counters = SpiderHelper.getTerms(m_table, field, prefix, 100);
	    		if(counters.size() >= 100) return null;
	    		for(String val : counters) {
	    			if(!Utils.matchesPattern(val, value)) continue;
	    			terms.add(FieldAnalyzer.makeTermKey(field, val));
	    		}
    		} else terms.add(FieldAnalyzer.makeTermKey(field, value));
		}
		return create(terms, null);
	}

	private FilteredIterable getContains(String field, String value, FieldAnalyzer analyzer, BinaryQuery query) {
		if(analyzer instanceof DateAnalyzer) {
			if(value.length() == 23) return getEquals(field, value, analyzer, query);
			else if(value.length() == 16) value = "minute/" + value;
			else if(value.length() == 13) value = "hour/" + value;
			else if(value.length() == 10) value = "day/" + value;
			else if(value.length() == 7) value = "month/" + value;
			else if(value.length() == 4) value = "year/" + value;
			else {
	    		DateTrie dt = new DateTrie();
	    		Date date = dt.parse(value);
	    		value = dt.format(date);
			}
			TermsIterable terms = new TermsIterable(m_table, m_shards, m_params.continuation, m_params.inclusive);
    		terms.add(FieldAnalyzer.makeTermKey(field, value));
    		return create(terms, null);
		} else {
            List<String> tokens = new SimpleText().tokenizeWithWildcards(value);
            if(tokens.size() == 0) return create(all(), filter(query));
            AndIterable and = new AndIterable(tokens.size());
            for(String token: tokens) {
    			TermsIterable terms = new TermsIterable(m_table, m_shards, m_params.continuation, m_params.inclusive);
	    		int idx = token.indexOf('?');
	    		int idx1 = token.indexOf('*');
	    		if(idx < 0 || (idx > idx1 && idx1 >= 0))idx = idx1;
	    		if(idx >= 0) {
		    		String prefix = token.substring(0, idx);
		    		List<String> counters = SpiderHelper.getTerms(m_table, field, prefix, 100);
		    		if(counters.size() >= 100) return null;
		    		for(String val : counters) {
		    		    // Skip "whole value" terms surrounded with single quotes
		    		    if (val.charAt(0) == '\'' || !Utils.matchesPattern(val, token)) continue;
		    			terms.add(FieldAnalyzer.makeTermKey(field, val));
		    		}
	    		} else terms.add(FieldAnalyzer.makeTermKey(field, token));
	    		and.add(terms);
            }
            return create(and, tokens.size() > 1 ? filter(query) : null);
		}
	}
	
	
}
