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

import com.dell.doradus.fieldanalyzer.DateAnalyzer;
import com.dell.doradus.fieldanalyzer.FieldAnalyzer;
import com.dell.doradus.fieldanalyzer.IntegerAnalyzer;
import com.dell.doradus.fieldanalyzer.OpaqueTextAnalyzer;
import com.dell.doradus.fieldanalyzer.TextAnalyzer;
import com.dell.doradus.search.FilteredIterable;
import com.dell.doradus.search.analyzer.DateTrie;
import com.dell.doradus.search.analyzer.NumericTrie;
import com.dell.doradus.search.filter.Filter;
import com.dell.doradus.search.filter.FilterRange;
import com.dell.doradus.search.filter.FilterRangeDate;
import com.dell.doradus.search.filter.FilterRangeInt;
import com.dell.doradus.search.iterator.TermsIterable;
import com.dell.doradus.search.query.Query;
import com.dell.doradus.search.query.RangeQuery;
import com.dell.doradus.service.spider.SpiderHelper;

public class BuilderRange extends SearchBuilder {
	private static final long MAXIMUM_VALUE = 1024L * 1024L * 1024L * 1024L * 1024L; 
    
	@Override public FilteredIterable search(Query query) {
		RangeQuery qu = (RangeQuery)query;
		String field = qu.field;
		if(field == null || "*".equals(field) || "".equals(field)) return null;

    	FieldAnalyzer analyzer = analyzer(field);
    	
    	TermsIterable terms = new TermsIterable(m_table, m_shards, m_params.continuation, m_params.inclusive);
    	
		if(analyzer instanceof DateAnalyzer) {
	    	DateTrie dateTrie = new DateTrie();
	    	Date minimum = dateTrie.parse("0001-01-01 00:00:00");
	    	Date maximum = dateTrie.parse("9999-01-01 00:00:00");
	    	//OPTIMIZATION: load min/max year from counters table
	    	List<String> years = SpiderHelper.getTerms(m_table, field, "year/", 100);
			if(years.size() == 100) return null;
			if(years.size() > 0) {
				String minYear = years.get(0).substring(5);
				String maxYear = years.get(years.size() - 1).substring(5);
				minimum = dateTrie.parse(minYear + "-01-01 00:00:00");
				maximum = dateTrie.parse(maxYear + "-12-31 23:59:59");
				maximum.setTime(maximum.getTime() + 1000);
			}
	    	
	    	if(qu.min != null) {
	    		minimum = dateTrie.parse(qu.min);
	    		if(!qu.minInclusive) minimum.setTime(minimum.getTime() + 1000);
	    	}
	    	if(qu.max != null) {
	    		maximum = dateTrie.parse(qu.max);
	    		if(qu.maxInclusive) maximum.setTime(maximum.getTime() + 1000);
	    	}
	    	
	    	List<String> tokens = new DateTrie(minimum, maximum).getSearchTerms();
	    	for(String token: tokens)terms.add(FieldAnalyzer.makeTermKey(field, token));
		} else if(analyzer instanceof IntegerAnalyzer) {
	    	long minimum = -MAXIMUM_VALUE;
	    	long maximum = MAXIMUM_VALUE;
	    	if(qu.min != null) {
	    		minimum = Long.parseLong(qu.min);
	    		if(!qu.minInclusive) minimum++;
	    	}
	    	if(qu.max != null) {
	    		maximum = Long.parseLong(qu.max);
	    		if(qu.maxInclusive) maximum++;
	    	}
	    	
	    	List<String> tokens = new NumericTrie(32, minimum, maximum).getSearchTerms();
	    	for(String token: tokens)terms.add(FieldAnalyzer.makeTermKey(field, token));
		} else {
			String min = qu.min;
			if(min == null)min = "";
			else if(!qu.minInclusive) min += "\u0000";
			min = min.toLowerCase();

			String max = qu.max;
			if(max == null)max = "\uffff";
			else if(qu.maxInclusive) max += "\u0000";
			max = max.toLowerCase();

			if(analyzer instanceof TextAnalyzer) {
				min = OpaqueTextAnalyzer.getOpaqueTerm(min);
				max = OpaqueTextAnalyzer.getOpaqueTerm(max);
			}
			
			int idx = 0;
			while(min.length() > idx && max.length() > idx && min.charAt(idx) == max.charAt(idx)) idx++;
			String prefix = min.substring(0, idx);
    		List<String> counters = SpiderHelper.getTerms(m_table, field, prefix, 100);
	    	if(counters.size() >= 100) return null;
	    	for(String val : counters) {
	    		if(val.compareTo(min) < 0)continue;
	    		if(val.compareTo(max) >= 0)continue;
    			terms.add(FieldAnalyzer.makeTermKey(field, val));
    		}
		}
		return create(terms, null);
	}
	
	@Override public Filter filter(Query query) {
		RangeQuery qu = (RangeQuery)query;
		String field = qu.field;
		if(field == null || "*".equals(field) || "".equals(field)) throw new IllegalArgumentException("all-field range query not supported");

		FieldAnalyzer analyzer = analyzer(field);
    	
		if(analyzer instanceof DateAnalyzer) {
			return new FilterRangeDate(qu);
		} else if(analyzer instanceof IntegerAnalyzer) {
			return new FilterRangeInt(qu);
		} else {
			return new FilterRange(qu);
		}
	}
   
}
