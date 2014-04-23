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

package com.dell.doradus.fieldanalyzer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import com.dell.doradus.common.FieldType;
import com.dell.doradus.search.analyzer.DateTrie;

/**
 * This {@link FieldAnalyzer} tokenizes date-time text fields that have format 'yyyy-MM-dd HH:mm:ss'
 */
public class DateAnalyzer extends FieldAnalyzer {
	
    private static final DateAnalyzer INSTANCE = new DateAnalyzer();
    public static DateAnalyzer instance() { return INSTANCE; }
    private DateAnalyzer() { }

    @Override
    public String[] tokenize(String value) {
		Date dat = new DateTrie().parse(value);
		List<String> tokens = new DateTrie(null, null).tokenize(dat);
    	return tokens.toArray(new String[tokens.size()]);
    }
    
    @Override
    protected Collection<FieldType> getCompatibleFieldTypes() {
        return Arrays.asList(new FieldType[]{FieldType.TIMESTAMP});
    }

}
