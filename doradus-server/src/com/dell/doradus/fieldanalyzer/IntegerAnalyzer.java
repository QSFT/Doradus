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
import java.util.List;

import com.dell.doradus.common.FieldType;
import com.dell.doradus.search.analyzer.NumericTrie;

/**
 * This {@link FieldAnalyzer} tokenizes integer text fields using integer tries with base=32
 * The value tokenized should be within the range -10^15 .. 10^15
 * If the value tokenized is outside the range, then one-bounded queries field<X and field>Y may not work correctly   
 */
public class IntegerAnalyzer extends FieldAnalyzer {
	public static final long MAXIMUM_VALUE = 1024L * 1024L * 1024L * 1024L * 1024L; 
    private static final IntegerAnalyzer INSTANCE = new IntegerAnalyzer();
    public static IntegerAnalyzer instance() { return INSTANCE; }
    private IntegerAnalyzer() { }

    @Override
    public String[] tokenize(String value) {
    	long val = Long.parseLong(value);
    	List<String> tokens = new NumericTrie(32).tokenize(val);
    	return tokens.toArray(new String[tokens.size()]);
    }

    @Override
    protected Collection<FieldType> getCompatibleFieldTypes() {
        return Arrays.asList(new FieldType[]{FieldType.INTEGER, FieldType.LONG});
    }

}
