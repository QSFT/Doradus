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

import com.dell.doradus.common.FieldType;

/**
 * This {@link FieldAnalyzer} tokenizes boolean text fields that have only True or False values
 */
public class BooleanAnalyzer extends FieldAnalyzer {
    private static final BooleanAnalyzer INSTANCE = new BooleanAnalyzer();
    public static BooleanAnalyzer instance() { return INSTANCE; }
    private BooleanAnalyzer() { }

    @Override
    public String[] tokenize(String value) {
    	if(!value.equalsIgnoreCase("True") && !value.equalsIgnoreCase("False") ) {
    		throw new RuntimeException("Boolean field must have only 'True' or 'False' values; instead found '" + value + "'");
    	}
    	return OpaqueTextAnalyzer.instance().tokenize(value);
    }
    
    @Override
    protected Collection<FieldType> getCompatibleFieldTypes() {
        return Arrays.asList(new FieldType[]{FieldType.BOOLEAN});
    }

}
