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
import com.dell.doradus.common.Utils;

/**
 * This {@link FieldAnalyzer} returns the field value as a single, down-cased term for
 * indexing purposes. Hence, the text is "opaque".
 */
public class OpaqueTextAnalyzer extends FieldAnalyzer {
    private static final OpaqueTextAnalyzer INSTANCE = new OpaqueTextAnalyzer();
    public static OpaqueTextAnalyzer instance() { return INSTANCE; }
    private OpaqueTextAnalyzer() { }

    @Override
    public String[] tokenize(String value) {
    	return new String[] { OpaqueTextAnalyzer.getOpaqueTerm(value) };
    }

    public static String getOpaqueTerm(String value) {
    	if(value == null) return null;
    	value = value.toLowerCase();
    	if(value.length() >= 1024) {
    		value = Utils.md5Encode(value);
    	}
    	return value;
    }

    @Override
    protected Collection<FieldType> getCompatibleFieldTypes() {
        return Arrays.asList(new FieldType[]{FieldType.TEXT});
    }

}
