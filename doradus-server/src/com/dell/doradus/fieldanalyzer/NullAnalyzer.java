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
 * This {@link FieldAnalyzer} prevents a field from being indexed since it returns no
 * terms regardless of the field value.
 */
public class NullAnalyzer extends FieldAnalyzer {
	
    private static final NullAnalyzer INSTANCE = new NullAnalyzer();
    public static NullAnalyzer instance() { return INSTANCE; }
    private NullAnalyzer() { }

    @Override
    public String[] tokenize(String value) {
        return new String[0];
    }

    @Override
    protected Collection<FieldType> getCompatibleFieldTypes() {
        return Arrays.asList(new FieldType[]{FieldType.BINARY,
                                             FieldType.BOOLEAN,
                                             FieldType.DOUBLE,
                                             FieldType.FLOAT,
                                             FieldType.INTEGER,
                                             FieldType.LONG,
                                             FieldType.TEXT,
                                             FieldType.TIMESTAMP});
    }

}
