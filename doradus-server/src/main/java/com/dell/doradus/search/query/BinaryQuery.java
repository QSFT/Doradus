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

package com.dell.doradus.search.query;

/**
 * Query that filters a field's value based on some value 
 * Contains, equals, greater, starts with and wild-card match 
 * are examples of BinaryQuery.
 */
public class BinaryQuery implements Query {
	//"Contains" operation
	public static String CONTAINS = "Contains";
	//"Equals" operation
	public static String EQUALS = "eq";
	//"Regexp" operation
	public static String REGEXP = "regexp";
	
	// the operation on the values
	public String operation;
	// The field whose value should be filtered
	public String field;
	// The second argument of the query
	public String value;
	
	public BinaryQuery() { }
	
	public BinaryQuery(String operation, String field, String value) {
		this.operation = operation;
		this.field = field;
		this.value = value;
	}
	
	@Override
	public String toString() {
		String opsign = null;
		if(CONTAINS.equals(operation)) opsign = ":";
		else if(EQUALS.equals(operation)) opsign = "=";
		else if(REGEXP.equals(operation)) opsign = "~=";
		else opsign = operation;
		return String.format("%s%s%s", field, opsign, value);
	}
	
	
	public static String encode(String value) {
		int idx = -1;
		while(true) {
			idx = value.indexOf('\\', idx + 1);
			if(idx < 0) break;
			value = value.substring(0, idx) + "\\\"" + value.substring(idx + 1);
			idx++;
		}
		return value;
		
	}
}
