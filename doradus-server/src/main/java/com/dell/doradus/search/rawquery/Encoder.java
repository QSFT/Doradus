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

package com.dell.doradus.search.rawquery;

public class Encoder {
	public static String encode(String value) {
		if(onlyLettersAndDigits(value)) return value;
		
		StringBuilder sb = new StringBuilder();
		sb.append('"');
		for(int i = 0; i < value.length(); i++) {
			char c = value.charAt(i);
			if(c == '\\' || c == '"') sb.append('\\');
			sb.append(c);
		}
		sb.append('"');
		return sb.toString();
		
	}
	
	private static boolean onlyLettersAndDigits(String value) {
		for(int i = 0; i < value.length(); i++) {
			if(!Character.isLetterOrDigit(value.charAt(i))) return false; 
		}
		return true;
	}
}
