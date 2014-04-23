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

package com.dell.doradus.search.analyzer;

import java.util.ArrayList;
import java.util.List;

/**
 * Helps tokenize and search on numeric trie values  
 *
 */
public class NumericTrie {
	//base of the trie. the number of sub-nodes in the trie nodes
	//Values that are powers of the base, result in much fewer search terms,
	//so chose it according to the type of the values.
	//For example, for values like age, height, weight, temperature etc. chose bas=10,
	//because query like age>30 is more probable then age>32.
	//For values like file size chose bas=32, because queries like size > 1KB is more probable than
	//size > 1000bytes.
	public int bas;
	//Lower bound (inclusive) of the range query min <= x < max.
	//If the query is x < max then you should set min to be the minimal possible value that x can take.
	//It is better though not required to chose it to be 0 or power of the base.
	//The less the range min-max is, the less are clauses in the query, so try to make it as small as possible
	//even better to take these values from the database if possible to build the query.
	//for example, age is usually less than 110 years. So for the query age > 3, you can chose max=1000, provided that
	//the base is 10 (because 1000 is the next power of 10 greater than 110).
	public long min;
	//Upper bound (exclusive) of the range query min <= x < max.
	public long max;

	public NumericTrie(int base) {
		this(base, Long.MIN_VALUE, Long.MAX_VALUE);
	}

	public NumericTrie(int bas, long min, long max) {
		this.bas = bas;
		this.min = min;
		this.max = max;
	}
	
	
	public List<String> tokenize(long num) {
		List<String> tokens = new ArrayList<String>();
		if(num < 0) tokenize(tokens, -num, true);
		else tokenize(tokens, num, false);
		return tokens;
	}
	
	public List<String> getSearchTerms() {
		List<String> terms = new ArrayList<String>();
		if(min < 0 && max >= 0) {
			range(terms, 1, -min + 1, true);
			range(terms, 0, max, false);
		}
		else if(min < 0 && max < 0) {
			range(terms, -max + 1, -min + 1, true);
		}
		// min >= 0 && max >= 0: no other option
		else {
			range(terms, min, max, false);
		}
		return terms;
	}
	
	
	private void tokenize(List<String> terms, long num, boolean lessThanZero) {
		long det = 1;
		add(terms, det, num, lessThanZero);
		while(num > 0) {
			num /= bas;
			det *= bas;
			add(terms, det, num, lessThanZero);
		}
	}
	
	private void range(List<String> terms, long a, long b, boolean lessThanZero) {
		long x = a;
		long det = 1;
		while(x < b) {
			if(x == 0) {
				add(terms, 1, 0, false);
				x = 1;
			}
			else if(x == det && x * bas <= b) {
				det *= bas;
				add(terms, det, 0, lessThanZero);
				x *= bas;
			}
			else if(x % (bas * det) == 0 && x + det * bas <= b) {
				det *= bas;
			}
			else if(x + det > b) {
				det /= bas;
			}
			else {
				add(terms, det, x/det, lessThanZero);
				x += det;
			}
		}
	}
	
	
	
	private static void add(List<String> terms, long det, long num, boolean lessThanZero) {
		StringBuilder sb = new StringBuilder();
		if(det != 1) {
			sb.append(det);
			sb.append('/');
		}
		if(lessThanZero) sb.append('-');
		sb.append(num);
		terms.add(sb.toString());
	}
}
