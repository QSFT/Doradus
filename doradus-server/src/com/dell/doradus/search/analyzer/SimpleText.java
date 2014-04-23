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
 * Helps tokenize and search on fields tokenized as a simple text
 *
 */
public class SimpleText {
	
	public SimpleText() { }

	private static boolean isApostrofe(char ch) {
		return ch == 0x27 || ch == 0x92 || ch == 0x2019 ;
	}

	private static boolean isWildcard(char ch) {
		return ch == '*' || ch == '?';
	}
	
	private static boolean isLetterOrDigitOrApostrofe(char ch) {
		return Character.isLetterOrDigit(ch) || isApostrofe(ch);
	}

	//get list of tokens from the text for indexing
	public List<String> tokenize(String text) {
		List<String> tokens = new ArrayList<String>();
		text = text.toLowerCase();
		char[] array = text.toCharArray();
		
		//convert all apostrophes to 0x27
		for(int i = 0; i < array.length; i++) {
			if(isApostrofe(array[i])) array[i] = 0x27;
		}
		
		int pos = 0;
		
		//term cycle
		while(pos < array.length) {
			//scan to the start of the term
			while(pos < array.length  && !Character.isLetterOrDigit(array[pos])) pos++;
			int start = pos;
			if(start == array.length) break;
			//scan to the end of the term
			while(pos < array.length  && isLetterOrDigitOrApostrofe(array[pos])) pos++;
			int newpos = pos;
			while(newpos > start && isApostrofe(array[newpos - 1])) newpos--;
			if(newpos > start) tokens.add(new String(array, start, newpos - start));
		}
		return tokens;
	}

	//get list of tokens from the query text, assuming that asterisks and question signs are parts of the tokens
	public List<String> tokenizeWithWildcards(String text) {
		List<String> tokens = new ArrayList<String>();
		text = text.toLowerCase();
		char[] array = text.toCharArray();
		int pos = 0;

		//convert all apostrophes to 0x27
		for(int i = 0; i < array.length; i++) {
			if(isApostrofe(array[i])) array[i] = 0x27;
		}
		
		//term cycle
		while(pos < array.length) {
			//scan to the start of the term
			while(pos < array.length  && !(Character.isLetterOrDigit(array[pos]) || isWildcard(array[pos]))) pos++;
			int start = pos;
			if(start == array.length) break;
			//scan to the end of the term
			while(pos < array.length  && (isLetterOrDigitOrApostrofe(array[pos]) || isWildcard(array[pos]))) pos++;
			int newpos = pos;
			while(newpos > start && isApostrofe(array[newpos - 1])) newpos--;
			if(newpos > start) tokens.add(new String(array, start, newpos - start));
		}
		return tokens;
	}
	
	//check for a match between field's value and a query's text
	//note that match between tokenized strings means that their token lists match, not that they match literally;
	//so, the strings "Cat in the hat" and "cat(in the hat?)" match
	//to check for exact match use method value.equals(text) instead.
	public boolean match(List<String> valueTokens, List<String> queryTokens) {
		if(valueTokens.size() != queryTokens.size())return false;
		for(int i=0; i<valueTokens.size(); i++) {
			if(!valueTokens.get(i).equals(queryTokens.get(i)))return false;
		}
		return true;
	}

	//check that a query's text is contained in the field's value
	//note that true means that the query's token list is a sublist of the text's token list;
	//so, the string "THE HAT" is contained in the string "cat, in, the, hat"
	//to check for exact match use method value.equals(text) instead.
	public boolean contains(List<String> valueTokens, List<String> queryTokens) {
		for(int i=0; i<valueTokens.size(); i++) {
			boolean match = true;
			for(int j=0; j<queryTokens.size(); j++) {
				if(i + j >= valueTokens.size() || !valueTokens.get(i+j).equals(queryTokens.get(j))) {
					match = false;
					break;
				}
			}
			if(match == true)return true;
		}
		return false;
	}
	
}
