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

import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import javax.swing.text.html.HTMLEditorKit.ParserCallback;
import javax.swing.text.html.parser.ParserDelegator;

import com.dell.doradus.common.FieldType;
import com.dell.doradus.search.analyzer.SimpleText;

public class HTMLAnalyzer extends FieldAnalyzer {
	private static final ParserDelegator s_parser = new ParserDelegator();
	
    private static final HTMLAnalyzer INSTANCE = new HTMLAnalyzer();
    public static HTMLAnalyzer instance() { return INSTANCE; }
    private HTMLAnalyzer() { }
    
    class HTMLParserCallback extends ParserCallback {
    	List<String> m_tokens = new LinkedList<String>();
    	
    	@Override
    	public void handleText(char[] data, int pos) {
    		String value = new String(data);
    		m_tokens.addAll(new SimpleText().tokenize(value));
    	}
    }

    @Override
    public String[] tokenize(String value) {
    	Reader reader = new StringReader(value);
    	HTMLParserCallback callback = new HTMLParserCallback();
    	try {
			s_parser.parse(reader, callback, true);
		} catch (Exception e) {
		}
    	return callback.m_tokens.toArray(new String[0]);
    }

    @Override
    protected Collection<FieldType> getCompatibleFieldTypes() {
        return Arrays.asList(new FieldType[]{FieldType.TEXT});
    }

}
