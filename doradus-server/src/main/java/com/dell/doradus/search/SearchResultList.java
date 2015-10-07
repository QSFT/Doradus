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

package com.dell.doradus.search;

import java.util.ArrayList;

import com.dell.doradus.common.UNode;

public class SearchResultList {
	public FieldSet fieldSet;
	public int documentsCount = -1;
	public ArrayList<SearchResult> results = new ArrayList<SearchResult>();
	public String continuation_token;
	
	public SearchResultList() { }

    public UNode toDoc() {
        UNode rootNode = UNode.createMapNode("results");
        if(documentsCount >= 0) {
        	rootNode.addValueNode("totalobjects", "" + documentsCount);
        }
        UNode docsNode = rootNode.addArrayNode("docs");
        for (SearchResult result: results) {
            docsNode.addChildNode(result.toDoc());
        }
        if(continuation_token != null) {
            rootNode.addValueNode("continue", continuation_token);
        }
        return rootNode;
    }
    
    @Override public String toString() {
    	return toDoc().toXML(true);
    	//return toDoc().toJSON(true);
    }
}
