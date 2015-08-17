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

package com.dell.doradus.logservice;

import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;
import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.olap.ParsedQuery;

public class LogQuery {
    private String m_query;					// &q parameter
    private int    m_pageSize = -1;			// &s parameter
    private int    m_skip = 0;        		// &k parameter
    private String m_fields;				// &f parameter
    private String m_sortOrder;				// &o parameter
    private String m_continueAt;			// &e parameter
    private String m_continueAfter;			// &g parameter
    private boolean m_skipCount = true;     // &skipCount parameter
    
    public LogQuery(UNode searchNode) {
        assert searchNode != null;
        ParsedQuery parsedQuery = new ParsedQuery("search", searchNode);
        m_query = parsedQuery.get("query");
        m_pageSize = parsedQuery.getInt("size", -1);
        m_skip = parsedQuery.getInt("skip", 0);
        m_fields = parsedQuery.get("fields");
        m_sortOrder = parsedQuery.get("order");
        m_continueAt = parsedQuery.get("continue-at");
        m_continueAfter = parsedQuery.get("continue-after");
        m_skipCount = parsedQuery.getBoolean("skipCount", true);
        parsedQuery.checkInvalidParameters();
        checkDefaults();
    }
    
    public LogQuery(String queryParam) throws IllegalArgumentException {
        assert queryParam != null;
        ParsedQuery parsedQuery = new ParsedQuery(queryParam);
        m_query = parsedQuery.get("q");
        m_pageSize = parsedQuery.getInt("s", -1);
        m_skip = parsedQuery.getInt("k", 0);
        m_fields = parsedQuery.get("f");
        m_sortOrder = parsedQuery.get("o");
        m_continueAt = parsedQuery.get("e");
        m_continueAfter = parsedQuery.get("g");
        m_skipCount = parsedQuery.getBoolean("skipCount", true);
        parsedQuery.checkInvalidParameters();
        checkDefaults();
    }
    
    public String getQuery() { return m_query; }
    public int getPageSize() { return m_pageSize; }
    public int getSkip() { return m_skip; }
    public String getFields() { return m_fields; }
    public String getSortOrder() { return m_sortOrder; }
    public String getContinueAt() { return m_continueAt; }
    public String getContinueAfter() { return m_continueAfter; }
    public boolean getSkipCount() { return m_skipCount; }
    
    public int getPageSizeWithSkip() {
    	if(m_pageSize < 0) return m_skip + ServerConfig.getInstance().search_default_page_size;
    	else if(m_pageSize == 0) return Integer.MAX_VALUE;
    	else return m_pageSize + m_skip;
    }
    
    
    // Check required parameters and set default values.
    private void checkDefaults() {
        Utils.require(m_continueAt == null || m_continueAfter == null, "Both continue-at and continue-after parameters cannot be set");
        //Utils.require((m_continueAt == null && m_continueAfter == null) || m_sortOrder == null, "continuation parameters cannot be set if sort order is set");
        if (m_pageSize == -1) m_pageSize = ServerConfig.getInstance().search_default_page_size;
    }
    
}
