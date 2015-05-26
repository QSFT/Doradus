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
import com.dell.doradus.olap.ParsedQuery;

public class LogAggregate {
    private String  m_query;
    private String  m_fields;
    private String  m_metrics;
    
    public LogAggregate(UNode searchNode) {
        assert searchNode != null;
        ParsedQuery parsedQuery = new ParsedQuery("aggregate-search", searchNode);
        m_query = parsedQuery.get("query");
        m_fields = parsedQuery.get("grouping-fields");
        Utils.require(parsedQuery.get("composite-fields") == null,
        		"OLAP queries cannot use composite grouping composite-fields parameter");
        m_metrics = parsedQuery.get("metric");
        parsedQuery.checkInvalidParameters();
        checkDefaults();
    }
    
    public LogAggregate(String queryParam) throws IllegalArgumentException {
        assert queryParam != null;
        ParsedQuery parsedQuery = new ParsedQuery(queryParam);
        m_query = parsedQuery.get("q");
        m_fields = parsedQuery.get("f");
        Utils.require(parsedQuery.get("cf") == null,
        		"OLAP queries cannot use composite grouping composite-fields parameter");
        m_metrics = parsedQuery.get("m");
        parsedQuery.checkInvalidParameters();
        checkDefaults();
    }

    public String getQuery() { return m_query; }
    public String getFields() { return m_fields; }
    public String getMetrics() { return m_metrics; }
    
    private void checkDefaults() {
        if (m_query == null) m_query = "*";
        if(m_metrics == null) m_metrics = "COUNT(*)";
    }
    
}
