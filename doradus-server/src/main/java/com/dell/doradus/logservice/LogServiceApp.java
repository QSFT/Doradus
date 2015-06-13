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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.search.SearchResult;
import com.dell.doradus.search.SearchResultList;
import com.dell.doradus.service.db.Tenant;
import com.dell.doradus.service.schema.SchemaService;

// simple REST application based on LogService
public class LogServiceApp {
    private Tenant m_tenant;
    private LogService m_logService;
	private Map<String, String> m_parameters;
	private StringBuilder m_builder;
	
	public String getApplication() { return getParam("application"); }
	public String getTable() { return getParam("table"); }
	public String getQuery() { return getParam("query"); }
    public String getFields() { return getParam("fields"); }
    public String getContinue() { return getParam("continue"); }
    public boolean getInverse() { return "true".equals(getParam("inverse")); }
	
	private String getParam(String name) {
		String value = m_parameters.get(name);
		if("null".equals(value)) return null;
		if(value != null) {
		    value = value.replace("'", "%27");
		}
		return value;
	}
	
	public LogServiceApp(Tenant tenant, LogService logService, Map<String, String> parameters) {
	    m_tenant = tenant;
	    m_logService = logService;
	    m_parameters = parameters;
	    m_builder = new StringBuilder();
	}
	
	public static String process(Tenant tenant, LogService logService, Map<String, String> parameters) {
	    LogServiceApp lsapp = new LogServiceApp(tenant, logService, parameters);
	    return lsapp.process();
	}
	
	private String process() {
		if(getApplication() == null) {
			return processGetApplications();
		} else {
			return processSearch();
		}
	}
	
	private String processGetApplications() {
	    List<ApplicationDefinition> appDefs = new ArrayList<>();
	    for(ApplicationDefinition appDef: SchemaService.instance().getAllApplications(m_tenant)) {
	        if(!"LoggingService".equals(appDef.getStorageService())) continue;
	        appDefs.add(appDef);
	    }
		m_builder.append("<html><body><table border='2'>");
		addHeader("Applications", "Tables");
		for(ApplicationDefinition appDef : appDefs) {
		    String application = appDef.getAppName();
			addRow("<b>"+application+"</b>", "");
			for(TableDefinition tableDef : appDef.getTableDefinitions().values()) {
			    String table = tableDef.getTableName();
			    String link = createLink(table, "application", application, "table", table);
				addRow("", link);
			}
		}
		m_builder.append("</table></body></html>");
		return m_builder.toString();
	}

	private String processSearch() {
	    String queryParam = createQueryParam(
	            "skipCount", "true",
	            "f", getFields(),
	            "q", getQuery(),
	            "s", "25",
	            "o", getInverse() ? "Timestamp DESC" : "Timestamp",
	            "e", getContinue());
	    LogQuery logQuery = new LogQuery(queryParam);
	    SearchResultList result = m_logService.search(m_tenant, getApplication(), getTable(), logQuery);
        m_builder.append("<html><body>");

        if(result.results.size() == 0) {
            m_builder.append("Nothing found");
            m_builder.append("</body></html>");
            return m_builder.toString();
        }
        
        List<String> columnsList = new ArrayList<>();
        columnsList.add("Timestamp");
        boolean hasMessage = false;
        for(String c: result.results.get(0).scalars.keySet()) {
            if("Timestamp".equals(c)) continue;
            if("_ID".equals(c)) continue;
            if("Message".equals(c)) {
                hasMessage = true;
                continue;
            }
            columnsList.add(c);
        }
        if(hasMessage) columnsList.add("Message");

        
        if(getInverse()) Collections.reverse(result.results);

        String firstCT = result.results.get(0).id();
        String lastCT = result.results.get(result.results.size() - 1).id();
        
        m_builder.append("<b>");
        m_builder.append(createLink("<<", 
                "application", getApplication(),
                "table", getTable(),
                "fields", getFields(),
                "query", getQuery()));
        m_builder.append("&nbsp;&nbsp;&nbsp;\r\n");
        m_builder.append(createLink("<", 
                "application", getApplication(),
                "table", getTable(),
                "fields", getFields(),
                "query", getQuery(),
                "inverse", "true",
                "continue", firstCT));
        m_builder.append("&nbsp;&nbsp;&nbsp;\r\n");
        m_builder.append("&nbsp;&nbsp;&nbsp;\r\n");
        m_builder.append(createLink(">", 
                "application", getApplication(),
                "table", getTable(),
                "fields", getFields(),
                "query", getQuery(),
                "continue", lastCT));
        m_builder.append("&nbsp;&nbsp;&nbsp;\r\n");
        m_builder.append(createLink(">>", 
                "application", getApplication(),
                "table", getTable(),
                "fields", getFields(),
                "query", getQuery(),
                "inverse", "true"));
        m_builder.append("&nbsp;&nbsp;&nbsp;\r\n");
        
        m_builder.append("</b><br/><br/>");
        m_builder.append("<pre>");
        m_builder.append("<b>");
        for(String c: columnsList) {
            m_builder.append(c);
            m_builder.append('\t');
        }
        m_builder.append("</b>");
        m_builder.append("\r\n");
        
        
		for(SearchResult r: result.results) {
		    for(String c: columnsList) {
		        String value = r.scalars.get(c);
		        m_builder.append(value.trim());
                m_builder.append('\t');
		    }
		    m_builder.append("\r\n");
		}
		
		m_builder.append("</pre></body></html>\n\n");
		return m_builder.toString();
	}
	
	
	private void addHeader(String... values) {
		m_builder.append("<tr>");
		for(String value : values) {
			m_builder.append("<th>");
			m_builder.append(value);
			m_builder.append("</th>");
		}
		m_builder.append("</tr>\n");
	}
	private void addRow(String... values) {
		m_builder.append("<tr>");
		for(String value : values) {
			m_builder.append("<td>");
			m_builder.append(value);
			m_builder.append("</td>");
		}
		m_builder.append("</tr>\n");
	}
	
	private String createLink(String caption, String... params) {
	    String paramString = createQueryParam(params);
	    return String.format("<a href='?%s'>%s</a>", paramString, caption);
	}
	
    private String createQueryParam(String... params) {
        Map<String, String> parameters = new LinkedHashMap<>();
        for(int i = 0; i < params.length / 2; i++) {
            parameters.put(params[i*2], params[i*2+1]);
        }
        String paramString = createQueryParam(parameters);
        return paramString;
    }
	private String createQueryParam(Map<String, String> params) {
        StringBuilder sb = new StringBuilder();
        for(Map.Entry<String, String> param: params.entrySet()) {
            if(param.getValue() == null || param.getValue().length() == 0) continue;
            sb.append(param.getKey());
            sb.append("=");
            sb.append(param.getValue());
            sb.append("&");
        }
        if(sb.length() > 0) sb.setLength(sb.length() - 1);
        return sb.toString();
	}
	
}


