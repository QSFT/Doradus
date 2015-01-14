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

package com.dell.doradus.olap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.FieldType;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.olap.aggregate.AggregationResult;
import com.dell.doradus.service.db.Tenant;
import com.dell.doradus.service.olap.OLAPService;
import com.dell.doradus.service.schema.SchemaService;

// simple REST application based on OLAP
public class Olapp {
    private Tenant m_tenant;
    private Olap m_olap;
	private Map<String, String> m_parameters;
	private StringBuilder m_builder;
	
	public int pcount() { return m_parameters.size(); }
	public String getApplication() { return getParam("application"); }
	public String getShard() { return getParam("shard"); }
	public String getTable() { return getParam("table"); }
	public String getQuery() { return getParam("query"); }
	public String getField() { return getParam("field"); }
	public String getLink() { return getParam("link"); }
	
	private String getParam(String name) {
		String value = m_parameters.get(name);
		if("null".equals(value)) return null;
		return value;
	}
	
	public Olapp(Tenant tenant, Olap olap, Map<String, String> parameters) {
	    m_tenant = tenant;
	    m_olap = olap;
	    m_parameters = parameters;
	    m_builder = new StringBuilder();
	}
	
	public static String process(Tenant tenant, Olap olap, Map<String, String> parameters) {
	    Olapp olapp = new Olapp(tenant, olap, parameters);
	    return olapp.process();
	}
	
	private String process() {
		if(pcount() == 0) {
			return processGetApplications();
		} else {
			return processAggregate();
		}
		
		//throw new IllegalArgumentException("Not supported");
	}
	
	private String processGetApplications() {
	    List<ApplicationDefinition> appDefs = OLAPService.instance().getAllOLAPApplications(m_tenant);
		m_builder.append("<html><body><table border='1'>");
		addHeader("Applications", "Shards", "Tables");
		for(ApplicationDefinition appDef : appDefs) {
		    String application = appDef.getAppName();
			addRow("<b>"+application+"</b>", "", "");
			for(String shard : m_olap.listShards(appDef)) {
				addRow("", "<b>" + shard + "</b>", "");
				for(TableDefinition tableDef : appDef.getTableDefinitions().values()) {
					addRow("", "",
							String.format("<a href='?application=%s&shard=%s&table=%s&query=*'>%s</a>",
									application, shard, tableDef.getTableName(), tableDef.getTableName()));
				}
			}
		}
		m_builder.append("</table></body></html>");
		return m_builder.toString();
	}
	
	private String getAppPar() {
		return String.format("?application=%s&shard=%s&table=%s", getApplication(), getShard(), getTable());
	}
	
	private String processAggregate() {
		m_builder.append("<html><body>");
		//Filter
		m_builder.append("<table border='0'>");
		addRow("<b>Filter</b>", String.format("<a href='%s&query=*&field=%s&link=%s'><i>clear</i></a>",
				getAppPar(), getField(), getLink()));
		String[] query = getQuery().split("\\|");
		for(String qq : query) {
			addRow(qq);
		}
		
		m_builder.append("</table>\n\n");

		//Fields to group by
		ApplicationDefinition appDef = SchemaService.instance().getApplication(m_tenant, getApplication());
		TableDefinition tableDef = appDef.getTableDef(getTable());
		
		if(getLink() == null) {
			m_builder.append("<b>Fields</b><br/>\n");
			int i = 0;
			for(FieldDefinition fieldDef : getFields(tableDef)) {
				if(!fieldDef.isScalarField()) continue;
				String fv = null;
				if(fieldDef.getType() == FieldType.INTEGER || fieldDef.getType() == FieldType.LONG) {
					fv = fieldDef.getName();
					m_builder.append(String.format("<a href='%s&query=%s&field=%s&link=%s'>%s</a>&nbsp;&nbsp;&nbsp;&nbsp;",
							getAppPar(), getQuery(), fv, getLink(), fieldDef.getName()));
					fv = "BATCH(" + fieldDef.getName() + ",0,1000,10000,100000,1000000)";
					m_builder.append(String.format("<a href='%s&query=%s&field=%s&link=%s'>%s</a>&nbsp;&nbsp;&nbsp;&nbsp;",
							getAppPar(), getQuery(), fv, getLink(), fieldDef.getName() + "<i>(batch)</i>"));
					i++;
				} else  if(fieldDef.getType() == FieldType.TIMESTAMP) {
					fv = "TRUNCATE(" + fieldDef.getName() + ",HOUR)";
					m_builder.append(String.format("<a href='%s&query=%s&field=%s&link=%s'>%s</a>&nbsp;&nbsp;&nbsp;&nbsp;",
							getAppPar(), getQuery(), fv, getLink(), fieldDef.getName() + "<i>(hour)</i>"));
				} else {
					fv = fieldDef.getName();
					m_builder.append(String.format("<a href='%s&query=%s&field=%s&link=%s'>%s</a>&nbsp;&nbsp;&nbsp;&nbsp;",
							getAppPar(), getQuery(), fv, getLink(), fieldDef.getName()));
				}
				i++;
				if(i > 7) {
					i = 0;
					m_builder.append("<br/>\n");
				}
			}
			m_builder.append("<br/>\n");
			m_builder.append("<b>Links</b><br/>\n");
			i = 0;
			for(FieldDefinition fieldDef : getFields(tableDef)) {
				if(!fieldDef.isLinkField()) continue;
				m_builder.append(String.format("<a href='%s&query=%s&link=%s'>%s</a>&nbsp;&nbsp;&nbsp;&nbsp;",
						getAppPar(), getQuery(), fieldDef.getName(), fieldDef.getName()));
				i++;
				if(i > 7) {
					i = 0;
					m_builder.append("<br/>\n");
				}
			}
			m_builder.append("<br/>\n");
		} else {
			m_builder.append("<b>Fields (");
			String[] strs = getLink().split("\\.");
			TableDefinition td = tableDef;
			String current = null;
			m_builder.append(String.format("<a href='%s&query=%s&field=%s'>%s</a>",
					getAppPar(), getQuery(), getField(), tableDef.getTableName()));
			for(String str : strs) {
				FieldDefinition link = td.getFieldDef(str);
				td = td.getAppDef().getTableDef(link.getLinkExtent());
				if(current == null) current = str;
				else current += "." + str;
				m_builder.append(".");
				m_builder.append(String.format("<a href='%s&query=%s&link=%s'>%s</a>",
						getAppPar(), getQuery(), current, str));
			}
			m_builder.append(")</b>\n<br/>");
			int i = 0;
			for(FieldDefinition fieldDef : getFields(td)) {
				if(!fieldDef.isScalarField()) continue;
				String fv = null;
				if(fieldDef.getType() == FieldType.INTEGER || fieldDef.getType() == FieldType.LONG) {
					fv = getLink() + "." + fieldDef.getName();
					m_builder.append(String.format("<a href='%s&query=%s&field=%s&link=%s'>%s</a>&nbsp;&nbsp;&nbsp;&nbsp;",
							getAppPar(), getQuery(), fv, getLink(), fieldDef.getName()));
					fv = "BATCH(" + getLink() + "." + fieldDef.getName() + ",0,1000,10000,100000,1000000)";
					m_builder.append(String.format("<a href='%s&query=%s&field=%s&link=%s'>%s</a>&nbsp;&nbsp;&nbsp;&nbsp;",
							getAppPar(), getQuery(), fv, getLink(), fieldDef.getName() + "<i>(batch)</i>"));
					i++;
				} else  if(fieldDef.getType() == FieldType.TIMESTAMP) {
					fv = "TRUNCATE(" + getLink() + "." + fieldDef.getName() + ",HOUR)";
					m_builder.append(String.format("<a href='%s&query=%s&field=%s&link=%s'>%s</a>&nbsp;&nbsp;&nbsp;&nbsp;",
							getAppPar(), getQuery(), fv, getLink(), fieldDef.getName() + "<i>(hour)</i>"));
				} else {
					fv = getLink() + "." + fieldDef.getName();
					m_builder.append(String.format("<a href='%s&query=%s&field=%s&link=%s'>%s</a>&nbsp;&nbsp;&nbsp;&nbsp;",
							getAppPar(), getQuery(), fv, getLink(), fieldDef.getName()));
				}
				i++;
				if(i > 7) {
					i = 0;
					m_builder.append("<br/>\n");
				}
			}
			m_builder.append("<br/>\n");
			m_builder.append("<b>Links</b><br/>\n");
			i = 0;
			for(FieldDefinition fieldDef : getFields(td)) {
				if(!fieldDef.isLinkField()) continue;
				m_builder.append(String.format("<a href='%s&query=%s&link=%s.%s'>%s</a>&nbsp;&nbsp;&nbsp;&nbsp;",
						getAppPar(), getQuery(), getLink(), fieldDef.getName(), fieldDef.getName()));
				i++;
				if(i > 7) {
					i = 0;
					m_builder.append("<br/>\n");
				}
			}
			m_builder.append("<br/>\n");
		}
		
		AggregationResult r = m_olap.aggregate(appDef, getTable(), new OlapAggregate(
				getShard(),
				getQuery().replaceAll("\\|", " AND "),
				getField() == null ? null : "TOP(20," + getField() + ")",
				"COUNT(*)",
				null));

		m_builder.append("<br/>\n");
		
		m_builder.append("<table border='1'>");
		addHeader("Results", "" + r.documentsCount);
		for(AggregationResult.AggregationGroup gc : r.groups) {
			String fld = getField();
			String value = gc.name;
			if (value == null) {
			    continue;
			}
			if(fld.startsWith("BATCH")) {
				fld = fld.split("\\(|\\,| ")[1];
				if(value.indexOf('-') > 0) {
					value = value.replace("-", "TO");
					fld += "=[" + value + "]";
				} else fld += value;
			} else if(fld.startsWith("TRUNCATE")) {
				fld = fld.split("\\(|\\,| ")[1];
				fld += "=[&quot;" + value +"&quot; TO &quot;" + value.substring(0, 14) + "59:59&quot;]"; 
			} else fld += "=&quot;" + value + "&quot;";
			addRow(String.format("<a href='%s&query=%s&field=%s&link=%s'>%s</a>", 
					getAppPar(),
					getQuery() + "|" + fld,
					getField(),
					getLink(),
					gc.name), "" + gc.metricSet.values[0].toString());
		}
		m_builder.append("</table>\n");

		m_builder.append("</body></html>");
		return m_builder.toString();
	}
	
	private List<FieldDefinition> getFields(TableDefinition tableDef) {
		Set<FieldDefinition> fieldsSet = new HashSet<FieldDefinition>();
		for(FieldDefinition fieldDef : tableDef.getFieldDefinitions()) addField(fieldDef, fieldsSet);
		List<FieldDefinition> fields = new ArrayList<FieldDefinition>(fieldsSet);
		Collections.sort(fields, new Comparator<FieldDefinition>(){
			@Override public int compare(FieldDefinition x, FieldDefinition y) {
				return x.getName().compareTo(y.getName());
			}});
		return fields;
	}
	
	private void addField(FieldDefinition fieldDef, Set<FieldDefinition> fields) {
		if(fieldDef.getType() == FieldType.GROUP) {
			for(FieldDefinition child : fieldDef.getNestedFields()) addField(child, fields);
		}
		else fields.add(fieldDef);
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
	
}
