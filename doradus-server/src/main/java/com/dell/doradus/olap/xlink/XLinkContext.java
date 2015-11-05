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

package com.dell.doradus.olap.xlink;

import java.util.ArrayList;
import java.util.List;

import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.olap.Olap;
import com.dell.doradus.search.query.AndQuery;
import com.dell.doradus.search.query.IdQuery;
import com.dell.doradus.search.query.LinkCountQuery;
import com.dell.doradus.search.query.LinkCountRangeQuery;
import com.dell.doradus.search.query.LinkIdQuery;
import com.dell.doradus.search.query.LinkQuery;
import com.dell.doradus.search.query.NotQuery;
import com.dell.doradus.search.query.OrQuery;
import com.dell.doradus.search.query.PathComparisonQuery;
import com.dell.doradus.search.query.PathCountRangeQuery;
import com.dell.doradus.search.query.Query;

// class representing structures needed during the search/aggregate, if external links are present 
public class XLinkContext {
	public String application;
	public Olap olap;
	public List<String> xshards;
	
	public XLinkContext(String application, Olap olap, List<String> xshards, TableDefinition tableDef) {
		this.application = application;
		this.olap = olap;
		this.xshards = xshards;
	}

	public static boolean isXLinkQuery(TableDefinition tableDef, Query query) {
		if(query instanceof AndQuery) {
			AndQuery q = (AndQuery)query;
			for(Query c : q.subqueries) {
				if(isXLinkQuery(tableDef, c)) return true; 
			}
		}
		else if(query instanceof OrQuery) {
			OrQuery q = (OrQuery)query;
			for(Query c : q.subqueries) {
				if(isXLinkQuery(tableDef, c)) return true; 
			}
		}
		else if(query instanceof NotQuery) {
			NotQuery q = (NotQuery)query;
			if(isXLinkQuery(tableDef, q.innerQuery)) return true; 
		}
		else if(query instanceof LinkQuery) {
			LinkQuery lq = (LinkQuery)query;
			FieldDefinition fieldDef = tableDef.getFieldDef(lq.link);
			Utils.require(fieldDef != null, "Field " + lq.link + " does not exist");
			if(fieldDef.isGroupField()) return false;
			if(fieldDef.isXLinkField()) return true;
			if(isXLinkQuery(fieldDef.getInverseTableDef(), lq.innerQuery)) return true; 
		}
		else if(query instanceof LinkIdQuery) {
			LinkIdQuery lq = (LinkIdQuery)query;
			FieldDefinition fieldDef = tableDef.getFieldDef(lq.link);
			Utils.require(fieldDef != null, "Field " + lq.link + " does not exist");
			if(fieldDef.isXLinkField()) return true;
		}
		else if(query instanceof LinkCountQuery) {
			LinkCountQuery lq = (LinkCountQuery)query;
			FieldDefinition fieldDef = tableDef.getFieldDef(lq.link);
			Utils.require(fieldDef != null, "Field " + lq.link + " does not exist");
			if(fieldDef.isXLinkField()) return true;
		}
		else if(query instanceof LinkCountRangeQuery) {
			LinkCountRangeQuery lq = (LinkCountRangeQuery)query;
			FieldDefinition fieldDef = tableDef.getFieldDef(lq.link);
			Utils.require(fieldDef != null, "Field " + lq.link + " does not exist");
			if(fieldDef.isXLinkField()) return true;
		}
		else if(query instanceof PathCountRangeQuery) {
			PathCountRangeQuery lq = (PathCountRangeQuery)query;
			return XLinkGroupContext.hasXLink(lq.path);
		}
		else if(query instanceof PathComparisonQuery) {
			PathComparisonQuery lq = (PathComparisonQuery)query;
			return XLinkGroupContext.hasXLink(lq.group1) || XLinkGroupContext.hasXLink(lq.group2);
		}
		return false;
	}
	
	
	public void setupXLinkQuery(TableDefinition tableDef, Query query) {
		if(query == null) return;
		if(query instanceof AndQuery) {
			AndQuery q = (AndQuery)query;
			for(Query c : q.subqueries) {
				setupXLinkQuery(tableDef, c); 
			}
		}
		else if(query instanceof OrQuery) {
			OrQuery q = (OrQuery)query;
			for(Query c : q.subqueries) {
				setupXLinkQuery(tableDef, c); 
			}
		}
		else if(query instanceof NotQuery) {
			NotQuery q = (NotQuery)query;
			setupXLinkQuery(tableDef, q.innerQuery); 
		}
		else if(query instanceof LinkQuery) {
			LinkQuery lq = (LinkQuery)query;
			FieldDefinition fieldDef = tableDef.getFieldDef(lq.link);
			Utils.require(fieldDef != null, "Field " + lq.link + " does not exist");
			setupXLinkQuery(fieldDef.getInverseTableDef(), lq.innerQuery);
			setupXLinkQuery(fieldDef.getInverseTableDef(), lq.filter);
			if(!fieldDef.isXLinkField()) return;
			if(fieldDef.isXLinkInverse()) {
				if(LinkQuery.ALL.equals(lq.quantifier)) lq.xlink = new InverseXLinkQueryAll(this, tableDef, lq);
				else lq.xlink = new InverseXLinkQueryAny(this, tableDef, lq);
			} else {
				if(LinkQuery.ALL.equals(lq.quantifier)) lq.xlink = new DirectXLinkQueryAll(this, tableDef, lq);
				else lq.xlink = new DirectXLinkQueryAny(this, tableDef, lq);
			}
			if(LinkQuery.NONE.equals(lq.quantifier)) lq.xlink = new XLinkQueryNone(lq.xlink);
		}
		else if(query instanceof LinkIdQuery) {
			LinkIdQuery lq = (LinkIdQuery)query;
			FieldDefinition fieldDef = tableDef.getFieldDef(lq.link);
			Utils.require(fieldDef != null, "Field " + lq.link + " does not exist");
			if(!fieldDef.isXLinkField()) return;
			if(lq.id == null) {  // xlink IS NULL
				Utils.require(LinkQuery.ANY.equals(lq.quantifier),
					"only ANY quantifier is allowed with IS NULL clause");
				LinkCountQuery q = new LinkCountQuery(lq.link, 0);
				if(fieldDef.isXLinkInverse()) lq.xlink = new InverseXLinkQueryCount(this, tableDef, q);
				else lq.xlink = new DirectXLinkQueryCount(this, tableDef, q);
				return;
			}
			LinkQuery q = new LinkQuery(lq.quantifier, lq.link, new IdQuery(lq.id));
			if(fieldDef.isXLinkInverse()) {
				if(LinkQuery.ALL.equals(lq.quantifier)) lq.xlink = new InverseXLinkQueryAll(this, tableDef, q);
				else lq.xlink = new InverseXLinkQueryAny(this, tableDef, q);
			} else {
				if(LinkQuery.ALL.equals(lq.quantifier)) lq.xlink = new DirectXLinkQueryAll(this, tableDef, q);
				else lq.xlink = new DirectXLinkQueryAny(this, tableDef, q);
			}
			if(LinkQuery.NONE.equals(lq.quantifier)) lq.xlink = new XLinkQueryNone(lq.xlink);
		}
		else if(query instanceof LinkCountQuery) {
			LinkCountQuery lq = (LinkCountQuery)query;
			FieldDefinition fieldDef = tableDef.getFieldDef(lq.link);
			Utils.require(fieldDef != null, "Field " + lq.link + " does not exist");
			if(!fieldDef.isXLinkField()) return;
			setupXLinkQuery(fieldDef.getInverseTableDef(), lq.filter);
			if(fieldDef.isXLinkInverse()) {
				lq.xlink = new InverseXLinkQueryCount(this, tableDef, lq);
			} else {
				lq.xlink = new DirectXLinkQueryCount(this, tableDef, lq);
			}
		}
		else if(query instanceof LinkCountRangeQuery) {
			LinkCountRangeQuery lq = (LinkCountRangeQuery)query;
			FieldDefinition fieldDef = tableDef.getFieldDef(lq.link);
			Utils.require(fieldDef != null, "Field " + lq.link + " does not exist");
			if(!fieldDef.isXLinkField()) return;
			setupXLinkQuery(fieldDef.getInverseTableDef(), lq.filter);
			if(fieldDef.isXLinkInverse()) {
				lq.xlink = new InverseXLinkQueryCount(this, tableDef, lq);
			} else {
				lq.xlink = new DirectXLinkQueryCount(this, tableDef, lq);
			}
		}
		else if(query instanceof PathCountRangeQuery) {
			PathCountRangeQuery lq = (PathCountRangeQuery)query;
			XLinkGroupContext gctx = new XLinkGroupContext(this);
			gctx.setupXLinkGroup(lq.path);
		}
		else if(query instanceof PathComparisonQuery) {
			PathComparisonQuery lq = (PathComparisonQuery)query;
			XLinkGroupContext gctx = new XLinkGroupContext(this);
			XGroups g1 = gctx.setupXLinkGroup(lq.group1);
			XGroups g2 = gctx.setupXLinkGroup(lq.group2);
			if(g1 != null && g2 != null) {
				List<XGroups> list = new ArrayList<>();
				list.add(g1);
				list.add(g2);
				XGroups.mergeGroups(list);
			}
		}
	}
	
	
}
