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

import java.util.HashSet;
import java.util.Set;

import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.olap.search.Result;
import com.dell.doradus.olap.search.ResultBuilder;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.olap.store.FieldSearcher;
import com.dell.doradus.olap.store.IdSearcher;
import com.dell.doradus.olap.store.IntIterator;
import com.dell.doradus.olap.store.ValueSearcher;
import com.dell.doradus.search.query.AllQuery;
import com.dell.doradus.search.query.LinkQuery;
import com.dell.doradus.search.query.Query;

public class InverseXLinkQueryAll implements Query, XLinkQuery {
	private FieldDefinition fieldDef;
	private Query innerQuery;
	private Query filterQuery;
	private Set<BSTR> xExisting;
	private Set<BSTR> xHasFalse;
	
	public InverseXLinkQueryAll(XLinkContext ctx, TableDefinition tableDef, LinkQuery lq) {
		fieldDef = tableDef.getFieldDef(lq.link);
		FieldDefinition inv = fieldDef.getInverseLinkDef();
		innerQuery = lq.innerQuery;
		filterQuery = lq.filter == null ? new AllQuery() : lq.filter;
		setup(ctx, fieldDef.getInverseTableDef(), inv.getXLinkJunction());
	}
	
	private void setup(XLinkContext ctx, TableDefinition tableDef, String field) {
		xExisting = new HashSet<BSTR>();
		xHasFalse = new HashSet<BSTR>();
		for(String xshard : ctx.xshards) {
			CubeSearcher searcher = ctx.olap.getSearcher(ctx.application, xshard);
			Result rQuery = ResultBuilder.search(tableDef, innerQuery, searcher);
			Result rFilter = ResultBuilder.search(tableDef, filterQuery, searcher);
			FieldSearcher fs = searcher.getFieldSearcher(tableDef.getTableName(), field);
			Result rExisting = new Result(fs.fields());
			Result rHasFalse = new Result(fs.fields());
			fs.fillValues(rFilter, rExisting);
			IntIterator iter = new IntIterator();
			for(int doc = 0; doc < rQuery.size(); doc++) {
				if(!rFilter.get(doc)) continue;
				fs.fields(doc, iter);
				for(int i=0; i<iter.count(); i++) {
					int val = iter.get(i);
					rExisting.set(val);
					if(!rQuery.get(doc)) rHasFalse.set(val);
				}
				
			}
			ValueSearcher vs = searcher.getValueSearcher(tableDef.getTableName(), field);
			for(int i = 0; i < rExisting.size(); i++) {
				if(rFilter.get(i)) {
					BSTR val = vs.getValue(i);
					xExisting.add(new BSTR(val));
				}
				if(rHasFalse.get(i)) {
					BSTR val = vs.getValue(i);
					xHasFalse.add(new BSTR(val));
				}
			}
		}
	}

	public void search(CubeSearcher searcher, Result result) {
		IdSearcher ids = searcher.getIdSearcher(fieldDef.getTableDef().getTableName());
		for(int i = 0; i < result.size(); i++) {
			BSTR id = ids.getId(i);
			if(xExisting.contains(id) && !xHasFalse.contains(id)) result.set(i);
		}
	}
	
}
