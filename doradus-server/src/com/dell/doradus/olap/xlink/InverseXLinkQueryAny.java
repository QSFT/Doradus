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
import com.dell.doradus.olap.store.ValueSearcher;
import com.dell.doradus.search.query.LinkQuery;
import com.dell.doradus.search.query.Query;

public class InverseXLinkQueryAny implements Query, XLinkQuery {
	private FieldDefinition fieldDef;
	private Query innerQuery;
	private Set<BSTR> xresult;
	private Set<BSTR> xfilter;
	
	public InverseXLinkQueryAny(XLinkContext ctx, TableDefinition tableDef, LinkQuery lq) {
		fieldDef = tableDef.getFieldDef(lq.link);
		FieldDefinition inv = fieldDef.getInverseLinkDef();
		innerQuery = lq.innerQuery;
		xresult = search(ctx, fieldDef.getInverseTableDef(), innerQuery, inv.getXLinkJunction());
		if(lq.filter != null) {
			xfilter = search(ctx, fieldDef.getInverseTableDef(), lq.filter, inv.getXLinkJunction());
		}
	}
	
	private Set<BSTR> search(XLinkContext ctx, TableDefinition tableDef, Query query, String field) {
		Set<BSTR> set = new HashSet<BSTR>();
		for(String xshard : ctx.xshards) {
			CubeSearcher searcher = ctx.olap.getSearcher(ctx.application, xshard);
			Result r = ResultBuilder.search(tableDef, query, searcher);
			FieldSearcher fs = searcher.getFieldSearcher(tableDef.getTableName(), field);
			Result r2 = new Result(fs.fields());
			fs.fillValues(r, r2);
			ValueSearcher vs = searcher.getValueSearcher(tableDef.getTableName(), field);
			for(int i = 0; i < r2.size(); i++) {
				if(!r2.get(i)) continue;
				BSTR val = vs.getValue(i);
				set.add(new BSTR(val));
			}
		}
		return set;
	}

	public void search(CubeSearcher searcher, Result result) {
		IdSearcher ids = searcher.getIdSearcher(fieldDef.getTableDef().getTableName());
		for(int i = 0; i < result.size(); i++) {
			BSTR id = ids.getId(i);
			if(xfilter != null && !xfilter.contains(id)) continue;
			if(!xresult.contains(id)) continue;
			result.set(i);
		}
	}
	
}
