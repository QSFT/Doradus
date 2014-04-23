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

public class DirectXLinkQueryAny implements Query, XLinkQuery {
	private FieldDefinition fieldDef;
	private Query innerQuery;
	private Set<BSTR> xresult;
	private Set<BSTR> xfilter;
	
	public DirectXLinkQueryAny(XLinkContext ctx, TableDefinition tableDef, LinkQuery lq) {
		fieldDef = tableDef.getFieldDef(lq.link);
		innerQuery = lq.innerQuery;
		xresult = search(ctx, fieldDef.getInverseTableDef(), innerQuery);
		if(lq.filter != null) {
			xfilter = search(ctx, fieldDef.getInverseTableDef(), lq.filter);
		}
	}
	
	private Set<BSTR> search(XLinkContext ctx, TableDefinition tableDef, Query query) {
		Set<BSTR> set = new HashSet<BSTR>();
		for(String xshard : ctx.xshards) {
			CubeSearcher searcher = ctx.olap.getSearcher(ctx.application, xshard);
			Result r = ResultBuilder.search(tableDef, query, searcher);
			IdSearcher ids = searcher.getIdSearcher(tableDef.getTableName());
			for(int i = 0; i < r.size(); i++) {
				if(!r.get(i)) continue;
				BSTR id = ids.getId(i);
				set.add(new BSTR(id));
			}
		}
		return set;
	}
	

	public void search(CubeSearcher searcher, Result result) {
		ValueSearcher vs = searcher.getValueSearcher(fieldDef.getTableName(), fieldDef.getXLinkJunction());
		Result r = new Result(vs.size());
		for(int i = 0; i < r.size(); i++) {
			BSTR val = vs.getValue(i);
			if(xfilter != null && !xfilter.contains(val)) continue;
			if(!xresult.contains(val)) continue;
			r.set(i);
		}
		FieldSearcher fs = searcher.getFieldSearcher(fieldDef.getTableName(), fieldDef.getXLinkJunction());
		fs.fillDocs(r, result);
	}
	
}
