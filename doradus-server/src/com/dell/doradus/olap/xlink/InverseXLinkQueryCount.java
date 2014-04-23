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

import java.util.HashMap;
import java.util.Map;

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
import com.dell.doradus.search.query.LinkCountQuery;
import com.dell.doradus.search.query.LinkCountRangeQuery;
import com.dell.doradus.search.query.Query;

public class InverseXLinkQueryCount implements Query, XLinkQuery {
	static class IntVal
	{
		public int val = 0;
		@Override public String toString() { return "" + val; }
	}
	
	private FieldDefinition fieldDef;
	private Map<BSTR, IntVal> xcount;
	private int min;
	private int max;
	
	public InverseXLinkQueryCount(XLinkContext ctx, TableDefinition tableDef, LinkCountQuery lq) {
		fieldDef = tableDef.getFieldDef(lq.link);
		FieldDefinition inv = fieldDef.getInverseLinkDef();
		xcount = search(ctx, fieldDef.getInverseTableDef(), lq.filter, inv.getXLinkJunction());
		min = lq.count;
		max = lq.count + 1;
	}

	public InverseXLinkQueryCount(XLinkContext ctx, TableDefinition tableDef, LinkCountRangeQuery lq) {
		fieldDef = tableDef.getFieldDef(lq.link);
		FieldDefinition inv = fieldDef.getInverseLinkDef();
		xcount = search(ctx, fieldDef.getInverseTableDef(), lq.filter, inv.getXLinkJunction());
		min = lq.range.min == null ? Integer.MIN_VALUE : Integer.parseInt(lq.range.min);
		max = lq.range.max == null ? Integer.MAX_VALUE : Integer.parseInt(lq.range.max);
		if(!lq.range.minInclusive) min++;
		if(lq.range.maxInclusive) max++;
	}
	
	private Map<BSTR, IntVal> search(XLinkContext ctx, TableDefinition tableDef, Query query, String field) {
		if(query == null) query = new AllQuery();
		Map<BSTR, IntVal> map = new HashMap<BSTR, IntVal>();
		for(String xshard : ctx.xshards) {
			CubeSearcher searcher = ctx.olap.getSearcher(ctx.application, xshard);
			Result r = ResultBuilder.search(tableDef, query, searcher);
			FieldSearcher fs = searcher.getFieldSearcher(tableDef.getTableName(), field);
			int[] counts = new int[fs.fields()];
			IntIterator iter = new IntIterator();
			for(int i=0; i<r.size(); i++) {
				if(!r.get(i)) continue;
				fs.fields(i, iter);
				for(int j=0; j<iter.count(); j++) {
					counts[iter.get(j)]++;
				}
			}
			ValueSearcher vs = searcher.getValueSearcher(tableDef.getTableName(), field);
			for(int i = 0; i < vs.size(); i++) {
				if(counts[i] == 0) continue;
				BSTR val = vs.getValue(i);
				IntVal cnt = map.get(val);
				if(cnt == null) {
					cnt = new IntVal();
					map.put(new BSTR(val), cnt);
				}
				cnt.val += counts[i];
			}
		}
		return map;
	}

	public void search(CubeSearcher searcher, Result result) {
		IdSearcher ids = searcher.getIdSearcher(fieldDef.getTableDef().getTableName());
		for(int i = 0; i < result.size(); i++) {
			BSTR id = ids.getId(i);
			IntVal cnt = xcount.get(id);
			int c = cnt == null ? 0 : cnt.val;
			if(c >= min && c < max) {
				result.set(i);
			}
		}
	}
	
}
