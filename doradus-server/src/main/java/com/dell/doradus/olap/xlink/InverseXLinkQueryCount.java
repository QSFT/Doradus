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

import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.olap.search.Result;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.olap.store.IdSearcher;
import com.dell.doradus.search.query.LinkCountQuery;
import com.dell.doradus.search.query.LinkCountRangeQuery;
import com.dell.doradus.search.query.Query;

public class InverseXLinkQueryCount implements Query, XLinkQuery {
	
	private FieldDefinition fieldDef;
	private XQueryCount xcount = new XQueryCount();
	private int min;
	private int max;
	
	public InverseXLinkQueryCount(XLinkContext ctx, TableDefinition tableDef, LinkCountQuery lq) {
		fieldDef = tableDef.getFieldDef(lq.link);
		xcount.setup(ctx, fieldDef, lq.filter);
		min = lq.count;
		max = lq.count + 1;
	}

	public InverseXLinkQueryCount(XLinkContext ctx, TableDefinition tableDef, LinkCountRangeQuery lq) {
		fieldDef = tableDef.getFieldDef(lq.link);
		xcount.setup(ctx, fieldDef, lq.filter);
		min = lq.range.min == null ? Integer.MIN_VALUE : Integer.parseInt(lq.range.min);
		max = lq.range.max == null ? Integer.MAX_VALUE : Integer.parseInt(lq.range.max);
		if(!lq.range.minInclusive) min++;
		if(lq.range.maxInclusive) max++;
	}
	
	public void search(CubeSearcher searcher, Result result) {
		IdSearcher ids = searcher.getIdSearcher(fieldDef.getTableDef().getTableName());
		for(int i = 0; i < result.size(); i++) {
			BSTR id = ids.getId(i);
			int count = xcount.get(id);
			if(count >= min && count < max) {
				result.set(i);
			}
		}
	}
	
}
