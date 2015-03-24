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

import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.olap.search.Result;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.search.query.LinkQuery;
import com.dell.doradus.search.query.NotQuery;
import com.dell.doradus.search.query.Query;

public class DirectXLinkQueryAll implements Query, XLinkQuery {
	private DirectXLinkQueryAny m_anyExists; 
	private DirectXLinkQueryAny m_hasFalse; 
	
	public DirectXLinkQueryAll(XLinkContext ctx, TableDefinition tableDef, LinkQuery lq) {
		m_anyExists = new DirectXLinkQueryAny(ctx, tableDef, new LinkQuery(LinkQuery.ANY, lq.link, lq.innerQuery, lq.filter));
		m_hasFalse = new DirectXLinkQueryAny(ctx, tableDef, new LinkQuery(LinkQuery.ANY, lq.link, new NotQuery(lq.innerQuery), lq.filter));
	}
	
	public void search(CubeSearcher searcher, Result result) {
		m_anyExists.search(searcher, result);
		Result hasFalse = new Result(result.size());
		m_hasFalse.search(searcher, hasFalse);
		result.andNot(hasFalse);
	}
	
}
