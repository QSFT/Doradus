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
import com.dell.doradus.search.query.Query;

public class XQueryAny {
	
	private Set<BSTR> xresult = new HashSet<BSTR>();

	public void add(BSTR id) {
		xresult.add(new BSTR(id));
	}
	
	public boolean contains(BSTR id) {
		return xresult.contains(id);
	}
	
	
	public void setup(XLinkContext ctx, FieldDefinition fieldDef, Query query, Query filter) {
		FieldDefinition inverse = fieldDef.getInverseLinkDef();
		if(inverse.isXLinkDirect()) setupInverse(ctx, inverse.getTableDef(), query, filter, inverse.getXLinkJunction());
		else setupDirect(ctx, inverse.getTableDef(), query, filter);
	}
	
	
	private void setupDirect(XLinkContext ctx, TableDefinition tableDef, Query query, Query filter) {
		for(String xshard : ctx.xshards) {
			CubeSearcher searcher = ctx.olap.getSearcher(ctx.application, xshard);
			Result r = ResultBuilder.search(tableDef, query, searcher);
			if(filter != null) {
				Result f = ResultBuilder.search(tableDef, filter, searcher);
				r.and(f);
			}
			IdSearcher ids = searcher.getIdSearcher(tableDef.getTableName());
			for(int i = 0; i < r.size(); i++) {
				if(!r.get(i)) continue;
				BSTR id = ids.getId(i);
				add(id);
			}
		}
	}
	
	
	private void setupInverse(XLinkContext ctx, TableDefinition tableDef, Query query, Query filter, String field) {
		for(String xshard : ctx.xshards) {
			CubeSearcher searcher = ctx.olap.getSearcher(ctx.application, xshard);
			Result r = ResultBuilder.search(tableDef, query, searcher);
			if(filter != null) {
				Result f = ResultBuilder.search(tableDef, filter, searcher);
				r.and(f);
			}
			FieldSearcher fs = searcher.getFieldSearcher(tableDef.getTableName(), field);
			Result r2 = new Result(fs.fields());
			fs.fillValues(r, r2);
			ValueSearcher vs = searcher.getValueSearcher(tableDef.getTableName(), field);
			for(int i = 0; i < r2.size(); i++) {
				if(!r2.get(i)) continue;
				BSTR val = vs.getValue(i);
				add(val);
			}
		}
	}
	
}
