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
import com.dell.doradus.search.query.Query;

public class XQueryCount {
	
	static class IntVal
	{
		public int val = 0;
		
		public IntVal(int val) {
			this.val = val;
		}
		
		@Override public String toString() {
			return "" + val;
		}
	}
	
	private Map<BSTR, IntVal> xcount = new HashMap<BSTR, IntVal>();

	public void add(BSTR id, int count) {
		IntVal val = xcount.get(id);
		if(val == null) {
			xcount.put(new BSTR(id), new IntVal(count));
		}
		else val.val += count;
	}
	
	public int get(BSTR id) {
		IntVal val = xcount.get(id);
		if(val == null) return 0;
		else return val.val;
	}
	
	
	public void setup(XLinkContext ctx, FieldDefinition fieldDef, Query query) {
		if(query == null) query = new AllQuery();
		FieldDefinition inverse = fieldDef.getInverseLinkDef();
		if(inverse.isXLinkDirect()) setupInverse(ctx, inverse.getTableDef(), query, inverse.getXLinkJunction());
		else setupDirect(ctx, inverse.getTableDef(), query);
	}
	
	private void setupDirect(XLinkContext ctx, TableDefinition tableDef, Query query) {
		for(String xshard : ctx.xshards) {
			CubeSearcher searcher = ctx.olap.getSearcher(tableDef.getAppDef(), xshard);
			Result r = ResultBuilder.search(tableDef, query, searcher);
			IdSearcher ids = searcher.getIdSearcher(tableDef.getTableName());
			for(int i = 0; i < r.size(); i++) {
				if(!r.get(i)) continue;
				BSTR id = ids.getId(i);
				add(id, 1);
			}
		}
	}
	
	
	private void setupInverse(XLinkContext ctx, TableDefinition tableDef, Query query, String field) {
		if(query == null) query = new AllQuery();
		for(String xshard : ctx.xshards) {
			CubeSearcher searcher = ctx.olap.getSearcher(tableDef.getAppDef(), xshard);
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
				add(val, counts[i]);
			}
		}
	}
	
}
