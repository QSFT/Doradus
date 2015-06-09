package com.dell.doradus.persistence.query;

import java.util.HashMap;
import java.util.Map;

import com.dell.doradus.common.Utils;

public class QueryBuilder  {
	
	private String queryText;
	private String fieldNames;
	private int pageSize = -1;
	private String afterObjID;
	private String sortOrder;
	
	public QueryBuilder() {
		queryText = null;
		fieldNames = null;
		pageSize = -1;
		afterObjID = null;
		sortOrder = null;
	}
	
	public QueryBuilder query(String queryText) {
		this.queryText = queryText;
		return this;
	}
	public QueryBuilder fields(String fieldNames) {
		this.fieldNames = fieldNames;
		return this;
	}	
	public QueryBuilder pageSize(int pageSize) {
		this.pageSize = pageSize;
		return this;
	}	
	public QueryBuilder afterObjID(String afterObjID) {
		this.afterObjID = afterObjID;
		return this;
	}
	public QueryBuilder sortOrder(String sortOrder) {
		this.sortOrder = sortOrder;
		return this;
	}	
	
	public Map<String, String> toMap() {
        Map<String, String> params = new HashMap<>();
        if (!Utils.isEmpty(queryText)) {
            params.put("q", queryText);
        }
        if (!Utils.isEmpty(fieldNames)) {
            params.put("f", fieldNames);
        }
        if (pageSize >= 0) {
            params.put("s", Integer.toString(pageSize));
        }
        if (!Utils.isEmpty(afterObjID)) {
            params.put("g", afterObjID);
        }
        if (!Utils.isEmpty(sortOrder)) {
            params.put("o", sortOrder);
        }		
		return params;
	}
	
}
