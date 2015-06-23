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

package com.dell.doradus.olap.search;

import java.util.List;

import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.olap.Olap;
import com.dell.doradus.olap.OlapQuery;
import com.dell.doradus.search.FieldSet;
import com.dell.doradus.search.aggregate.SortOrder;
import com.dell.doradus.search.parser.AggregationQueryBuilder;
import com.dell.doradus.search.parser.DoradusQueryBuilder;
import com.dell.doradus.search.query.AndQuery;
import com.dell.doradus.search.query.IdRangeQuery;
import com.dell.doradus.search.query.Query;

public class OlapQueryRequest {
    private TableDefinition m_tableDef;
    private Query m_query;
    private int m_pageSizeWithSkip;
    private int m_skip;
    private FieldSet m_fieldSet;
    private SortOrder[] m_sortOrder;
    private List<String> m_shards;
    private List<String> m_xshards;
    
    public OlapQueryRequest() {}
    
    public OlapQueryRequest(Olap olap, TableDefinition tableDef, OlapQuery olapQuery) {
        m_tableDef = tableDef;
        olapQuery.fixPairParameter();
        m_query = DoradusQueryBuilder.Build(olapQuery.getQuery(), tableDef);
        if(olapQuery.getContinueAfter() != null) {
            m_query = new AndQuery(new IdRangeQuery(olapQuery.getContinueAfter(), false, null, false), m_query);
        }
        if(olapQuery.getContinueAt() != null) {
            m_query = new AndQuery(new IdRangeQuery(olapQuery.getContinueAt(), true, null, false), m_query);
        }
        m_fieldSet = new FieldSet(tableDef, olapQuery.getFieldSet());
        m_fieldSet.expand();
        m_sortOrder = AggregationQueryBuilder.BuildSortOrders(olapQuery.getSortOrder(), tableDef);
        m_shards = olapQuery.getShards(tableDef.getAppDef(), olap); 
        m_xshards = olapQuery.getXShards(tableDef.getAppDef(), olap);
        
        m_pageSizeWithSkip = olapQuery.getPageSizeWithSkip();
        m_skip = olapQuery.getSkip();
    }

    public TableDefinition getTableDef() { return m_tableDef; }
    public Query getQuery() { return m_query; }
    public int getPageSizeWithSkip() { return m_pageSizeWithSkip; }
    public int getSkip() { return m_skip; }
    public FieldSet getFieldSet() { return m_fieldSet; }
    public SortOrder[] getSortOrder() { return m_sortOrder; }
    public List<String> getShards() { return m_shards; }
    public List<String> getXShards() { return m_xshards; }

    public void setTableDef(TableDefinition tableDef) { m_tableDef = tableDef; }
    public void setQuery(Query query) { m_query = query; }
    public void setPageSizeWithSkip(int pageSizeWithSkip) { m_pageSizeWithSkip = pageSizeWithSkip; }
    public void setSkip(int skip) { m_skip = skip; }
    public void setFieldSet(FieldSet fieldSet) { m_fieldSet = fieldSet; }
    public void setSortOrder(SortOrder[] sortOrder) { m_sortOrder = sortOrder; }
    public void setShards(List<String> shards) { m_shards = shards; }
    public void setXShards(List<String> xshards) { m_xshards = xshards; }
    
}
