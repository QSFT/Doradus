package com.dell.doradus.logservice.search;

import java.util.ArrayList;
import java.util.List;

import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.logservice.LogQuery;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.search.FieldSet;
import com.dell.doradus.search.aggregate.SortOrder;
import com.dell.doradus.search.parser.AggregationQueryBuilder;
import com.dell.doradus.search.parser.DoradusQueryBuilder;
import com.dell.doradus.search.query.AllQuery;
import com.dell.doradus.search.query.AndQuery;
import com.dell.doradus.search.query.Query;
import com.dell.doradus.search.query.RangeQuery;
import com.dell.doradus.service.db.Tenant;

public class SearchRequest {
    private LogQuery m_logQuery;
    private Tenant m_tenant;
    private TableDefinition m_tableDef;
    private Query m_query;
    private int m_count;
    private int m_skip;
    private FieldSet m_fieldSet;
    private BSTR[] m_fields;
    private SortOrder[] m_sortOrders;
    private boolean m_bSortDescending;
    
    private long m_minTimestamp;
    private long m_maxTimestamp;
    
    public SearchRequest(Tenant tenant, String application, String table, LogQuery logQuery) {
        m_logQuery = logQuery;
        m_tenant = tenant;
        m_tableDef = Searcher.getTableDef(tenant, application, table);
        fillRequest();
    }

    public LogQuery getLogQuery() { return m_logQuery; }
    public Tenant getTenant() { return m_tenant; }
    public TableDefinition getTableDef() { return m_tableDef; }
    public Query getQuery() { return m_query; }
    public int getCount() { return m_count; }
    public int getSkip() { return m_skip; }
    public FieldSet getFieldSet() { return m_fieldSet; }
    public BSTR[] getFields() { return m_fields; }
    public SortOrder[] getSortOrders() { return m_sortOrders; }
    public boolean getSortDescending() { return m_bSortDescending; }
    public long getMinTimestamp() { return m_minTimestamp; }
    public long getMaxTimestamp() { return m_maxTimestamp; }
    public boolean getSkipCount() { return m_logQuery.getSkipCount(); }
    
    
    private void fillRequest() {
        if(m_logQuery.getQuery() != null && !"*".equals(m_logQuery.getQuery())) {
            m_query = DoradusQueryBuilder.Build(m_logQuery.getQuery(), m_tableDef);
        }
        else m_query = new AllQuery();
        
        m_count = m_logQuery.getPageSizeWithSkip();
        Utils.require(m_count <= 10000000, "Page size is too large");
        m_skip = m_logQuery.getSkip();
        
        m_fieldSet = new FieldSet(m_tableDef, m_logQuery.getFields());
        m_fieldSet.expand();
        m_fields = SearchRequest.getFields(m_fieldSet);

        m_sortOrders = AggregationQueryBuilder.BuildSortOrders(m_logQuery.getSortOrder(), m_tableDef);
        m_bSortDescending = SearchRequest.isSortDescending(m_sortOrders);

        m_minTimestamp = 0;
        m_maxTimestamp = Long.MAX_VALUE;
        
        extractDates(m_query);
        
        if(m_bSortDescending) {
            if(m_logQuery.getContinueAfter() != null) {
                long time = Utils.parseDate(m_logQuery.getContinueAfter()).getTimeInMillis();
                m_maxTimestamp = Math.min(m_maxTimestamp, time);
            }
            if(m_logQuery.getContinueAt() != null) {
                long time = Utils.parseDate(m_logQuery.getContinueAt()).getTimeInMillis() + 1;
                m_maxTimestamp = Math.min(m_maxTimestamp, time);
            }
        } else {
            if(m_logQuery.getContinueAfter() != null) {
                long time = Utils.parseDate(m_logQuery.getContinueAfter()).getTimeInMillis() + 1;
                m_minTimestamp = Math.max(m_minTimestamp, time);
            }
            if(m_logQuery.getContinueAt() != null) {
                long time = Utils.parseDate(m_logQuery.getContinueAt()).getTimeInMillis();
                m_minTimestamp = Math.max(m_minTimestamp, time);
            }
        }
    }
    
    private void extractDates(Query q) {
          if(q == null) return;
          else if(q instanceof RangeQuery) {
              RangeQuery rq = (RangeQuery)q;
              if(!"Timestamp".equals(rq.field)) return;
              if(rq.min != null) {
                  long time = Utils.parseDate(rq.min).getTimeInMillis();
                  if(!rq.minInclusive)time++;
                  if(m_minTimestamp < time) m_minTimestamp = time;
              }
              if(rq.max != null) {
                  long time = Utils.parseDate(rq.max).getTimeInMillis();
                  if(rq.maxInclusive)time++;
                  if(m_maxTimestamp > time) m_maxTimestamp = time;
              }
          }
          else if(q instanceof AndQuery) {
              AndQuery aq = (AndQuery)q;
              for(Query subquery: aq.subqueries) {
                  extractDates(subquery);
              }
          }
    }
    
    
    public static BSTR[] getFields(FieldSet fieldSet) {
        List<BSTR> fields = new ArrayList<>(fieldSet.ScalarFields.size());
        for(String f: fieldSet.ScalarFields) {
            if("Timestamp".equals(f)) continue;
            fields.add(new BSTR(f));
        }
        return fields.toArray(new BSTR[fields.size()]);
    }
    
    public static boolean isSortDescending(SortOrder[] order) {
        if(order == null || order.length == 0) return false;
        Utils.require(order.length == 1, "Cannot sort by more than one value");
        Utils.require(order[0].items.size() == 1, "Cannot sort by link path");
        Utils.require("Timestamp".equals(order[0].items.get(0).name), "Only sort by timestamp is supported");
        return !order[0].ascending;
    }
    
    
}
