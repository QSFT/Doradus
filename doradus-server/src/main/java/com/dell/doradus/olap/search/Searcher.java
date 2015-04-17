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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.olap.Olap;
import com.dell.doradus.olap.OlapQuery;
import com.dell.doradus.olap.io.FileDeletedException;
import com.dell.doradus.olap.merge.MergeResult;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.olap.xlink.XLinkContext;
import com.dell.doradus.search.FieldSet;
import com.dell.doradus.search.SearchResult;
import com.dell.doradus.search.SearchResultList;
import com.dell.doradus.search.aggregate.SortOrder;
import com.dell.doradus.search.parser.AggregationQueryBuilder;
import com.dell.doradus.search.parser.DoradusQueryBuilder;
import com.dell.doradus.search.query.AndQuery;
import com.dell.doradus.search.query.IdRangeQuery;
import com.dell.doradus.search.query.Query;

public class Searcher {
    private static Logger LOG = LoggerFactory.getLogger("Olap.Searcher");

    public static SearchResultList search(Olap olap, ApplicationDefinition appDef, String table, OlapQuery olapQuery) {
        olapQuery.fixPairParameter();
        TableDefinition tableDef = appDef.getTableDef(table);
        String application = appDef.getAppName();
        if(tableDef == null) throw new IllegalArgumentException("Table " + table + " does not exist");
        Query query = DoradusQueryBuilder.Build(olapQuery.getQuery(), tableDef);
        if(olapQuery.getContinueAfter() != null) {
            query = new AndQuery(new IdRangeQuery(olapQuery.getContinueAfter(), false, null, false), query);
        }
        if(olapQuery.getContinueAt() != null) {
            query = new AndQuery(new IdRangeQuery(olapQuery.getContinueAt(), true, null, false), query);
        }
        FieldSet fieldSet = new FieldSet(tableDef, olapQuery.getFieldSet());
        fieldSet.expand();
        SortOrder[] sortOrders = AggregationQueryBuilder.BuildSortOrders(olapQuery.getSortOrder(), tableDef);
        List<String> shardsList = olapQuery.getShards(appDef, olap); 
        List<String> xshardsList = olapQuery.getXShards(appDef, olap); 
        XLinkContext xcontext = new XLinkContext(application, olap, xshardsList, tableDef);
        xcontext.setupXLinkQuery(tableDef, query);
        
        SearchResultList result =
                Olap.getSearchThreadPool() == null ?
                    searchSinglethreaded(olap, appDef, shardsList, tableDef, query, fieldSet, olapQuery, sortOrders):
                    searchMultithreaded(olap, appDef, shardsList, tableDef, query, fieldSet, olapQuery, sortOrders);
        
        if(olapQuery.getSkip() > 0) {
            int sz = result.results.size();
            result.results = new ArrayList<SearchResult>(result.results.subList(Math.min(olapQuery.getSkip(), sz), sz));
        }
        //metrics in query
        if(olapQuery.getMetrics() != null) {
            MetricsInSearch.addMetricsInSearch(olap, tableDef, result, olapQuery);
        }
        
        return result;
    }
    
    private static SearchResultList searchSinglethreaded(Olap olap, ApplicationDefinition appDef, List<String> shardsList, TableDefinition tableDef, Query query, FieldSet fieldSet, OlapQuery olapQuery, SortOrder[] sortOrders) {
        List<SearchResultList> results = new ArrayList<SearchResultList>();
        for(String shard : shardsList) {
            results.add(search(olap, appDef, shard, tableDef, query, fieldSet, olapQuery, sortOrders));
        }
        SearchResultList result = MergeResult.merge(results, fieldSet);
        return result;
    }

    private static SearchResultList searchMultithreaded(Olap olap, ApplicationDefinition appDef, List<String> shardsList, TableDefinition tableDef, Query query, FieldSet fieldSet, OlapQuery olapQuery, SortOrder[] sortOrders) {
        try {
            final List<SearchResultList> results = new ArrayList<SearchResultList>();
            List<Future<?>> futures = new ArrayList<>();
            for(String shard : shardsList) {
                final Olap f_olap = olap;
                final ApplicationDefinition f_appDef = appDef;
                final String f_shard = shard;
                final TableDefinition f_tableDef = tableDef;
                final Query f_query = query;
                final FieldSet f_fieldSet = fieldSet;
                final OlapQuery f_olapQuery = olapQuery;
                final SortOrder[] f_sortOrders = sortOrders;
                futures.add(Olap.getSearchThreadPool().submit(new Runnable() {
                    @Override public void run() {
                        SearchResultList r = search(f_olap, f_appDef, f_shard, f_tableDef, f_query, f_fieldSet, f_olapQuery, f_sortOrders);
                        synchronized (results) {
                            results.add(r);
                        }
                                
                    }}));
            }
            for(Future<?> f: futures) f.get();
            futures.clear();
            SearchResultList result = MergeResult.merge(results, fieldSet);
            return result;
        }catch(ExecutionException ee) {
            throw new RuntimeException(ee);
        }catch(InterruptedException ee) {
            throw new RuntimeException(ee);
        }
    }
    
    
    public static SearchResultList search(Olap olap, ApplicationDefinition appDef, String shard, TableDefinition tableDef, Query query, FieldSet fieldSet, OlapQuery olapQuery, SortOrder[] sortOrders) {
        // repeat if segment was merged
        for(int i = 0; i <= 2; i++) {
            try {
                CubeSearcher s = olap.getSearcher(appDef, shard);
                SearchResultList result = search(s, tableDef, query, fieldSet, olapQuery.getPageSizeWithSkip(), sortOrders);
                for(SearchResult sr: result.results) sr.scalars.put("_shard", shard);
                return result;
            }catch(FileDeletedException ex) {
                LOG.warn(ex.getMessage() + " - retrying: " + i);
                continue;
            }
        }
        CubeSearcher s = olap.getSearcher(appDef, shard);
        return Searcher.search(s, tableDef, query, fieldSet, olapQuery.getPageSizeWithSkip(), sortOrders);
    }
    
	public static SearchResultList search(CubeSearcher searcher, TableDefinition tableDef, Query query, FieldSet fieldSet, int size, SortOrder[] sortOrders) {
    	Result documents = ResultBuilder.search(tableDef, query, searcher);
		SearchResultList list = SearchResultBuilder.build(searcher, documents, fieldSet, size, sortOrders);
		return list;
	}
	
}
