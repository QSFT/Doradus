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
import com.dell.doradus.search.SearchResult;
import com.dell.doradus.search.SearchResultList;

public class Searcher {
    private static Logger LOG = LoggerFactory.getLogger("Olap.Searcher");

    public static SearchResultList search(Olap olap, ApplicationDefinition appDef, String table, OlapQuery olapQuery) {
        TableDefinition tableDef = appDef.getTableDef(table);
        if(tableDef == null) throw new IllegalArgumentException("Table " + table + " does not exist");
        OlapQueryRequest olapQueryRequest = new OlapQueryRequest(olap, tableDef, olapQuery);
        SearchResultList result = search(olap, olapQueryRequest);
        
        //metrics in query
        if(olapQuery.getMetrics() != null) {
            MetricsInSearch.addMetricsInSearch(olap, tableDef, result, olapQuery);
        }
        
        return result;
    }
    public static SearchResultList search(Olap olap, OlapQueryRequest olapQuery) {
        ApplicationDefinition appDef = olapQuery.getTableDef().getAppDef();
        XLinkContext xcontext = new XLinkContext(appDef.getAppName(), olap, olapQuery.getXShards(), olapQuery.getTableDef());
        xcontext.setupXLinkQuery(olapQuery.getTableDef(), olapQuery.getQuery());
        
        SearchResultList result =
                Olap.getSearchThreadPool() == null ?
                    searchSinglethreaded(olap, olapQuery):
                    searchMultithreaded(olap, olapQuery);
        
        if(olapQuery.getSkip() > 0) {
            int sz = result.results.size();
            result.results = new ArrayList<SearchResult>(result.results.subList(Math.min(olapQuery.getSkip(), sz), sz));
        }

        //xlinks in fields
        XLinksInFields.updateXLinksInFields(olap, olapQuery, result);
        
        return result;
    }
    
    private static SearchResultList searchSinglethreaded(Olap olap, OlapQueryRequest olapQuery) {
        List<SearchResultList> results = new ArrayList<SearchResultList>();
        for(String shard : olapQuery.getShards()) {
            results.add(search(olap, shard, olapQuery));
        }
        SearchResultList result = MergeResult.merge(results, olapQuery.getFieldSet());
        return result;
    }

    private static SearchResultList searchMultithreaded(Olap olap, OlapQueryRequest olapQuery) {
        try {
            final List<SearchResultList> results = new ArrayList<SearchResultList>();
            List<Future<?>> futures = new ArrayList<>();
            for(String shard : olapQuery.getShards()) {
                final Olap f_olap = olap;
                final String f_shard = shard;
                final OlapQueryRequest f_olapQuery = olapQuery;
                futures.add(Olap.getSearchThreadPool().submit(new Runnable() {
                    @Override public void run() {
                        SearchResultList r = search(f_olap, f_shard, f_olapQuery);
                        synchronized (results) {
                            results.add(r);
                        }
                                
                    }}));
            }
            for(Future<?> f: futures) f.get();
            futures.clear();
            SearchResultList result = MergeResult.merge(results, olapQuery.getFieldSet());
            return result;
        }catch(ExecutionException ee) {
            throw new RuntimeException(ee);
        }catch(InterruptedException ee) {
            throw new RuntimeException(ee);
        }
    }
    
    
    public static SearchResultList search(Olap olap, String shard, OlapQueryRequest olapQuery) {
        // repeat if segment was merged
        for(int i = 0; i <= 3; i++) {
            try {
                SearchResultList result = searchInternal(olap, shard, olapQuery);
                for(SearchResult sr: result.results) sr.scalars.put("_shard", shard);
                return result;
            }catch(FileDeletedException ex) {
                LOG.warn(ex.getMessage() + " - retrying: " + i);
                continue;
            }
        }
        throw new FileDeletedException("All retries failed");
    }
    
    private static SearchResultList searchInternal(Olap olap, String shard, OlapQueryRequest olapQuery) {
        ApplicationDefinition appDef = olapQuery.getTableDef().getAppDef();
        if(!olapQuery.getUncommitted()) {
            CubeSearcher s = olap.getSearcher(appDef, shard);
            SearchResultList result = search(s, olapQuery);
            return result;
        } else {
            List<SearchResultList> results = new ArrayList<SearchResultList>();
            for(String segment: olap.listSegments(appDef, shard)) {
                CubeSearcher searcher = olap.getSearcher(appDef, shard, segment);
                results.add(search(searcher, olapQuery));
            }
            SearchResultList result = MergeResult.merge(results, olapQuery.getFieldSet());
            return result;
        }
    }

	public static SearchResultList search(CubeSearcher searcher, OlapQueryRequest olapQuery) {
    	Result documents = ResultBuilder.search(olapQuery.getTableDef(), olapQuery.getQuery(), searcher);
		SearchResultList list = SearchResultBuilder.build(searcher, documents, olapQuery.getFieldSet(), olapQuery.getPageSizeWithSkip(), olapQuery.getSortOrder());
		return list;
	}
	
}
