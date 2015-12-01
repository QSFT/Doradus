package com.dell.doradus.olap.search;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.common.CommonDefs;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.olap.Olap;
import com.dell.doradus.search.FieldSet;
import com.dell.doradus.search.SearchResult;
import com.dell.doradus.search.SearchResultList;
import com.dell.doradus.search.query.AndQuery;
import com.dell.doradus.search.query.BinaryQuery;
import com.dell.doradus.search.query.IdInQuery;
import com.dell.doradus.search.query.OrQuery;
import com.dell.doradus.search.query.Query;

public class XLinksInFields {
    private static Logger LOG = LoggerFactory.getLogger(XLinksInFields.class.getSimpleName());
	
    public static void updateXLinksInFields(Olap olap, OlapQueryRequest olapQuery, SearchResultList result) {
    	List<SearchResult> results = new ArrayList<>();
    	results.addAll(result.results);
    	updateXLinksInFields(olap, olapQuery, results);
    }

    private static void updateXLinksInFields(Olap olap, OlapQueryRequest olapQuery, List<SearchResult> results) {
    	if(results.size() == 0) return;
        FieldSet fieldSet = results.get(0).fieldSet;
        for(String linkField: fieldSet.getLinks()) {
            FieldDefinition linkDef = fieldSet.tableDef.getFieldDef(linkField);
            int pos = 0;
        	for(FieldSet fs: fieldSet.getLinks(linkField)) {
        		
                if(linkDef.isXLinkField()) {
                    String junction = linkDef.getXLinkJunction();
                    Set<String> keys = new HashSet<>();
                    for(SearchResult r: results) {
                        String value = r.scalars.get(junction);
                        if(value == null) continue;
                        Set<String> values = Utils.split(value, CommonDefs.MV_SCALAR_SEP_CHAR);
                        keys.addAll(values);
                    }
                    
                    Map<String, List<SearchResult>> map = searchForXlink(keys, olap, fs, linkDef, olapQuery);
                    
                    for(SearchResult r: results) {
                        String value = r.scalars.get(junction);
                        if(value == null) continue;
                        Set<String> values = Utils.split(value, CommonDefs.MV_SCALAR_SEP_CHAR);
                        SearchResultList list = new SearchResultList();
                        list.fieldSet = fs;
                        list.results = new ArrayList<>();
                        for(String key: values) {
                        	List<SearchResult> mappedResults = map.get(key);
                        	if(mappedResults == null) continue;
                        	list.results.addAll(mappedResults);
                        }
                        list.documentsCount = list.results.size();
                        r.links.get(linkField).set(pos, list);
                    }
                }
             
                
                if(linkDef.isLinkField() || linkDef.isXLinkField()) {
                    List<SearchResult> children = new ArrayList<>();
                    for(SearchResult result: results) {
                    	children.addAll(result.links.get(linkField).get(pos).results);
                    }
                    updateXLinksInFields(olap, olapQuery, children);
                }
                
                pos++;
        	}
        }
        

    }
    
    
    private static Map<String, List<SearchResult>> searchForXlink(Set<String> values, Olap olap, FieldSet fieldSet, FieldDefinition fieldDef, OlapQueryRequest olapQuery) {
    	LOG.info("Searching for xlink {} for {} values", fieldDef.getName(), values.size());
        OlapQueryRequest xRequest = new OlapQueryRequest();
        String refField = fieldDef.getInverseLinkDef().getXLinkJunction();
        Query query = null;
        if("_ID".equals(refField)) {
            query = new IdInQuery(new ArrayList<String>(values));
        }
        else {
            query = new OrQuery();
            for(String value: values) {
                ((OrQuery)query).subqueries.add(new BinaryQuery(BinaryQuery.EQUALS, refField, value));    
            }
        }
        
        if(fieldSet.filter != null) {
            query = new AndQuery(query, fieldSet.filter);
        }
        
        int pageSize = 1000000; // arbitrarily chosen limitation
        List<String> shards = olapQuery.getXShards(); // x-shards are used as shards for X-links
        if(!"_ID".equals(refField)) fieldSet.ScalarFields.add(refField);
        
        xRequest.setTableDef(fieldDef.getInverseTableDef());
        xRequest.setFieldSet(fieldSet);
        xRequest.setQuery(query);
        xRequest.setPageSizeWithSkip(pageSize);
        xRequest.setShards(shards);
        xRequest.setXShards(shards);
        xRequest.setUncommitted(olapQuery.getUncommitted());
        
        SearchResultList result = Searcher.search(olap, xRequest);
        
        Map<String, List<SearchResult>> map = new HashMap<>();
        for(SearchResult r: result.results) {
        	String key = r.scalars.get(refField);
        	if(key == null) continue;
            Set<String> keys = Utils.split(key, CommonDefs.MV_SCALAR_SEP_CHAR);
            for(String k: keys) {
            	List<SearchResult> value = map.get(k);
            	if(value == null) {
            		value = new ArrayList<>();
            		map.put(k, value);
            	}
            	value.add(r);
            }
        }
        
        return map;
    }
}
