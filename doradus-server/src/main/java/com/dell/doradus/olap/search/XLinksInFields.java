package com.dell.doradus.olap.search;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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
    public static void updateXLinksInFields(Olap olap, OlapQueryRequest olapQuery, SearchResultList result) {
        for(SearchResult searchResult: result.results) {
            updateXLinksInFields(olap, olapQuery, searchResult);
        }
    }
    
    private static void updateXLinksInFields(Olap olap, OlapQueryRequest olapQuery, SearchResult searchResult) {
        FieldSet fieldSet = searchResult.fieldSet;
        for(String linkField: fieldSet.getLinks()) {
            FieldDefinition linkDef = fieldSet.tableDef.getFieldDef(linkField);
            if(linkDef.isLinkField()) {
                List<SearchResultList> list = searchResult.links.get(linkField);
                for(SearchResultList resultList: list) {
                    updateXLinksInFields(olap, olapQuery, resultList);
                }
                continue;
            }
            if(!linkDef.isXLinkField()) continue;
            int pos = 0;
            for(FieldSet fs: fieldSet.getLinks(linkField)) {
                String junction = linkDef.getXLinkJunction();
                String value = searchResult.scalars.get(junction);
                if(value == null) continue;
                Set<String> values = Utils.split(value, CommonDefs.MV_SCALAR_SEP_CHAR);
                SearchResultList result = searchForXlink(values, olap, fs, linkDef, olapQuery);
                List<SearchResultList> list = searchResult.links.get(linkField);
                list.set(pos++, result);
            }
        }
    }
    
    
    private static SearchResultList searchForXlink(Set<String> values, Olap olap, FieldSet fieldSet, FieldDefinition fieldDef, OlapQueryRequest olapQuery) {
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
        
        int pageSize = fieldSet.limit;
        if(pageSize <= 0 || pageSize > 100000) pageSize = 100000; // arbitrarily chosen limitation
        List<String> shards = olapQuery.getXShards(); // x-shards are used as shards for X-links
        
        xRequest.setTableDef(fieldDef.getInverseTableDef());
        xRequest.setFieldSet(fieldSet);
        xRequest.setQuery(query);
        xRequest.setPageSizeWithSkip(pageSize);
        xRequest.setShards(shards);
        xRequest.setXShards(shards);
        
        SearchResultList result = Searcher.search(olap, xRequest);
        
        return result;
    }
}
