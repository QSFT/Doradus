package spider3;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.logservice.LogQuery;
import com.dell.doradus.search.FieldSet;
import com.dell.doradus.search.SearchResult;
import com.dell.doradus.search.SearchResultList;
import com.dell.doradus.search.aggregate.SortOrder;
import com.dell.doradus.search.parser.AggregationQueryBuilder;
import com.dell.doradus.search.parser.DoradusQueryBuilder;
import com.dell.doradus.search.query.AllQuery;
import com.dell.doradus.search.query.Query;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.Tenant;

public class Spider3Search {

    public static SearchResultList search(TableDefinition tableDef, LogQuery query) {
        Query qu = new AllQuery();
        if(query.getQuery() != null && !"*".equals(query.getQuery())) {
            qu = DoradusQueryBuilder.Build(query.getQuery(), tableDef);
        }
        SortOrder[] sortOrders = AggregationQueryBuilder.BuildSortOrders(query.getSortOrder(), tableDef);
        DocSet set = Spider3Filter.search(tableDef, qu);
        SearchResultList list = new SearchResultList();
        for(String id: set.getIDs()) {
            SearchResult r = new SearchResult();
            r.scalars.put("_ID", id);
            r.orders = sortOrders;
            list.results.add(r);
        }

        Map<String, SearchResult> map = new HashMap<>();
        for(SearchResult sr: list.results) {
            map.put(sr.id(), sr);
        }

        if(sortOrders != null) {
            for(SortOrder o: sortOrders) {
                populateField(map, o.items.get(0).fieldDef);
            }
        }
        
        Collections.sort(list.results);
        list.documentsCount = list.results.size();
        int start = query.getSkip();
        int end = Math.min(list.results.size(), query.getPageSizeWithSkip());
        if(end < list.results.size()) {
            list.continuation_token = list.results.get(end - 1).id();
        }
        list.results = new ArrayList<>(list.results.subList(start, end));
        
        FieldSet fieldSet = new FieldSet(tableDef, query.getFields());
        fieldSet.expand();

        map.clear();
        for(SearchResult sr: list.results) {
            map.put(sr.id(), sr);
        }
        
        for(String field: fieldSet.ScalarFields) {
            populateField(map, tableDef.getFieldDef(field));
        }
        
        return list;
    }
    
    private static void populateField(Map<String, SearchResult> map, FieldDefinition fieldDef) {
        TableDefinition tableDef = fieldDef.getTableDef();
        ApplicationDefinition appDef = tableDef.getAppDef();
        Tenant tenant = Spider3.instance().getTenant(tableDef.getAppDef());
        String store = appDef.getAppName();
        String table = tableDef.getTableName();
        String field = fieldDef.getName();
        String row = table + "/" + field;
        if(fieldDef.isLinkField()) {
            // not supported yet
        }
        else if(fieldDef.isCollection()) {
            for(DColumn column: DBService.instance().getAllColumns(tenant, store, row)) {
                String[] nv = Spider3.split(column.getName());
                String id = nv[0];
                String value = nv[1];
                SearchResult r = map.get(id);
                if(r == null) continue;
                String v = r.scalars.get(field);
                if(v == null) r.scalars.put(field, value);
                else r.scalars.put(field, v + "\uFFFE" + value);
            }
        }
        else {
            for(DColumn column: DBService.instance().getAllColumns(tenant, store, row)) {
                SearchResult r = map.get(column.getName());
                if(r == null) continue;
                r.scalars.put(field, column.getValue());
            }
        }

    }
}
