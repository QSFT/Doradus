package com.dell.doradus.logservice.search;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.FieldType;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.logservice.ChunkInfo;
import com.dell.doradus.logservice.ChunkReader;
import com.dell.doradus.logservice.LogAggregate;
import com.dell.doradus.logservice.LogEntry;
import com.dell.doradus.logservice.LogQuery;
import com.dell.doradus.logservice.LogService;
import com.dell.doradus.logservice.search.filter.FilterBuilder;
import com.dell.doradus.logservice.search.filter.IFilter;
import com.dell.doradus.olap.aggregate.AggregationResult;
import com.dell.doradus.search.SearchResultList;
import com.dell.doradus.search.aggregate.AggregationGroup;
import com.dell.doradus.search.parser.DoradusQueryBuilder;
import com.dell.doradus.search.query.Query;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.Tenant;

public class Searcher {
    
    public static SearchResultList search(LogService ls, Tenant tenant, String application, String table, LogQuery logQuery) {
        SearchRequest request = new SearchRequest(tenant, application, table, logQuery);
        SearchCollector collector = new SearchCollector(request.getCount());
        LogEntry current = null;
        List<String> partitions = ls.getPartitions(tenant, application, table, request.getMinTimestamp(), request.getMaxTimestamp());
        //optimization: inverse partitions
        if(request.getSkipCount() && request.getSortDescending()) {
            Collections.reverse(partitions);
        }
        IFilter filter = FilterBuilder.build(request.getQuery());
        ChunkReader chunkReader = new ChunkReader();
        int documentsCount = 0;
        for(String partition: partitions) {
            long minPartitionTimestamp = ls.getTimestamp(partition);
            long maxPartitionTimestamp = minPartitionTimestamp + 1000 * 3600 * 24;
            if(!checkInRange(minPartitionTimestamp, maxPartitionTimestamp, request, collector)) continue;
            Iterable<ChunkInfo> chunks = ls.getChunks(tenant, application, table, partition);
            chunks = new SortedChunkIterable(chunks, request.getSortDescending());
            for(ChunkInfo chunkInfo: chunks) {
                if(!checkInRange(chunkInfo.getMinTimestamp(), chunkInfo.getMaxTimestamp(), request, collector)) continue;
                ls.readChunk(tenant, application, table, chunkInfo, chunkReader);
                if(request.getSortDescending()) {
                    for(int i = chunkReader.size() - 1; i >= 0; i--) {
                        long timestamp = chunkReader.getTimestamp(i);
                        if(timestamp < request.getMinTimestamp()) continue;
                        if(timestamp >= request.getMaxTimestamp()) continue;
                        if(!filter.check(chunkReader, i)) continue;
                        //optimization: avoid instantiating LogEntry if it won't go to the results
                        if(collector.size() < request.getCount() || timestamp > collector.getMinTimestamp()) {
                            if(current == null) current = new LogEntry(request.getFields(), request.getSortDescending());
                            current.set(chunkReader, i);
                            current = collector.add(current);
                        }
                        documentsCount++;
                    }
                } else {
                    for(int i = 0; i < chunkReader.size(); i++) {
                        long timestamp = chunkReader.getTimestamp(i);
                        if(timestamp < request.getMinTimestamp()) continue;
                        if(timestamp >= request.getMaxTimestamp()) continue;
                        if(!filter.check(chunkReader, i)) continue; 
                        //optimization: avoid instantiating LogEntry if it won't go to the results
                        if(collector.size() < request.getCount() || timestamp < collector.getMaxTimestamp()) {
                            if(current == null) current = new LogEntry(request.getFields(), request.getSortDescending());
                            current.set(chunkReader, i);
                            current = collector.add(current);
                        }
                        documentsCount++;
                    }
                }
            }
        }
        
        SearchResultList list = collector.getSearchResult(request.getFieldSet(), request.getSortOrders());
        if(!request.getSkipCount()) list.documentsCount = documentsCount;
        if(list.results.size() == request.getCount()) list.continuation_token = list.results.get(list.results.size() - 1).id();
        if(request.getSkip() > 0) {
            int size = list.results.size();
            if(request.getSkip() >= size) list.results.clear();
            else list.results = new ArrayList<>(list.results.subList(request.getSkip(), size));
        }
        return list;
    }
    
    private static boolean checkInRange(long minTimestamp, long maxTimestamp, SearchRequest request, SearchCollector collector) {
        if(maxTimestamp < request.getMinTimestamp()) return false;
        if(minTimestamp >= request.getMaxTimestamp()) return false;
        if(request.getSkipCount() && collector.size() >= request.getCount()) {
            if(request.getSortDescending()) {
                if(maxTimestamp <= collector.getMinTimestamp()) return false;
            } else {
                if(minTimestamp >= collector.getMaxTimestamp()) return false;
            }
        }
        return true;
    }
    
    
    public static AggregationResult aggregate(LogService ls, Tenant tenant, String application, String table, LogAggregate logAggregate) {
        TableDefinition tableDef = Searcher.getTableDef(tenant, application, table);
        Query query = DoradusQueryBuilder.Build(logAggregate.getQuery(), tableDef);
        AggregationGroup group = Aggregate.getAggregationGroup(tableDef, logAggregate.getFields());
        String field = Aggregate.getAggregateField(group);
        IFilter filter = FilterBuilder.build(query);
        AggregateCollector collector = null;
        if(group != null && group.batchexFilters != null) {
            collector = new AggregateCollectorSets(filter, group.batchexFilters, group.batchexAliases);
        }
        else if(field == null) {
            collector = new AggregateCollectorNoField(filter);
        }
        else if("Timestamp".equals(field)) {
            collector = new AggregateCollectorTimestamp(filter, group.truncate, group.timeZone); 
        }
        else {
            collector = new AggregateCollectorField(filter, field);
        }
        collector.setContext(ls, tenant, application, table);
        
        List<String> partitions = ls.getPartitions(tenant, application, table);
        for(String partition: partitions) {
            for(ChunkInfo chunkInfo: ls.getChunks(tenant, application, table, partition)) {
                collector.addChunk(chunkInfo);
            }
        }
            
        AggregationResult result = collector.getResult();

        if(group != null) {
            Comparator<AggregationResult.AggregationGroup> comparer = AggregationGroupComparator.getComparator(group);
            Collections.sort(result.groups, comparer);
            if(group.selectionValue > 0 && group.selectionValue < result.groups.size()) {
                result.groups = new ArrayList<>(result.groups.subList(0, group.selectionValue));
            }
        }
        
        return result;
    }
    
    
    
    public static TableDefinition getTableDef(Tenant tenant, String application, String table) {
        String store = application + "_" + table;
        ApplicationDefinition appDef = new ApplicationDefinition();
        appDef.setAppName(application);
        TableDefinition tableDef = new TableDefinition(appDef, table);
        appDef.addTable(tableDef);
        FieldDefinition fieldDef = new FieldDefinition(tableDef);
        fieldDef.setType(FieldType.TIMESTAMP);
        fieldDef.setName("Timestamp");
        tableDef.addFieldDefinition(fieldDef);
        Iterator<DColumn> it = DBService.instance().getAllColumns(tenant, store, "fields");
        if(it != null) {
            while(it.hasNext()) {
                String field = it.next().getName();
                fieldDef = new FieldDefinition(tableDef);
                fieldDef.setType(FieldType.TEXT);
                fieldDef.setName(field);
                tableDef.addFieldDefinition(fieldDef);
            }
        }
        return tableDef;
    }
    
}
