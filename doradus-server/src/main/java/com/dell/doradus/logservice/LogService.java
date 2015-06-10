package com.dell.doradus.logservice;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.logservice.search.Aggregate;
import com.dell.doradus.logservice.search.QueryFilter;
import com.dell.doradus.logservice.search.Searcher;
import com.dell.doradus.logservice.store.BatchWriter;
import com.dell.doradus.logservice.store.ChunkMerger;
import com.dell.doradus.olap.OlapBatch;
import com.dell.doradus.olap.aggregate.AggregationResult;
import com.dell.doradus.olap.aggregate.MetricValueCount;
import com.dell.doradus.olap.aggregate.MetricValueSet;
import com.dell.doradus.olap.collections.strings.BstrSet;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.olap.store.IntList;
import com.dell.doradus.search.FieldSet;
import com.dell.doradus.search.SearchResultList;
import com.dell.doradus.search.aggregate.SortOrder;
import com.dell.doradus.search.parser.AggregationQueryBuilder;
import com.dell.doradus.search.parser.DoradusQueryBuilder;
import com.dell.doradus.search.query.Query;
import com.dell.doradus.search.util.HeapList;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.DRow;
import com.dell.doradus.service.db.Tenant;

public class LogService {
    public LogService() { }

    public void createApplication(Tenant tenant, String application) {
        //DBService.instance().createStoreIfAbsent(tenant, application, true);
    }

    public void createTable(Tenant tenant, String application, String table) {
        String store = application + "_" + table;
        DBService.instance().createStoreIfAbsent(tenant, store, true);
    }

    public void deleteApplication(Tenant tenant, String application) {
        //DBService.instance().deleteStoreIfPresent(tenant, application);
    }

    public void deleteTable(Tenant tenant, String application, String table) {
        String store = application + "_" + table;
        DBService.instance().deleteStoreIfPresent(tenant, store);
    }
    
    public void addBatch(Tenant tenant, String application, String table, OlapBatch batch) {
        String store = application + "_" + table;
        int size = batch.size();
        if(size == 0) return;
        int start = 0;
        BatchWriter writer = new BatchWriter();
        DBTransaction transaction = DBService.instance().startTransaction(tenant);
        while(start < size) {
            String day = batch.get(start).getId().substring(0, 10);
            int end = start + 1;
            while(end < size) {
                String nextDay = batch.get(end).getId().substring(0, 10);
                if(!nextDay.equals(day)) break;
                end++;
            }
            byte[] data = writer.writeChunk(batch, start, end - start);
            String partition = day.replace("-", "");
            String uuid = Utils.getUniqueId();
            ChunkInfo chunkInfo = new ChunkInfo();
            chunkInfo.set(partition, uuid, writer.getWriter());
            transaction.addColumn(store, "partitions", partition, "");
            transaction.addColumn(store, "partitions_" + partition, uuid, chunkInfo.getByteData());
            for(BSTR field: writer.getFields()) {
                transaction.addColumn(store, "fields", field.toString(), "");
            }
            transaction.addColumn(store, partition, uuid, data);
            
            
            start = end;
        }
        DBService.instance().commit(transaction);
    }
    
    public void deleteOldSegments(Tenant tenant, String application, String table, long removeBeforeTimestamp) {
        String store = application + "_" + table;
        String partitionToCompare = new DateFormatter().format(removeBeforeTimestamp).substring(0, 10).replace("-", ""); 
        List<String> partitions = getPartitions(tenant, application, table);
        DBTransaction transaction = null;
        for(String partition: partitions) {
            if(partition.compareTo(partitionToCompare) >= 0) continue;
            if(transaction == null) transaction = DBService.instance().startTransaction(tenant);
            transaction.deleteColumn(store, "partitions", partition);
            transaction.deleteRow(store, partition);
            transaction.deleteRow(store, "partitions_" + partition);
        }
        if(transaction != null) DBService.instance().commit(transaction);
    }
    

    public void mergePartition(Tenant tenant, String application, String table, String partition) {
        final int MERGE_SEGMENTS = 8192;
        final int MIN_MERGE_DOCS = 8192;
        final int MAX_MERGE_DOCS = 65536;
        
        String store = application + "_" + table;
        Iterator<DColumn> it = DBService.instance().getAllColumns(tenant, store, "partitions_" + partition);
        if(it == null) return;
        List<ChunkInfo> infos = new ArrayList<ChunkInfo>(MERGE_SEGMENTS);
        int totalSize = 0;
        ChunkInfo info = new ChunkInfo();
        ChunkMerger merger = null;
        while(it.hasNext()) {
            DColumn c = it.next();
            info.set(partition, c.getName(), c.getRawValue());
            int eventsCount = info.getEventsCount();
            if(eventsCount > MIN_MERGE_DOCS) continue;
            if(totalSize + eventsCount > MAX_MERGE_DOCS || infos.size() == MAX_MERGE_DOCS) {
                if(merger == null) merger = new ChunkMerger(this, tenant, application, table);
                mergeChunks(infos, merger);
                infos.clear();
                totalSize = 0;
            }
            infos.add(new ChunkInfo(info));
            totalSize += eventsCount;
        }
        if(totalSize >= MIN_MERGE_DOCS) {
            if(merger == null) merger = new ChunkMerger(this, tenant, application, table);
            mergeChunks(infos, merger);
            infos.clear();
            totalSize = 0;
        }
    }
    
    private void mergeChunks(List<ChunkInfo> infos, ChunkMerger merger) {
        byte[] data = merger.mergeChunks(infos);
        String store = merger.getApplication() + "_" + merger.getTable();
        String partition = infos.get(0).getPartition();
        String uuid = Utils.getUniqueId();
        ChunkInfo chunkInfo = new ChunkInfo();
        chunkInfo.set(partition, uuid, merger.getWriter());
        DBTransaction transaction = DBService.instance().startTransaction(merger.getTenant());
        transaction.addColumn(store, "partitions_" + partition, uuid, chunkInfo.getByteData());
        transaction.addColumn(store, partition, uuid, data);
        for(ChunkInfo info: infos) {
            transaction.deleteColumn(store, "partitions_" + partition, info.getChunkId());
            transaction.deleteColumn(store, partition, info.getChunkId());
        }
        DBService.instance().commit(transaction);
    }
    
    public List<String> getPartitions(Tenant tenant, String application, String table) {
        String store = application + "_" + table;
        List<String> partitions = new ArrayList<>();
        Iterator<DColumn> it = DBService.instance().getAllColumns(tenant, store, "partitions");
        if(it == null) return partitions;
        while(it.hasNext()) {
            DColumn c = it.next();
            partitions.add(c.getName());
        }
        return partitions;
    }

    public List<String> getPartitions(Tenant tenant, String application, String table, String minDate, String maxDate) {
        String store = application + "_" + table;
        if(minDate != null) minDate = minDate.substring(0, 10).replace("-", "");
        if(maxDate != null) maxDate = maxDate.substring(0, 10).replace("-", "") + '\0';
        List<String> partitions = new ArrayList<>();
        Iterator<DColumn> it = DBService.instance().getColumnSlice(tenant, store, "partitions", minDate, maxDate);
        if(it == null) return partitions;
        while(it.hasNext()) {
            DColumn c = it.next();
            partitions.add(c.getName());
        }
        return partitions;
    }
    
    public void readChunk(Tenant tenant, String application, String table, ChunkInfo chunkInfo, ChunkReader chunkReader) {
        String store = application + "_" + table;
        DColumn column = DBService.instance().getColumn(tenant, store, chunkInfo.getPartition(), chunkInfo.getChunkId());
        if(column == null) throw new RuntimeException("Data was deleted");
        chunkReader.read(column.getRawValue());
    }

    public List<byte[]> readChunks(Tenant tenant, String application, String table, List<ChunkInfo> infos) {
        String store = application + "_" + table;
        List<String> chunkIds = new ArrayList<>(infos.size());
        for(ChunkInfo info: infos) chunkIds.add(info.getChunkId());
        List<String> rows = new ArrayList<>(1);
        rows.add(infos.get(0).getPartition());
        List<byte[]> data = new ArrayList<>(infos.size());
        int SIZE = 100;
        for(int start = 0; start < infos.size(); start += SIZE) {
            int end = Math.min(start + SIZE, infos.size());
            Iterator<DRow> rowIt = DBService.instance().getRowsColumns(tenant, store, rows, chunkIds.subList(start, end));
            if(rowIt == null || !rowIt.hasNext()) throw new RuntimeException("Error merging data");
            DRow drow = rowIt.next();
            Iterator<DColumn> colIt = drow.getColumns();
            if(colIt == null) throw new RuntimeException("Error merging data");
            while(colIt.hasNext()) {
                DColumn c = colIt.next();
                data.add(c.getRawValue());
            }
        }
        if(data.size() != infos.size()) throw new RuntimeException("Error merging data");
        return data;
    }
    
    
    public SearchResultList search(Tenant tenant, String application, String table, LogQuery logQuery) {
        TableDefinition tableDef = Searcher.getTableDef(tenant, application, table);
        
        Query query = DoradusQueryBuilder.Build(logQuery.getQuery(), tableDef);
        if(logQuery.getContinueAfter() != null) {
            throw new RuntimeException("Not implemented");
        }
        if(logQuery.getContinueAt() != null) {
            throw new RuntimeException("Not implemented");
        }
        if(logQuery.getSkip() > 0) {
            throw new RuntimeException("Not implemented");
        }
        int size = logQuery.getPageSizeWithSkip();
        FieldSet fieldSet = new FieldSet(tableDef, logQuery.getFields());
        fieldSet.expand();
        BSTR[] fields = Searcher.getFields(fieldSet);
        SortOrder[] sortOrders = AggregationQueryBuilder.BuildSortOrders(logQuery.getSortOrder(), tableDef);
        boolean bSortDescending = Searcher.isSortDescending(sortOrders);
        LogEntry current = null;
        HeapList<LogEntry> heap = new HeapList<>(size);
        int count = 0;
        List<String> partitions = getPartitions(tenant, application, table);
        ChunkReader chunkReader = new ChunkReader();
        for(String partition: partitions) {
            for(ChunkInfo chunkInfo: getChunks(tenant, application, table, partition)) {
                readChunk(tenant, application, table, chunkInfo, chunkReader);
                for(int i = 0; i < chunkReader.size(); i++) {
                    if(!QueryFilter.filter(query, chunkReader, i)) continue; 
                    count++;
                    if(current == null) {
                        current = new LogEntry(fields, bSortDescending);
                    }
                    current.set(chunkReader, i);
                    current = heap.AddEx(current);
                }
            }
        }
        
        SearchResultList list = new SearchResultList();
        list.documentsCount = count;
        LogEntry[] entries = heap.GetValues(LogEntry.class);
        for(LogEntry e: entries) {
            list.results.add(e.createSearchResult(fieldSet));
        }
        
        return list;
    }
    
    public AggregationResult aggregate(Tenant tenant, String application, String table, LogAggregate logAggregate) {
        TableDefinition tableDef = Searcher.getTableDef(tenant, application, table);
        Query query = DoradusQueryBuilder.Build(logAggregate.getQuery(), tableDef);
        String field = Aggregate.getAggregateField(tableDef, logAggregate.getFields());
        
        if(field == null) {
            int count = 0;
            List<String> partitions = getPartitions(tenant, application, table);
            ChunkReader chunkReader = new ChunkReader();
            for(String partition: partitions) {
                for(ChunkInfo chunkInfo: getChunks(tenant, application, table, partition)) {
                    readChunk(tenant, application, table, chunkInfo, chunkReader);
                    for(int i = 0; i < chunkReader.size(); i++) {
                        if(!QueryFilter.filter(query, chunkReader, i)) continue; 
                        count++;
                    }
                }
            }
            
            AggregationResult result = new AggregationResult();
            result.documentsCount = count;
            result.summary = new AggregationResult.AggregationGroup();
            result.summary.id = null;
            result.summary.name = "*";
            result.summary.metricSet = new MetricValueSet(1);
            MetricValueCount c = new MetricValueCount();
            c.metric = count;
            result.summary.metricSet.values[0] = c; 
            return result;
        }
        else {
            IntList list = new IntList();
            BstrSet fields = new BstrSet();
            BSTR temp = new BSTR();
            int count = 0;
            List<String> partitions = getPartitions(tenant, application, table);
            ChunkReader chunkReader = new ChunkReader();
            for(String partition: partitions) {
                for(ChunkInfo chunkInfo: getChunks(tenant, application, table, partition)) {
                    readChunk(tenant, application, table, chunkInfo, chunkReader);
                    int index = chunkReader.getFieldIndex(new BSTR(field));
                    if(index < 0) continue;
                    for(int i = 0; i < chunkReader.size(); i++) {
                        if(!QueryFilter.filter(query, chunkReader, i)) continue;
                        chunkReader.getFieldValue(i, index, temp);
                        int pos = fields.add(temp);
                        if(pos == list.size()) list.add(1);
                        else list.set(pos, list.get(pos) + 1);
                        count++;
                    }
                }
            }
            
            AggregationResult result = new AggregationResult();
            result.documentsCount = count;
            result.summary = new AggregationResult.AggregationGroup();
            result.summary.id = null;
            result.summary.name = "*";
            result.summary.metricSet = new MetricValueSet(1);
            MetricValueCount c = new MetricValueCount();
            c.metric = count;
            result.summary.metricSet.values[0] = c;
            for(int i = 0; i < fields.size(); i++) {
                AggregationResult.AggregationGroup g = new AggregationResult.AggregationGroup();
                g.id = fields.get(i).toString();
                g.name = g.id.toString();
                g.metricSet = new MetricValueSet(1);
                MetricValueCount cc = new MetricValueCount();
                cc.metric = list.get(i);
                g.metricSet.values[0] = cc;
                result.groups.add(g);
            }
            result.groupsCount = result.groups.size();
            Collections.sort(result.groups);
            return result;
            
        }
    }
    
    
    public ChunkIterable getChunks(Tenant tenant, String application, String table, String partition) {
        String store = application + "_" + table;
        return new ChunkIterable(tenant, store, partition);
    }
}
