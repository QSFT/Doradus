package com.dell.doradus.logservice;

import java.util.ArrayList;
import java.util.List;

import com.dell.doradus.common.Utils;
import com.dell.doradus.logservice.search.Searcher;
import com.dell.doradus.logservice.store.BatchWriter;
import com.dell.doradus.logservice.store.ChunkMerger;
import com.dell.doradus.olap.OlapBatch;
import com.dell.doradus.olap.aggregate.AggregationResult;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.search.SearchResultList;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.DRow;
import com.dell.doradus.service.db.Tenant;

public class LogService {
    public LogService() { }

    public void createTable(Tenant tenant, String application, String table) {
        String store = application + "_" + table;
        DBService.instance().createStoreIfAbsent(tenant, store, true);
    }

    public void deleteTable(Tenant tenant, String application, String table) {
        String store = application + "_" + table;
        DBService.instance().deleteStoreIfPresent(tenant, store);
    }
    
    public String getPartition(long timestamp) {
        String date = new DateFormatter().format(timestamp);
        String partition = date.substring(0, 10).replace("-", "");
        return partition;
    }
    
    public long getTimestamp(String partition) {
        String date = partition.substring(0, 4) + "-" + partition.substring(4, 6) + "-" + partition.substring(6, 8);
        long timestamp = Utils.parseDate(date).getTimeInMillis();
        return timestamp;
    }
    
    public void addBatch(Tenant tenant, String application, String table, OlapBatch batch) {
        String store = application + "_" + table;
        int size = batch.size();
        if(size == 0) return;
        int start = 0;
        BatchWriter writer = new BatchWriter();
        DBTransaction transaction = DBService.instance().startTransaction(tenant);
        while(start < size) {
            String dateStr = batch.get(start).getId();
            Utils.parseDate(dateStr);
            String day = dateStr.substring(0, 10);
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
        String partitionToCompare = getPartition(removeBeforeTimestamp); 
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
        List<ChunkInfo> infos = new ArrayList<ChunkInfo>(MERGE_SEGMENTS);
        int totalSize = 0;
        ChunkInfo info = new ChunkInfo();
        ChunkMerger merger = null;
        for(DColumn c: DBService.instance().getAllColumns(tenant, store, "partitions_" + partition)) {
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
        for(DColumn c: DBService.instance().getAllColumns(tenant, store, "partitions")) {
            partitions.add(c.getName());
        }
        return partitions;
    }

    public List<String> getPartitions(Tenant tenant, String application, String table, long minTimestamp, long maxTimestamp) {
        long oneDayMillis = 1000 * 3600 * 24;
        String minPartition = minTimestamp == 0 ? "" : getPartition(minTimestamp);
        String maxPartition = maxTimestamp == Long.MAX_VALUE ? "z" : getPartition(maxTimestamp + oneDayMillis - 1);
        return getPartitions(tenant, application, table, minPartition, maxPartition);
    }
    
    public List<String> getPartitions(Tenant tenant, String application, String table, String fromPartition, String toPartition) {
        String store = application + "_" + table;
        List<String> partitions = new ArrayList<>();
        for(DColumn c: DBService.instance().getColumnSlice(tenant, store, "partitions", fromPartition, toPartition + '\0')) {
            partitions.add(c.getName());
        }
        return partitions;
    }
    
    public void readChunk(Tenant tenant, String application, String table, ChunkInfo chunkInfo, ChunkReader chunkReader) {
        byte[] data = readChunkData(tenant, application, table, chunkInfo);
        if(data == null) throw new RuntimeException("Data was deleted");
        chunkReader.read(data);
    }

    public byte[] readChunkData(Tenant tenant, String application, String table, ChunkInfo chunkInfo) {
        String store = application + "_" + table;
        DColumn column = DBService.instance().getColumn(tenant, store, chunkInfo.getPartition(), chunkInfo.getChunkId());
        if(column == null) return null;
        return column.getRawValue();
    }
    
    public List<byte[]> readChunks(Tenant tenant, String application, String table, List<ChunkInfo> infos) {
        String store = application + "_" + table;
        List<String> chunkIds = new ArrayList<>(infos.size());
        for(ChunkInfo info: infos) chunkIds.add(info.getChunkId());
        List<byte[]> data = new ArrayList<>(infos.size());
        DRow row = DBService.instance().getRow(tenant, store, infos.get(0).getPartition());
        for(DColumn c: row.getColumns(chunkIds, 100)) {
            data.add(c.getRawValue());
        }
        if(data.size() != infos.size()) throw new RuntimeException("Error reading data");
        return data;
    }
    
    
    public SearchResultList search(Tenant tenant, String application, String table, LogQuery logQuery) {
        return Searcher.search(this, tenant, application, table, logQuery);
    }
    
    public AggregationResult aggregate(Tenant tenant, String application, String table, LogAggregate logAggregate) {
        return Searcher.aggregate(this, tenant, application, table, logAggregate);
    }
    
    
    public ChunkIterable getChunks(Tenant tenant, String application, String table, String partition) {
        String store = application + "_" + table;
        return new ChunkIterable(tenant, store, partition);
    }
}
