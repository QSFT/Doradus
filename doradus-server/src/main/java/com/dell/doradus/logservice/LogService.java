package com.dell.doradus.logservice;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.olap.OlapBatch;
import com.dell.doradus.olap.io.BSTR;
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
import com.dell.doradus.service.db.Tenant;

public class LogService {
    public LogService() { }

    public void createApplication(Tenant tenant, String application) {
        DBService.instance().createStoreIfAbsent(tenant, application, true);
    }

    public void deleteApplication(Tenant tenant, String application) {
        DBService.instance().deleteStoreIfPresent(tenant, application);
    }

    public void addBatch(Tenant tenant, String application, OlapBatch batch) {
        if(batch.size() == 0) return;
        ChunkWriter writer = new ChunkWriter();
        byte[] data = writer.writeChunk(batch);
        String partition = batch.get(0).getId().substring(0, 10).replace("-", "");
        String uuid = UUID.randomUUID().toString();
        DBTransaction transaction = DBService.instance().startTransaction(tenant);
        transaction.addColumn(application, "partitions", partition, "");
        transaction.addColumn(application, "partitions_" + partition, uuid, "");
        for(BSTR field: writer.getFields()) {
            transaction.addColumn(application, "fields", field.toString(), "");
        }
        transaction.addColumn(application, partition, uuid, data);
        DBService.instance().commit(transaction);
    }
    
    public List<String> getPartitions(Tenant tenant, String application) {
        List<String> partitions = new ArrayList<>();
        Iterator<DColumn> it = DBService.instance().getAllColumns(tenant, application, "partitions");
        if(it == null) return partitions;
        while(it.hasNext()) {
            DColumn c = it.next();
            partitions.add(c.getName());
        }
        return partitions;
    }
    
    
    public SearchResultList search(Tenant tenant, String application, LogQuery logQuery) {
        TableDefinition tableDef = Searcher.getTableDef(tenant, application);
        
        Query query = DoradusQueryBuilder.Build(logQuery.getQuery(), tableDef);
        if(logQuery.getContinueAfter() != null) {
            throw new NotImplementedException();
        }
        if(logQuery.getContinueAt() != null) {
            throw new NotImplementedException();
        }
        if(logQuery.getSkip() > 0) {
            throw new NotImplementedException();
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
        List<String> partitions = getPartitions(tenant, application);
        for(String partition: partitions) {
            for(ChunkReader reader: getChunks(tenant, application, partition)) {
                for(int i = 0; i < reader.size(); i++) {
                    if(!QueryFilter.filter(query, reader, i)) continue; 
                    count++;
                    if(current == null) {
                        current = new LogEntry(fields, bSortDescending);
                    }
                    current.set(reader, i);
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
    
    public ChunkIterable getChunks(Tenant tenant, String application, String partition) {
        return new ChunkIterable(tenant, application, partition);
    }
}
