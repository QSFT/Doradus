package com.dell.doradus.logservice;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import com.dell.doradus.olap.OlapBatch;
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
    
    public ChunkIterable getChunks(Tenant tenant, String application, String partition) {
        return new ChunkIterable(tenant, application, partition);
    }
}
