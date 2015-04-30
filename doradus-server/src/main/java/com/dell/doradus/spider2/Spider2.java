package com.dell.doradus.spider2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.Tenant;
import com.dell.doradus.spider2.jsonbuild.JMapNode;

public class Spider2 {
    private static final int MAX_CHUNK_SIZE = 8192;
    //private static Logger LOG = LoggerFactory.getLogger("Spider2");
    
    public Spider2() { }

    public void createApplication(Tenant tenant, String application) {
        DBService.instance().createStoreIfAbsent(tenant, application, true);
    }

    public void deleteApplication(Tenant tenant, String application) {
        DBService.instance().deleteStoreIfPresent(tenant, application);
    }
   
    public void addObjects(Tenant tenant, String application, String table, List<JMapNode> nodes) {
        List<S2Object> objects = new ArrayList<>();
        for(JMapNode node: nodes) {
            objects.add(new S2Object(node));
        }
        Collections.sort(objects);
        
        Schema schema = readSchema(tenant, application, table);
        
        ChunkBuilder chunkBuilder = null;
        for(S2Object obj: objects) {
            Binary chunkId = schema.getChunkId(obj.getId());
            if(chunkBuilder != null && !chunkId.equals(chunkBuilder.getChunkId())) {
                flushChunk(tenant, application, table, chunkBuilder.getChunk(), schema);
                chunkBuilder = null;
            }
            if(chunkBuilder == null) {
                chunkBuilder = new ChunkBuilder(readChunk(tenant, application, table, chunkId));
            }
            chunkBuilder.add(obj);
        }
        if(chunkBuilder != null) {
            flushChunk(tenant, application, table, chunkBuilder.getChunk(), schema);
        }
    }

    private void flushChunk(Tenant tenant, String application, String table, Chunk chunk, Schema schema) {
        if(chunk.size() <= MAX_CHUNK_SIZE) {
            writeChunk(tenant, application, table, chunk);
        }
        else {
            List<Chunk> subchunks = chunk.split(MAX_CHUNK_SIZE);
            for(Chunk c: subchunks) {
                schema.addId(c.getChunkId());
                writeChunk(tenant, application, table, c);
            }
            writeSchema(tenant, application, table, schema);
        }
    }
    
    
    public Iterable<S2Object> objects(Tenant tenant, String application, String table) {
        return new TableIterable(this, tenant, application, table);
    }
    
    protected List<S2Object> getObjects(Tenant tenant, String application, String table, Binary fromId) {
        Schema schema = readSchema(tenant, application, table);
        Binary chunkId = schema.getChunkIdAfter(fromId);
        if(chunkId == null) return null;
        Chunk chunk = readChunk(tenant, application, table, chunkId);
        List<S2Object> objects = new ArrayList<S2Object>(chunk.getObjects());
        return objects;
    }
    
    private Schema readSchema(Tenant tenant, String store, String table) {
        DColumn column = DBService.instance().getColumn(tenant, store, "schema", table);
        if(column == null) {
            Schema schema = new Schema();
            Chunk initialChunk = new Chunk(Binary.EMPTY);
            schema.addId(Binary.EMPTY);
            writeChunk(tenant, store, table, initialChunk);
            writeSchema(tenant, store, table, schema);
            return schema;
        }
        Schema schema = Schema.fromByteArray(column.getRawValue());
        return schema;
    }

    private void writeSchema(Tenant tenant, String store, String table, Schema schema) {
        byte[] data = schema.toByteArray();
        DBTransaction transaction = DBService.instance().startTransaction(tenant);
        transaction.addColumn(store, "schema", table, data);
        DBService.instance().commit(transaction);
    }
    
    private Chunk readChunk(Tenant tenant, String store, String table, Binary chunkId) {
        DColumn column = DBService.instance().getColumn(tenant, store, table + "_" + chunkId, "data");
        Chunk chunk = Chunk.fromByteArray(chunkId, column.getRawValue());
        return chunk;
    }
    
    private void writeChunk(Tenant tenant, String store, String table, Chunk chunk) {
        byte[] data = chunk.toByteArray();
        DBTransaction transaction = DBService.instance().startTransaction(tenant);
        transaction.addColumn(store, table + "_" + chunk.getChunkId(), "data", data);
        DBService.instance().commit(transaction);
    }

}
