package com.dell.doradus.spider2;

import java.util.ArrayList;
import java.util.HashMap;
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
        Schema schema = readSchema(tenant, application, table);
        
        HashMap<String, Chunk> chunks = new HashMap<>();
        for(JMapNode node: nodes) {
            S2Object obj = new S2Object(node);
            String chunkId = schema.getChunkId(obj.getId());
            Chunk chunk = chunks.get(chunkId);
            if(chunk == null) {
                chunk = readChunk(tenant, application, table, chunkId);
                chunks.put(chunkId, chunk);
            }
            chunk.add(obj);
        }
        
        boolean schemaModified = false;
        List<Chunk> modifiedChunks = new ArrayList<Chunk>();
        for(Chunk chunk: chunks.values()) {
            if(chunk.size() <= MAX_CHUNK_SIZE) {
                modifiedChunks.add(chunk);
            }
            else {
                schemaModified = true;
                List<Chunk> subchunks = chunk.split(MAX_CHUNK_SIZE);
                for(Chunk c: subchunks) {
                    schema.addId(c.getChunkId());
                    modifiedChunks.add(c);
                }
            }
        }
        for(Chunk c: modifiedChunks) {
            writeChunk(tenant, application, table, c);
        }
        if(schemaModified) writeSchema(tenant, application, table, schema);
    }

    public Iterable<S2Object> objects(Tenant tenant, String application, String table) {
        return new TableIterable(this, tenant, application, table);
    }
    
    protected List<S2Object> getObjects(Tenant tenant, String application, String table, String fromId) {
        Schema schema = readSchema(tenant, application, table);
        String chunkId = schema.getChunkIdAfter(fromId);
        if(chunkId == null) return null;
        Chunk chunk = readChunk(tenant, application, table, chunkId);
        List<S2Object> objects = new ArrayList<S2Object>(chunk.getObjects());
        return objects;
    }
    
    private Schema readSchema(Tenant tenant, String store, String table) {
        DColumn column = DBService.instance().getColumn(tenant, store, "schema", table);
        if(column == null) {
            Schema schema = new Schema();
            Chunk initialChunk = new Chunk("");
            schema.addId("");
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
    
    private Chunk readChunk(Tenant tenant, String store, String table, String chunkId) {
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
