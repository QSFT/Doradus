package com.dell.doradus.spider2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dell.doradus.search.util.LRUSizeCache;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.Tenant;
import com.dell.doradus.spider2.jsonbuild.JMapNode;

public class Spider2 {
    private static final int MAX_CHUNK_SIZE = 8192;
    private static final Object schemaSync = new Object();
    private static final Map<String, Schema> m_schemaCache = new HashMap<>();
    //100MB chunks cache
    private static final LRUSizeCache<String, byte[]> m_chunksCache = new LRUSizeCache<>(0, 1024 * 1024 * 100);
    
    //private static Logger LOG = LoggerFactory.getLogger("Spider2");
    
    public Spider2() { }

    public void createApplication(Tenant tenant, String application) {
        DBService.instance().createStoreIfAbsent(tenant, application, true);
    }

    public void deleteApplication(Tenant tenant, String application) {
        DBService.instance().deleteStoreIfPresent(tenant, application);
    }
   
    public void addObjects(Tenant tenant, String application, String table, List<JMapNode> nodes) {
        if(nodes.size() == 0) return;
        List<S2Object> objects = new ArrayList<>();
        for(JMapNode node: nodes) {
            objects.add(new S2Object(node));
        }
        Collections.sort(objects);
        
        int position = 0;
        while(position < objects.size()) {
            S2Object obj = objects.get(position);
            Binary chunkId = null;
            synchronized(schemaSync) {
                Schema schema = readSchema(tenant, application, table);
                chunkId = schema.getChunkId(obj.getId());
            }
            synchronized(Locking.getLock(chunkId)) {

                Chunk chunk = null;
                
                String key = tenant.getKeyspace() + "/" + application + "/" + table + "/" + chunkId.toString();
                byte[] data = m_chunksCache.get(key);
                if(data != null) {
                    chunk = Chunk.fromByteArray(data);
                }
                else {
                    chunk = readChunk(tenant, application, table, chunkId);
                }
                
                Binary nextId = chunk.getNextId();
                if(!nextId.isEmpty() && nextId.compareTo(obj.getId()) <= 0) {
                    // it's not our chunk because someone
                    // modified the schema between locks. Retry with the same object
                    System.out.println("Retry");
                    continue;
                }
                ChunkBuilder chunkBuilder = new ChunkBuilder(chunk);
                while(position < objects.size()) {
                    obj = objects.get(position);
                    if(!nextId.isEmpty() && nextId.compareTo(obj.getId()) <= 0) break;
                    chunkBuilder.add(obj);
                    position++;
                }
                chunk = chunkBuilder.getChunk();
                flushChunk(tenant, application, table, chunk);
            }
        }
    }

    public Iterable<S2Object> objects(Tenant tenant, String application, String table) {
        return new TableIterable(this, tenant, application, table);
    }
    
    protected Chunk getObjects(Tenant tenant, String application, String table, Binary chunkId) {
        Chunk chunk = readChunk(tenant, application, table, chunkId);
        return chunk;
    }
    
    private void flushChunk(Tenant tenant, String application, String table, Chunk chunk) {
        if(chunk.size() <= MAX_CHUNK_SIZE) {
            writeChunk(tenant, application, table, chunk);
        }
        else {
            List<Chunk> subchunks = chunk.split(MAX_CHUNK_SIZE);
            List<Binary> chunkIds = new ArrayList<>(subchunks.size() - 1);
            for(int i = 1; i < subchunks.size(); i++) {
                Chunk c = subchunks.get(i);
                chunkIds.add(c.getChunkId());
                writeChunk(tenant, application, table, c);
            }
            synchronized(schemaSync) {
                Schema schema = readSchema(tenant, application, table);
                for(Binary id: chunkIds) {
                    schema.addId(id);
                }
                writeChunk(tenant, application, table, subchunks.get(0));
                writeSchema(tenant, application, table, schema);
            }
        }
    }
    
    private Schema readSchema(Tenant tenant, String store, String table) {
        String key = tenant.getKeyspace() + "/" + store + "/" + table;
        Schema schema = m_schemaCache.get(key);
        if(schema != null) return schema;
        
        DColumn column = DBService.instance().getColumn(tenant, store, "schema", table);
        if(column == null) {
            schema = new Schema();
            Chunk initialChunk = new Chunk(Binary.EMPTY, Binary.EMPTY);
            schema.addId(Binary.EMPTY);
            writeChunk(tenant, store, table, initialChunk);
            writeSchema(tenant, store, table, schema);
            return schema;
        }
        schema = Schema.fromByteArray(column.getRawValue());
        m_schemaCache.put(key, schema);
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
        Chunk chunk = Chunk.fromByteArray(column.getRawValue());
        return chunk;
    }
    
    private void writeChunk(Tenant tenant, String store, String table, Chunk chunk) {
        byte[] data = chunk.toByteArray();

        String key = tenant.getKeyspace() + "/" + store + "/" + table + "/" + chunk.getChunkId().toString();
        m_chunksCache.put(key, data, data.length + key.length() * 2);
        
        DBTransaction transaction = DBService.instance().startTransaction(tenant);
        transaction.addColumn(store, table + "_" + chunk.getChunkId(), "data", data);
        DBService.instance().commit(transaction);
    }

}
