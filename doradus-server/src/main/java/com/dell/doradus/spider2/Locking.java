package com.dell.doradus.spider2;

import java.util.HashMap;
import java.util.Map;

public class Locking {
    private static final Map<Binary, Object> m_chunksLock = new HashMap<>();
    
    public static Object getLock(Binary chunkId) {
        synchronized(m_chunksLock) {
            Object syncObject = m_chunksLock.get(chunkId);
            if(syncObject == null) {
                syncObject = new Object();
                m_chunksLock.put(chunkId, syncObject);
            }
            return syncObject;
        }
    }

}
