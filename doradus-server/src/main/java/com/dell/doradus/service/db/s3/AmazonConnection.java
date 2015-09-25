/*
 * Copyright (C) 2014 Dell, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dell.doradus.service.db.s3;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.jets3t.service.S3Service;
import org.jets3t.service.ServiceException;
import org.jets3t.service.StorageObjectsChunk;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.model.StorageObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmazonConnection {
    protected final Logger m_logger = LoggerFactory.getLogger(getClass().getSimpleName());
    public static final int CHUNK_SIZE = 2048;
    private S3Service m_s3service;
    private long m_totalRequests = 0;
    private String BUCKET;

    AmazonConnection(S3Service service, String bucket) {
        m_s3service = service;
        BUCKET = bucket;
    }

    
    private void inc() {
        m_totalRequests++;
        if(m_totalRequests % 10000 == 0) {
            m_logger.info("Requests: {}", "" + (m_totalRequests / 1000) + "k");
        }
    }
    
    public List<ListItem> listAll(String path) {
        m_logger.debug("Start list all: " + path);
        try {
            List<ListItem> result = new ArrayList<>();
            String priorLastKey = null;
            while(true) {
                StorageObjectsChunk chunk = m_s3service.listObjectsChunked(BUCKET, path, "/", CHUNK_SIZE, priorLastKey);
                m_logger.trace("ListObjects: {}", path);
                inc();
                StorageObject[] objects = chunk.getObjects();
                for(int i = 0; i < objects.length; i++) {
                    String key = objects[i].getKey();
                    if(key.endsWith("/")) key = key.substring(0, key.length() - 1);
                    key = key.substring(path.length(), key.length());
                    ListItem item = new ListItem(key, objects[i].getContentLength() != 0);
                    result.add(item);
                }
                if(chunk.isListingComplete()) break;
                priorLastKey = chunk.getPriorLastKey();
            }
            return result;
       } catch (ServiceException e) {
            throw new RuntimeException(e);
        }
    }
    
    public void deleteAll(String path) {
        try {
            String priorLastKey = null;
            while(true) {
                StorageObjectsChunk chunk = m_s3service.listObjectsChunked(BUCKET, path, "?", CHUNK_SIZE, priorLastKey);
                m_logger.trace("ListObjects to delete: {}", path);
                inc();
                StorageObject[] objects = chunk.getObjects();
                if(objects.length == 0) break;
                String[] names = new String[objects.length];
                for(int i = 0; i < objects.length; i++) {
                    names[i] = objects[i].getKey();
                }
                m_s3service.deleteMultipleObjects(BUCKET, names);
                m_logger.trace("DeleteObjects: {}", objects.length);
                // do not inc() because delete requests are not counted
                
                if(chunk.isListingComplete()) break;
                priorLastKey = chunk.getPriorLastKey();
            }
        } catch (ServiceException e) {
            throw new RuntimeException(e);
        }
    }
    
    
    public byte[] get(String path) {
        try {
            S3Object obj = m_s3service.getObject(BUCKET, path);
            m_logger.trace("GetObject: {}", path);
            inc();
            if(obj == null) return null;
            int len = (int)obj.getContentLength();
            byte[] data = new byte[len];
            int start = 0;
            InputStream is = obj.getDataInputStream();
            while(start < len) {
                start += is.read(data, start, len - start);
            }
            is.close();
            return data;
        } catch (Exception e) {
            return null;
        }
    }
    
    public void put(String path, byte[] value) {
        try {
            S3Object obj = new S3Object(path, value);
            m_s3service.putObject(BUCKET, obj);
            m_logger.trace("PutObject: {}", path);
            inc();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public void delete(String path) {
        try {
            m_s3service.deleteObject(BUCKET, path);
            m_logger.trace("DeleteObject: {}", path);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}