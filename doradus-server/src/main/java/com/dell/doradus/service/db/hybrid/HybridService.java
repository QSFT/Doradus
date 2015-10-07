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

package com.dell.doradus.service.db.hybrid;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.service.db.ColumnDelete;
import com.dell.doradus.service.db.ColumnUpdate;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.RowDelete;
import com.dell.doradus.service.db.Tenant;


/**
 * Hybrid service that stores small columns one storage service (service-nosql parameter),
 * and large columns in another service (service-datastore parameter).
 * Column is large if its value is larger than number of bytes specified by value-threshold parameter (default is 1024).
 * 
 * Implementation notes:
 *   nosql service is the main one, so we first do any operation on datastore service,
 *   and, only after it succeeds, apply changes to the nosql store.
 *   
 *   if a value is large, is is replaced in nosql store by a placeholder which is a UUID.
 *   If the value is accidentally equal to the placeholder (should be extremely rare),
 *   it is copied to the datastore, so there will be no errors.
 *   
 *   If there is a already working database, it can be turned into nosql part of the HybridService anytime.
 *   Of course, the opposite change is not allowed: you cannot use one part or the hybrid service without the other.
 *   
 *   value-threshold affects how values are stored and is not used in reading, so it can be modified at any time, causing no errors. 
 * 
 */
public class HybridService extends DBService {
    // Random UUID. DO NOT CHANGE IT EVER! otherwise all current databases will stop working!
    private static final byte[] PLACEHOLDER = new byte[] {
        (byte)0xDD, (byte)0xB9, (byte)0x33, (byte)0xB0, (byte)0x97, (byte)0x34, (byte)0x41, (byte)0x8C,
        (byte)0xB5, (byte)0x78, (byte)0xD0, (byte)0x8A, (byte)0x36, (byte)0x0A, (byte)0xA2, (byte)0x34
    };
    
    private DBService m_serviceNosql;
    private DBService m_serviceDatastore;
    private int m_valueThreshold;
    protected final Logger m_logger = LoggerFactory.getLogger(getClass());
    
    private HybridService(Tenant tenant) {
        super(tenant);
    }

    public static HybridService instance(Tenant tenant) { 
        return new HybridService(tenant);
    }
    
    @Override public void initService() {
        String serviceNosqlName = getParamString("service-nosql");
        String serviceDatastoreName = getParamString("service-datastore");
        m_valueThreshold = getParamInt("value-threshold", 1024);
        
        try {
            Method instanceMethod;
            Class<? extends DBService> serviceNosqlClass = Class.forName(serviceNosqlName).asSubclass(DBService.class);
            instanceMethod = serviceNosqlClass.getMethod("instance", new Class<?>[]{Tenant.class});
            m_serviceNosql = (DBService)instanceMethod.invoke(null, new Object[]{getTenant()});
            Class<? extends DBService> serviceDatastoreClass = Class.forName(serviceDatastoreName).asSubclass(DBService.class);
            instanceMethod = serviceDatastoreClass.getMethod("instance", new Class<?>[]{Tenant.class});
            m_serviceDatastore = (DBService)instanceMethod.invoke(null, new Object[]{getTenant()});
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }

        m_serviceDatastore.initialize();
        m_serviceNosql.initialize();
        m_logger.info("Using Shared API");
        m_logger.info("NoSql service: {}", serviceNosqlName);
        m_logger.info("Datastore service: {}", serviceDatastoreName);
    }

    @Override public void startService() {
        m_serviceDatastore.start();
        m_serviceNosql.start();
    }
    
    @Override public void stopService() {
        m_serviceDatastore.stop();
        m_serviceNosql.stop();
    }
    
    @Override public void createNamespace() {
        m_serviceDatastore.createNamespace();
        m_serviceNosql.createNamespace();
    }

    @Override public void dropNamespace() {
        m_serviceDatastore.dropNamespace();
        m_serviceNosql.dropNamespace();
    }
    
    @Override public void createStoreIfAbsent(String storeName, boolean bBinaryValues) {
        m_serviceDatastore.createStoreIfAbsent(storeName, bBinaryValues);
        m_serviceNosql.createStoreIfAbsent(storeName, bBinaryValues);
    }
    
    @Override public void deleteStoreIfPresent(String storeName) {
        m_serviceDatastore.deleteStoreIfPresent(storeName);
        m_serviceNosql.deleteStoreIfPresent(storeName);
    }
    
    @Override public void commit(DBTransaction dbTran) {
        DBTransaction nosqlTransaction = new DBTransaction(dbTran.getTenant());
        DBTransaction datastoreTransaction = new DBTransaction(dbTran.getTenant());
        
        for(ColumnUpdate mutation: dbTran.getColumnUpdates()) {
            String storeName = mutation.getStoreName();
            String rowKey = mutation.getRowKey();
            String columnName = mutation.getColumn().getName();
            byte[] value = mutation.getColumn().getRawValue();
            // placeholder is a UUID so value is very unlikely equal to it.
            // But if it happens, we store it in datastore, so there will be no error. 
            if(value.length > m_valueThreshold || isPlaceholder(value)) {
                datastoreTransaction.addColumn(storeName, rowKey, columnName, value);
                nosqlTransaction.addColumn(storeName, rowKey, columnName, PLACEHOLDER);
            }
            else {
                nosqlTransaction.addColumn(storeName, rowKey, columnName, value);
            }
        }

        for(ColumnDelete mutation: dbTran.getColumnDeletes()) {
            String storeName = mutation.getStoreName();
            String rowKey = mutation.getRowKey();
            String columnName = mutation.getColumnName();
            datastoreTransaction.deleteColumn(storeName, rowKey, columnName);
            nosqlTransaction.deleteColumn(storeName, rowKey, columnName);
        }
        
        for(RowDelete mutation: dbTran.getRowDeletes()) {
            String storeName = mutation.getStoreName();
            String rowKey = mutation.getRowKey();
            datastoreTransaction.deleteRow(storeName, rowKey);
            nosqlTransaction.deleteRow(storeName, rowKey);
        }
        
        m_serviceDatastore.commit(datastoreTransaction);
        m_serviceNosql.commit(nosqlTransaction);
    }
    
    @Override public List<DColumn> getColumns(String storeName, String rowKey, String startColumn, String endColumn, int count) {
        List<DColumn> nosqlColumns = m_serviceNosql.getColumns(storeName, rowKey, startColumn, endColumn, count);
        List<DColumn> columns = processColumns(storeName, rowKey, nosqlColumns);
        return columns;
    }

    @Override
    public List<DColumn> getColumns(String storeName, String rowKey, Collection<String> columnNames) {
        List<DColumn> nosqlColumns = m_serviceNosql.getColumns(storeName, rowKey, columnNames);
        List<DColumn> columns = processColumns(storeName, rowKey, nosqlColumns);
        return columns;
    }

    @Override public List<String> getRows(String storeName, String continuationToken, int count) {
        return m_serviceNosql.getRows(storeName, continuationToken, count);
    }

    private List<DColumn> processColumns(String storeName, String rowKey, List<DColumn> nosqlColumns) {
        List<DColumn> columns = new ArrayList<>();
        List<String> colNames = new ArrayList<>();
        for(DColumn column: nosqlColumns) {
            byte[] value = column.getRawValue();
            if(isPlaceholder(value)) {
                colNames.add(column.getName());
            }
            else {
                columns.add(column);
            }
        }
        if(colNames.size() > 0) {
            List<DColumn> datastoreColumns = m_serviceDatastore.getColumns(storeName, rowKey, colNames);
            columns.addAll(datastoreColumns);
            Collections.sort(columns);
        }
        return columns;
    }
    
    private boolean isPlaceholder(byte[] value) {
        if(value.length != PLACEHOLDER.length) return false;
        for(int i = 0; i < PLACEHOLDER.length; i++) {
            if(value[i] != PLACEHOLDER[i]) return false;
        }
        return true;
    }
}
