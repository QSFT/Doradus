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

package com.dell.doradus.service.db;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import com.dell.doradus.common.Utils;
import com.dell.doradus.core.ServerParams;
import com.dell.doradus.service.Service;
import com.dell.doradus.service.tenant.TenantService;

/**
 * Managers the DBService objects used by the Doradus Server. Creates tenant-specific
 * DBServices as needed and maintains a map for reuse. When the DBManagerService is
 * started, it creates the DBService for the default database and waits for it to
 * connect. If the service throws a {@link DBNotAvailableException}, it keeps retrying
 * indefinitely. But when secondary DBServices are created, retry logic is not used so
 * to notify the caller that the DBService and therefore the tenant is not available. 
 */
public class DBManagerService extends Service {
    private final static DBManagerService INSTANCE = new DBManagerService();
    
    public static DBManagerService instance() { return INSTANCE; }
    
    // TODO: Make this an evaporative map in case tenants are deleted by another server.
    private final Map<String, DBService> m_tenantDBMap = new HashMap<>();
    
    private final int db_connect_retry_wait_millis;

    public DBManagerService() {
        db_connect_retry_wait_millis =
            ServerParams.instance().getModuleParamInt("DBService", "db_connect_retry_wait_millis", 5000);
    }
    
    //----- Service public methods
    
    @Override
    protected void initService() { }

    @Override
    protected void startService() {
        DBService dbservice = createDefaultDBService();
        Tenant tenant = dbservice.getTenant();
        m_tenantDBMap.put(tenant.getName(), dbservice);
    }

    @Override
    protected void stopService() {
        synchronized (m_tenantDBMap) {
            for (DBService dbservice : m_tenantDBMap.values()) {
                dbservice.stop();
            }
        }
    }

    //----- DBManagerService public methods
    
    /**
     * Get the DBService for the default database. This object is created when the
     * DBManagerService is started.
     * 
     * @return {@link DBService} for the defaultdatabase.
     */
    public DBService getDefaultDB() {
        String defaultTenantName = TenantService.instance().getDefaultTenantName();
        synchronized (m_tenantDBMap) {
            DBService dbservice = m_tenantDBMap.get(defaultTenantName);
            assert dbservice != null : "Database for default tenant not found";
            return dbservice;
        }
    }
    
    /**
     * Get the DBService for the given tenant. The DBService is created if necessary,
     * causing it to connect to its underlying DB. An exception is thrown if the DB cannot
     * be connected.
     * 
     * @param tenant    {@link Tenant} to get DBService for.
     * @return          {@link DBService} that manages data for that tenant.
     */
    public DBService getTenantDB(Tenant tenant) {
        synchronized (m_tenantDBMap) {
            DBService dbservice = m_tenantDBMap.get(tenant.getName());
            if (dbservice == null) {
                dbservice = createTenantDBService(tenant);
                m_tenantDBMap.put(tenant.getName(), dbservice);
            }
            return dbservice;
        }
    }
    
    /**
     * Get the information about all active tenants, which are those whose DBService has
     * been created. The is keyed by tenant name, and its value is a map of parameters
     * configured for the corresponding DBService.
     *  
     * @return  Map of information for all active tenants.
     */
    public SortedMap<String, SortedMap<String, Object>> getActiveTenantInfo() {
        SortedMap<String, SortedMap<String, Object>> activeTenantMap = new TreeMap<>();
        synchronized (m_tenantDBMap) {
            for (String tenantName : m_tenantDBMap.keySet()) {
                DBService dbservice = m_tenantDBMap.get(tenantName);
                Map<String, Object> dbServiceParams = dbservice.getAllParams();
                activeTenantMap.put(tenantName, new TreeMap<String, Object>(dbServiceParams));
            }
        }
        return activeTenantMap;
    }
    
    //----- Private methods
    
    // Create the default DBService object based on doradus.yaml file settings. If the DBService
    // throws a DBNotAvailableException, keep trying.
    private DBService createDefaultDBService() {
        String dbServiceName = ServerParams.instance().getModuleParamString("DBService", "dbservice");
        if (Utils.isEmpty(dbServiceName)) {
            throw new RuntimeException("'DBService.dbservice' parameter is not defined.");
        }

        DBService dbservice = null;
        Tenant defaultTenant = TenantService.instance().getDefaultTenant();
        boolean bDBOpened = false;
        while (!bDBOpened) {
            try {
                // Find and call the constructor DBService(Tenant).
                @SuppressWarnings("unchecked")
                Class<DBService> serviceClass = (Class<DBService>) Class.forName(dbServiceName);
                Constructor<DBService> constructor = serviceClass.getConstructor(Tenant.class);
                dbservice = constructor.newInstance(defaultTenant);
                dbservice.initialize();
                dbservice.start();
                bDBOpened = true;
            } catch (IllegalArgumentException e) {
                throw new RuntimeException("Cannot load specified 'dbservice': " + dbServiceName, e);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Could not load dbservice class '" + dbServiceName + "'", e);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("Required constructor missing for dbservice class: " + dbServiceName, e);
            } catch (SecurityException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException("Could not invoke constructor for dbservice class: " + dbServiceName, e);
            } catch (DBNotAvailableException e) {
                // This is the only one we can retry for the default DB.
                m_logger.info("Database is not reachable. Waiting to retry");
                try {
                    Thread.sleep(db_connect_retry_wait_millis);
                } catch (InterruptedException ex2) {
                    // ignore
                }
            } catch (Throwable e) {
                throw new RuntimeException("Failed to initialize default DBService: " + dbServiceName, e);
            }
        }
        return dbservice;
        
    }
    // Create a new DBService for the given tenant.
    private DBService createTenantDBService(Tenant tenant) {
        Map<String, Object> paramMap = tenant.getDefinition().getOptionMap("DBService");
        String dbServiceName = null;
        if (paramMap == null) {
            dbServiceName = ServerParams.instance().getModuleParamString("DBService", "dbservice");
            Utils.require(!Utils.isEmpty(dbServiceName), "DBService.dbservice parameter is not defined");
        } else {
            dbServiceName = paramMap.get("dbservice").toString();
            Utils.require(!Utils.isEmpty(dbServiceName),
                          "Tenant '%s' must define 'dbservice' within 'DBService' option", tenant.getName());
        }

        DBService dbservice = null;
        try {
            @SuppressWarnings("unchecked")
            Class<DBService> serviceClass = (Class<DBService>) Class.forName(dbServiceName);
            Constructor<DBService> constructor = serviceClass.getConstructor(Tenant.class);
            dbservice = constructor.newInstance(tenant);
            dbservice.initialize();
            dbservice.start();
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Cannot load specified 'dbservice': " + dbServiceName, e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Could not load dbservice class '" + dbServiceName + "'", e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Required constructor missing for dbservice class: " + dbServiceName, e);
        } catch (SecurityException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Could not invoke constructor for dbservice class: " + dbServiceName, e);
        } catch (Throwable e) {
            throw new RuntimeException("Failed to initialize DBService '" + dbServiceName +
                                       "' for tenant: " + tenant.getName(), e);
        }
        return dbservice;
    }

}
