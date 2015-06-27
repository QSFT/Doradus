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

package com.dell.doradus.service.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.CommonDefs;
import com.dell.doradus.common.ContentType;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;
import com.dell.doradus.core.DoradusServer;
import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.service.Service;
import com.dell.doradus.service.StorageService;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.DRow;
import com.dell.doradus.service.db.Tenant;
import com.dell.doradus.service.rest.RESTCommand;
import com.dell.doradus.service.rest.RESTService;
import com.dell.doradus.service.taskmanager.TaskManagerService;
import com.dell.doradus.service.tenant.TenantService;

/**
 * Provides common schema services for the Doradus server. The SchemaService parses new
 * and modified application schemas, add them to the Applications table, and notify the
 * appropriate storage service of the change.
 */
public class SchemaService extends Service {
    // Application ColumnFamily name:
    public static final String APPS_STORE_NAME = "Applications";
    
    // Application definition row column names:
    private static final String COLNAME_APP_SCHEMA = "_application";
    private static final String COLNAME_APP_SCHEMA_FORMAT = "_format";
    private static final String COLNAME_APP_SCHEMA_VERSION = "_version";

    // Singleton instance:
    private static final SchemaService INSTANCE = new SchemaService();
    
    // Current format version with which we store schema definitions:
    private static final int CURRENT_SCHEMA_LEVEL = 2;

    // REST commands supported by the SchemaService:
    private static final List<RESTCommand> REST_RULES = Arrays.asList(new RESTCommand[] {
        new RESTCommand("GET    /_applications                     com.dell.doradus.service.schema.ListApplicationsCmd"),
        new RESTCommand("GET    /_applications/{application}       com.dell.doradus.service.schema.ListApplicationCmd"),
        new RESTCommand("POST   /_applications                     com.dell.doradus.service.schema.DefineApplicationCmd"),
        new RESTCommand("PUT    /_applications/{application}       com.dell.doradus.service.schema.ModifyApplicationCmd"),
        new RESTCommand("DELETE /_applications/{application}       com.dell.doradus.service.schema.DeleteApplicationCmd"),
        new RESTCommand("DELETE /_applications/{application}/{key} com.dell.doradus.service.schema.DeleteApplicationCmd"),
    });

    //----- Service methods
    
    /**
     * Get the singleton instance of the StorageService. The object may or may not have
     * been initialized yet.
     * 
     * @return  The singleton instance of the StorageService.
     */ 
    public static SchemaService instance() {
        return INSTANCE;
    } // instance

    // Called once before startService. 
    @Override
    public void initService() {
        RESTService.instance().registerGlobalCommands(REST_RULES);
    }   // initService

    // Wait for the DB service to be up and check application schemas.
    @Override
    public void startService() {
        TenantService.instance().waitForFullService();
        TenantService.instance().createDefaultTenant();
        checkAppStores();
    }   // startService

    // Currently, we have nothing special to do to "stop".
    @Override
    public void stopService() {
    }   // stopService

    //----- Public SchemaService methods

    /**
     * Create the application with the given name in the default tenant. If the given
     * application already exists, the request is treated as an application update. If the
     * update is successfully validated, its schema is stored in the database, and the
     * appropriate storage service is notified to implement required physical database
     * changes, if any.
     * 
     * @param appDef    {@link ApplicationDefinition} of application to create or update.
     *                  Note that appDef is updated with the "Tenant" option.
     */
    public void defineApplication(ApplicationDefinition appDef) {
        checkServiceState();
        Tenant tenant = TenantService.instance().getDefaultTenant();
        defineApplication(tenant, appDef);
    }   // defineApplication

    /**
     * Create the application with the given name in the given Tenant. If the given
     * application already exists, the request is treated as an application update. If the
     * update is successfully validated, its schema is stored in the database, and the
     * appropriate storage service is notified to implement required physical database
     * changes, if any.
     * 
     * @param tenant    {@link Tenant} in which application is being created or updated.
     * @param appDef    {@link ApplicationDefinition} of application to create or update.
     *                  Note that appDef is updated with the "Tenant" option.
     */
    public void defineApplication(Tenant tenant, ApplicationDefinition appDef) {
        checkServiceState();
        setTenant(appDef, tenant);
        ApplicationDefinition currAppDef = checkApplicationKey(appDef);
        StorageService storageService = verifyStorageServiceOption(currAppDef, appDef);
        storageService.validateSchema(appDef);
        initializeApplication(currAppDef, appDef);
    }   // defineApplication
    
    /**
     * Return the {@link ApplicationDefinition} for all applications in the given Tenant.
     * 
     * @param  tenant   Tenant in which to query all applications.
     * @return          A collection of application definitions. 
     */
    public Collection<ApplicationDefinition> getAllApplications(Tenant tenant) {
        checkServiceState();
        return findAllApplications(tenant);
    }   // getAllApplications
    
    /**
     * Return the {@link ApplicationDefinition} for the application in the default tenant.
     * Null is returned if no application is found with the given name in the default
     * tenant.
     * 
     * @return The {@link ApplicationDefinition} for the given application or null if no
     *         no application such application is defined in the default tenant.
     *         
     * @deprecated  This method only works for the default tenant and hence only in
     *              single-tenant mode. {@link SchemaService#getApplication(Tenant, String)}
     *              should be used instead. 
     */
    public ApplicationDefinition getApplication(String appName) {
        checkServiceState();
        Tenant tenant = TenantService.instance().getDefaultTenant();
        return getApplicationDefinition(tenant, appName);
    }   // getApplication

    /**
     * Return the {@link ApplicationDefinition} for the application in the given tenant.
     * Null is returned if no application is found with the given name and tenant.
     * 
     * @return The {@link ApplicationDefinition} for the given application or null if no
     *         no such application is defined in the default tenant.
     */
    public ApplicationDefinition getApplication(Tenant tenant, String appName) {
        checkServiceState();
        return getApplicationDefinition(tenant, appName);
    }   // getApplication
    
    /**
     * Examine the given application's StorageService option and return the corresponding
     * {@link StorageService}. An error is thrown if the storage service is unknown or has
     * not been initialized.
     * 
     * @param   appDef  {@link ApplicationDefinition} of an application.
     * @return          The application's assigned {@link StorageService}.
     */
    public StorageService getStorageService(ApplicationDefinition appDef) {
        checkServiceState();
        String ssName = getStorageServiceOption(appDef);
        StorageService storageService = DoradusServer.instance().findStorageService(ssName);
        Utils.require(storageService != null, "StorageService is unknown or hasn't been initialized: " + ssName);
        return storageService;
    }   // getStorageService

    /**
     * Get the given application's StorageService option. If none is found, assign and
     * return the default. Unlike {@link #getStorageService(ApplicationDefinition)}, this
     * method will not throw an exception if the storage service is unknown or has not
     * been initialized.
     * 
     * @param   appDef  {@link ApplicationDefinition} of an application.
     * @return          The application's declared or assigned StorageService option.
     */
    public String getStorageServiceOption(ApplicationDefinition appDef) {
        String ssName = appDef.getOption(CommonDefs.OPT_STORAGE_SERVICE);
        if (Utils.isEmpty(ssName)) {
            ssName = DoradusServer.instance().getDefaultStorageService();
            appDef.setOption(CommonDefs.OPT_STORAGE_SERVICE, ssName);
        }
        return ssName;
    }   // getStorageServiceOption

    /**
     * Delete the given application, including all of its data, from the default tenant.
     * If the given application doesn't exist, the call is a no-op. WARNING: This method
     * deletes an application regardless of whether it has a key defined.
     * 
     * @param appName   Name of application to delete in default tenant.
     */
    public void deleteApplication(String appName) {
        checkServiceState();
        ApplicationDefinition appDef = getApplication(appName);
        if (appDef == null) {
            return; 
        }
        deleteApplication(appName, appDef.getKey());
    }   // deleteApplication
    
    /**
     * Delete the given application, including all of its data, from the default tenant.
     * If the given application doesn't exist, the call is a no-op. If the application
     * exists, the given key must match the current key, if one is defined, or be
     * null/empty if no key is defined.
     * 
     * @param appName   Name of application to delete in the default tenant.
     * @param key       Application key of existing application, if any.
     */
    public void deleteApplication(String appName, String key) {
        checkServiceState();
        ApplicationDefinition appDef = getApplication(appName);
        if (appDef == null) {
            return; 
        }
        deleteApplication(appDef, key);
    }   // deleteApplication
    
    /**
     * Delete the application with the given definition, including all of its data. The
     * given {@link ApplicationDefinition} must define the tenant in which the application
     * resides. If the given application doesn't exist, the call is a no-op. If the
     * application exists, the given key must match the current key, if one is defined, or
     * be null/empty if no key is defined.
     * 
     * @param appDef    {@link ApplicationDefinition} of application to delete.
     * @param key       Application key of existing application, if any.
     */
    public void deleteApplication(ApplicationDefinition appDef, String key) {
        checkServiceState();
        String appKey = appDef.getKey();
        if (Utils.isEmpty(appKey)) {
            Utils.require(Utils.isEmpty(key), "Application key does not match: %s", key);
        } else {
            Utils.require(appKey.equals(key), "Application key does not match: %s", key);
        }
        assert Tenant.getTenant(appDef) != null;
        
        // Delete storage service-specific data first.
        m_logger.info("Deleting application: {}", appDef.getAppName());
        StorageService storageService = getStorageService(appDef);
        storageService.deleteApplication(appDef);
        TaskManagerService.instance().deleteApplicationTasks(appDef);
        deleteAppProperties(appDef);
    }   // deleteApplication
    
    //----- Private methods

    // Singleton construction only
    private SchemaService() {}

    // Check to see if the storage manager is active for each application.
    private void checkAppStores() {
        m_logger.info("The following tenants and applications are defined:");
        Collection<Tenant> tenantList = TenantService.instance().getTenants();
        for (Tenant tenant : tenantList) {
            m_logger.info("   Tenant: {}", tenant.getKeyspace());
            Iterator<DRow> rowIter =
                DBService.instance().getAllRowsAllColumns(tenant, SchemaService.APPS_STORE_NAME);
            if (!rowIter.hasNext()) {
                m_logger.info("      <no applications>");
            }
            while (rowIter.hasNext()) {
                DRow row = rowIter.next();
                ApplicationDefinition appDef = loadAppRow(tenant, getColumnMap(row.getColumns()));
                if (appDef != null) {
                    String appName = appDef.getAppName();
                    String ssName = getStorageServiceOption(appDef);
                    m_logger.info("      Application '{}': StorageService={}; keyspace={}",
                                  new Object[]{appName, ssName, tenant.getKeyspace()});
                    if (DoradusServer.instance().findStorageService(ssName) == null) {
                        m_logger.warn("      >>>Application '{}' uses storage service '{}' which has not been " +
                                      "initialized; application will not be accessible via this server",
                                      appDef.getAppName(), ssName);
                    }
                }
            }
        }
        if (tenantList.size() == 0) {
            m_logger.info("   <no tenants>");
        }
    }
    
    // Delete the given application's schema row from the Applications CF.
    private void deleteAppProperties(ApplicationDefinition appDef) {
        Tenant tenant = Tenant.getTenant(appDef);
        DBTransaction dbTran = DBService.instance().startTransaction(tenant);
        dbTran.deleteRow(SchemaService.APPS_STORE_NAME, appDef.getAppName());
        DBService.instance().commit(dbTran);
    }   // deleteAppProperties
    
    // Initialize storage and store the given schema for the given new or updated application.
    private void initializeApplication(ApplicationDefinition currAppDef, ApplicationDefinition appDef) {
        Tenant tenant = Tenant.getTenant(appDef);
        if (tenant.getKeyspace().equals(ServerConfig.getInstance().keyspace)) {
            TenantService.instance().createDefaultTenant();
        }
        getStorageService(appDef).initializeApplication(currAppDef, appDef);
        storeApplicationSchema(appDef);
    }   // initializeApplication

    // Store the application row with schema, version, and format.
    private void storeApplicationSchema(ApplicationDefinition appDef) {
        String appName = appDef.getAppName();
        Tenant tenant = Tenant.getTenant(appDef);
        DBTransaction dbTran = DBService.instance().startTransaction(tenant);
        dbTran.addColumn(SchemaService.APPS_STORE_NAME, appName, COLNAME_APP_SCHEMA, appDef.toDoc().toJSON());
        dbTran.addColumn(SchemaService.APPS_STORE_NAME, appName, COLNAME_APP_SCHEMA_FORMAT, ContentType.APPLICATION_JSON.toString());
        dbTran.addColumn(SchemaService.APPS_STORE_NAME, appName, COLNAME_APP_SCHEMA_VERSION, Integer.toString(CURRENT_SCHEMA_LEVEL));
        DBService.instance().commit(dbTran);
    }   // storeApplicationSchema
    
    // Verify key match of an existing application, if any, and return it's definition. 
    private ApplicationDefinition checkApplicationKey(ApplicationDefinition appDef) {
        Tenant tenant = Tenant.getTenant(appDef);
        ApplicationDefinition currAppDef = getApplication(tenant, appDef.getAppName());
        if (currAppDef == null) {
            m_logger.info("Defining application: {}", appDef.getAppName());
        } else {
            m_logger.info("Updating application: {}", appDef.getAppName());
            String appKey = currAppDef.getKey();
            Utils.require(Utils.isEmpty(appKey) || appKey.equals(appDef.getKey()),
                          "Application key cannot be changed: %s", appDef.getKey());
        }
        return currAppDef;
    }   // checkApplicationKey
    
    // Set the given application's "Tenant" option to the given tenant's keyspace.
    private void setTenant(ApplicationDefinition appDef, Tenant tenant) {
        appDef.setOption(CommonDefs.OPT_TENANT, tenant.getKeyspace());
    }

    // Verify the given application's StorageService option and, if this is a schema
    // change, ensure it hasn't changed.  Return the application's StorageService object.
    private StorageService verifyStorageServiceOption(ApplicationDefinition currAppDef, ApplicationDefinition appDef) {
        // Verify or assign StorageService
        String ssName = getStorageServiceOption(appDef);
        StorageService storageService = getStorageService(appDef);
        Utils.require(storageService != null, "StorageService is unknown or hasn't been initialized: %s", ssName);
        
        // Currently, StorageService can't be changed.
        if (currAppDef != null) {
            String currSSName = getStorageServiceOption(currAppDef);
            Utils.require(currSSName.equals(ssName), "'StorageService' cannot be changed for application: %s", appDef.getAppName());
        }
        return storageService;
    }   // verifyStorageServiceOption

    private Map<String, String> getColumnMap(Iterator<DColumn> colIter) {
        Map<String, String> colMap = new HashMap<>();
        while (colIter.hasNext()) {
            DColumn col = colIter.next();
            colMap.put(col.getName(), col.getValue());
        }
        return colMap;
    }   // getColumnMap
    
    // Parse the application schema from the given application row.
    private ApplicationDefinition loadAppRow(Tenant tenant, Map<String, String> colMap) {
        ApplicationDefinition appDef = new ApplicationDefinition();
        String appSchema = colMap.get(COLNAME_APP_SCHEMA);
        if (appSchema == null) {
            return null;    // Not a real application definition row
        }
        String format = colMap.get(COLNAME_APP_SCHEMA_FORMAT);
        ContentType contentType = Utils.isEmpty(format) ? ContentType.TEXT_XML : new ContentType(format);
        String versionStr = colMap.get(COLNAME_APP_SCHEMA_VERSION);
        int schemaVersion = Utils.isEmpty(versionStr) ? CURRENT_SCHEMA_LEVEL : Integer.parseInt(versionStr);
        if (schemaVersion > CURRENT_SCHEMA_LEVEL) {
            m_logger.warn("Skipping schema with advanced version: {}", schemaVersion);
            return null;
        }
        try {
            appDef.parse(UNode.parse(appSchema, contentType));
        } catch (Exception e) {
            m_logger.warn("Error parsing schema for application '" + appDef.getAppName() + "'; skipped", e);
            return null;
        }
        setTenant(appDef, tenant);
        return appDef;
    }   // loadAppRow

    // Get the given application's application. If it's not in our app-to-tenant map,
    // refresh the map in case the application was just created.
    private ApplicationDefinition getApplicationDefinition(Tenant tenant, String appName) {
        Iterator<DColumn> colIter =
            DBService.instance().getAllColumns(tenant, SchemaService.APPS_STORE_NAME, appName);
        if (!colIter.hasNext()) {
            return null;
        }
        return loadAppRow(tenant, getColumnMap(colIter));
    }   // getApplicationDefinition

    // Get all application definitions for the given Tenant.
    private Collection<ApplicationDefinition> findAllApplications(Tenant tenant) {
        List<ApplicationDefinition> result = new ArrayList<>();
        Iterator<DRow> rowIter =
            DBService.instance().getAllRowsAllColumns(tenant, SchemaService.APPS_STORE_NAME);
        while (rowIter.hasNext()) {
            DRow row = rowIter.next();
            ApplicationDefinition appDef = loadAppRow(tenant, getColumnMap(row.getColumns()));
            if (appDef != null) {
                result.add(appDef);
            }
        }
        return result;
    }   // findAllApplications

}   // class SchemaService
