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
import java.util.List;
import java.util.Map;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.CommonDefs;
import com.dell.doradus.common.ContentType;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;
import com.dell.doradus.core.Defs;
import com.dell.doradus.core.DoradusServer;
import com.dell.doradus.service.Service;
import com.dell.doradus.service.StorageService;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.rest.RESTCommand;
import com.dell.doradus.service.rest.RESTService;
import com.dell.doradus.service.taskmanager.TaskDBUtils;

/**
 * Provides common schema services for the Doradus server. The SchemaService parses new
 * and modified application schemas, add them to the Applications table, and notify the
 * appropriate storage service of the change.
 */
public class SchemaService extends Service {
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
        RESTService.instance().registerRESTCommands(REST_RULES);
    }   // initService

    // Wait for the DB service to be up and check application schemas.
    @Override
    public void startService() {
        DBService.instance().waitForFullService();
        checkAppStores();
    }   // startService

    // Currently, we have nothing special to do to "stop".
    @Override
    public void stopService() {
    }   // stopService

    //----- Public SchemaService methods

    /**
     * Create the application with the given name. If the given application already
     * exists, the request is treated as an application update. If the update is
     * successfully validated, its schema is stored in the database, and the
     * appropriate storage service is notified to implement required physical database
     * changes, if any.
     * 
     * @param appDef    {@link ApplicationDefinition} of application to create or update.
     */
    public void defineApplication(ApplicationDefinition appDef) {
        checkServiceState();
        ApplicationDefinition currAppDef = getApplication(appDef.getAppName());
        if (currAppDef == null) {
            m_logger.info("Defining application: {}", appDef.getAppName());
        } else {
            m_logger.info("Updating application: {}", appDef.getAppName());
        }
        StorageService storageService = verifyStorageServiceOption(currAppDef, appDef);
        storageService.validateSchema(appDef);
        storeApplicationSchema(appDef);
        storageService.initializeApplication(currAppDef, appDef);
    }   // defineApplication

    /**
     * Return a list of all {@link ApplicationDefinition}s for all applications registered
     * in the database.
     * 
     * @return A list of all registered applications. 
     */
    public List<ApplicationDefinition> getAllApplications() {
        checkServiceState();
        Map<String, Map<String, String>> rowColMap = DBService.instance().getAllAppProperties();
        List<ApplicationDefinition> result = new ArrayList<>();
        for (String appName : rowColMap.keySet()) {
            Map<String, String> colMap = rowColMap.get(appName);
            ApplicationDefinition appDef = loadAppRow(colMap);
            if (appDef != null) {
                result.add(appDef);
            }
        }
        return result;
    }   // getApplicationList
    
    /**
     * Return the {@link ApplicationDefinition} for the application with the given name.
     * Null is returned if no application is found with the given name.
     * 
     * @return The {@link ApplicationDefinition} for the given application or null if no
     *         such application is registered in the database. 
     */
    public ApplicationDefinition getApplication(String appName) {
        checkServiceState();
        Map<String, String> colMap = DBService.instance().getAppProperties(appName);
        if (colMap == null || colMap.size() == 0) {
            return null;
        }
        ApplicationDefinition appDef = new ApplicationDefinition();
        String appSchema = colMap.get(Defs.COLNAME_APP_SCHEMA);
        String format = colMap.get(Defs.COLNAME_APP_SCHEMA_FORMAT);
        ContentType contentType = Utils.isEmpty(format) ? ContentType.TEXT_XML : new ContentType(format);
        appDef.parse(UNode.parse(appSchema, contentType));
        return appDef;
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
     * Delete the given application, including all of its data. If the given application
     * doesn't exist, the call is a no-op.
     * 
     * @param appName   Name of application to delete.
     */
    public void deleteApplication(String appName) {
        checkServiceState();
        ApplicationDefinition appDef = SchemaService.instance().getApplication(appName);
        if (appDef == null) {
            return; 
        }
        
        // Delete storage service-specific data first.
        m_logger.info("Deleting application: {}", appName);
        StorageService storageService = getStorageService(appDef);
        storageService.deleteApplication(appDef);
        
        // Delete the application's schema and properties.
        deleteAppProperties(appDef);
        TaskDBUtils.deleteAppTasks(appDef.getAppName());
    }   // deleteApplication
    
    //----- Private methods

    // Singleton construction only
    private SchemaService() {}

    // Check to see if the storage manager is active for each application.
    private void checkAppStores() {
        Map<String, Map<String, String>> rowColMap = DBService.instance().getAllAppProperties();
        List<ApplicationDefinition> appList = new ArrayList<>();
        for (String appName : rowColMap.keySet()) {
            Map<String, String> colMap = rowColMap.get(appName);
            ApplicationDefinition appDef = loadAppRow(colMap);
            if (appDef != null) {
                appList.add(appDef);
            }
        }
        for (ApplicationDefinition appDef : appList) {
            String ssName = getStorageServiceOption(appDef);
            if (DoradusServer.instance().findStorageService(ssName) == null) {
                m_logger.warn("Application '{}' uses storage service '{}' which has not been " +
                              "initialized; application will not be accessible via this server",
                              appDef.getAppName(), ssName);
            }
        }
    }   // checkAppStores
    
    // Get the given application's StorageService option. If none is found, assign and
    // return the default.
    private String getStorageServiceOption(ApplicationDefinition appDef) {
        String ssName = appDef.getOption(CommonDefs.OPT_STORAGE_SERVICE);
        if (Utils.isEmpty(ssName)) {
            ssName = DoradusServer.instance().getDefaultStorageService();
            appDef.setOption(CommonDefs.OPT_STORAGE_SERVICE, ssName);
        }
        return ssName;
    }   // getStorageServiceOption

    // Delete the given application's schema row from the Applications CF.
    private void deleteAppProperties(ApplicationDefinition appDef) {
        DBTransaction dbTran = DBService.instance().startTransaction();
        dbTran.deleteAppRow(appDef.getAppName());
        DBService.instance().commit(dbTran);
    }   // deleteApplicationSchema
    
    // Store the application row with schema, version, and format.
    private void storeApplicationSchema(ApplicationDefinition appDef) {
        DBTransaction dbTran = DBService.instance().startTransaction();
        String appName = appDef.getAppName();
        dbTran.addAppColumn(appName, Defs.COLNAME_APP_SCHEMA, appDef.toDoc().toJSON());
        dbTran.addAppColumn(appName, Defs.COLNAME_APP_SCHEMA_FORMAT, ContentType.APPLICATION_JSON.toString());
        dbTran.addAppColumn(appName, Defs.COLNAME_APP_SCHEMA_VERSION, Integer.toString(CURRENT_SCHEMA_LEVEL));
        DBService.instance().commit(dbTran);
    }   // storeApplicationSchema
    
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

    // Parse the application schema from the given application row.
    private ApplicationDefinition loadAppRow(Map<String, String> colMap) {
        ApplicationDefinition appDef = new ApplicationDefinition();
        String appSchema = colMap.get(Defs.COLNAME_APP_SCHEMA);
        String format = colMap.get(Defs.COLNAME_APP_SCHEMA_FORMAT);
        ContentType contentType = Utils.isEmpty(format) ? ContentType.TEXT_XML : new ContentType(format);
        String versionStr = colMap.get(Defs.COLNAME_APP_SCHEMA_VERSION);
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
        return appDef;
    }   // loadAppRow
        
}   // class SchemaService
