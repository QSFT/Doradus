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

package com.dell.doradus.service.tenant;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;
import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.service.Service;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.DuplicateException;
import com.dell.doradus.service.db.Tenant;
import com.dell.doradus.service.db.UnauthorizedException;
import com.dell.doradus.service.rest.RESTCommand;
import com.dell.doradus.service.rest.RESTService;
import com.dell.doradus.service.schema.SchemaService;
import com.dell.doradus.service.taskmanager.TaskManagerService;

/**
 * Provides tenant management services such as creating new tenants, listing tenants, and
 * validating a user id/password for a given tenant.
 */
public class TenantService extends Service {
    // Singleton object required for the Service pattern:
    private static final TenantService INSTANCE = new TenantService();

    // Tenant definition row key/column names:
    private static final String TENANT_ROW_KEY = "_tenant";
    private static final String TENANT_DEF_COL_NAME = "Definition";
    
    // Tenant name (keyspace) to TenantDefinition map:
    private final Map<String, TenantDefinition> m_tenantMap = new HashMap<>();
    
    // REST commands supported by the SchemaService:
    private static final List<RESTCommand> REST_RULES = Arrays.asList(new RESTCommand[] {
        new RESTCommand("GET    /_tenants           com.dell.doradus.service.tenant.ListTenantsCmd", true),
        new RESTCommand("GET    /_tenants/{tenant}  com.dell.doradus.service.tenant.ListTenantCmd", true),
        new RESTCommand("POST   /_tenants           com.dell.doradus.service.tenant.DefineTenantCmd", true),
        new RESTCommand("PUT    /_tenants/{tenant}  com.dell.doradus.service.tenant.ModifyTenantCmd", true),
    });

    //----- Service methods
    
    /**
     * Get the singleton instance of the TenantService. The object may or may not have
     * been initialized yet.
     * 
     * @return  The singleton instance of the StorageService.
     */
    public static TenantService instance() {
        return INSTANCE;
    }
    
    @Override
    protected void initService() {
        if (ServerConfig.getInstance().multitenant_mode && !ServerConfig.getInstance().use_cql) {
            throw new RuntimeException("Multitenant_mode currently requires use_cql=true");
        }
        RESTService.instance().registerRESTCommands(REST_RULES);
    }

    @Override
    protected void startService() {
        DBService.instance().waitForFullService();
    }

    @Override
    protected void stopService() { }

    //----- TenantService services

    /**
     * Indicate if the given tenant exists. This means it's keyspace exists and, for
     * non-default tenants, a tenant definition exists.
     * 
     * @param tenant    Tenant to test.
     * @return          True if it exists and has been initialized.
     */
    public boolean tenantExists(Tenant tenant) {
        checkServiceState();
        synchronized (m_tenantMap) {
            if (m_tenantMap.keySet().contains(tenant.getKeyspace())) {
                return true;
            }
            refreshTenantMap();
            return m_tenantMap.keySet().contains(tenant.getKeyspace());
        }
    }   // tenantExists

    /**
     * Ensure that the default tenant exists, which requires no credentials to access.
     */
    public void createDefaultTenant() {
        checkServiceState();
        DBService dbService = DBService.instance();
        Tenant tenant = new Tenant(ServerConfig.getInstance().keyspace);
        dbService.createTenant(tenant, null);
        dbService.createStoreIfAbsent(tenant, SchemaService.APPS_STORE_NAME, false);
        dbService.createStoreIfAbsent(tenant, TaskManagerService.TASKS_STORE_NAME, false);
    }   // createDefaultTenant

    /**
     * Create a new tenant with the given definition and return the updated definition.
     * Throw a {@link DuplicateException} if the tenant has already been defined.
     * 
     * @param tenantDef {@link TenantDefinition} of new tenant.
     * @return          Updated definition.
     */
    public TenantDefinition defineTenant(TenantDefinition tenantDef) {
        checkServiceState();
        Utils.require(ServerConfig.getInstance().multitenant_mode,
                      "This command is not valid in single-tenant mode");
        String tenantName = tenantDef.getName();
        if (getTenantDef(tenantName) != null) {
            throw new DuplicateException("Tenant already exists: " + tenantName);
        }
        Utils.require(!tenantName.equals(ServerConfig.getInstance().keyspace),
                      "Cannot create a tenant with the default keyspace name: " + tenantName);
        defineNewTenant(tenantDef);
        return getTenantDef(tenantName);
    }   // defineTenant

    /**
     * Modify an existing tenant with the given definition and return the updated
     * definition. The only modification currently allowed is a password update.
     * 
     * @param tenantDef {@link TenantDefinition} of new tenant.
     * @return          Updated definition.
     */
    public TenantDefinition modifyTenant(TenantDefinition tenantDef) {
        checkServiceState();
        Utils.require(ServerConfig.getInstance().multitenant_mode,
                      "This command is not valid in single-tenant mode");
        // TODO
        throw new RuntimeException("Not yet implemented");
    }   // modifyTenant
    
    /**
     * Delete an existing tenant.
     * 
     * @param tenantName    Name of tenant to delete.
     */
    public void deleteTenant(String tenantName) {
        checkServiceState();
        Utils.require(ServerConfig.getInstance().multitenant_mode,
                      "This command is not valid in single-tenant mode");
        // TODO
        throw new RuntimeException("Not yet implemented");
    }   // deleteTenant
    
    /**
     * Validate that the given authorization header contains a valid userid/password of a
     * system user. The given authorization string must be the value of an "Authorization"
     * header in the format "Basic xxx" where xxx is the base64-encoded value of
     * "userid:password" (including the colon). If the user ID/password are not valid
     * system credentials, a {@link UnauthorizedException} exception is thrown. If the
     * user ID/password are valid, the default tenant is returned.
     * 
     * @param  authorizationHeader      Value from "Authorization" header or null.
     * @return
     * @throws UnauthorizedException
     */
    public Tenant validateSystemUser(String authorizationHeader) throws UnauthorizedException {
        checkServiceState();
        Utils.require(ServerConfig.getInstance().multitenant_mode,
                      "This command is not valid in single-tenant mode");
        Tenant tenant = validateSystemAuthString(authorizationHeader);
        if (tenant == null) {
            throw new UnauthorizedException("Unrecognized system user id/password");
        }
        return tenant;
    }   // validateSystemUser

    /**
     * Validate access to the given tenant name using the given user ID and password. If
     * the tenant name is unknown or the user ID/password is not valid for the tenant, an
     * {@link UnauthorizedException} exception is thrown.
     *  
     * @param  tenantName               Name of a tenant.  
     * @param  userid                   Candidate user ID.
     * @param  password                 Candidate user password. 
     * @return                          {@link Tenant} that represents specified tenant.
     * @throws UnauthorizedException    If the tenant name is unknown or the user ID or
     *                                  password is not valid.
     */
    public Tenant validateTenant(String tenantName, String userid, String password) throws UnauthorizedException {
        checkServiceState();
        Utils.require(ServerConfig.getInstance().multitenant_mode,
                      "This command is not valid in single-tenant mode");
        assert !Utils.isEmpty(tenantName);
        Tenant tenant = validateTenantUserPassword(tenantName, userid, password);
        if (tenant == null) {
            throw new UnauthorizedException("Unrecognized tenant user id/password");
        }
        return tenant;
    }   // validateTenant
    
    /**
     * Validate access to the given tenant name. The given authorization string must be
     * the value of an "Authorization" header in the format "Basic xxx" where xxx is the
     * base64-encoded value of "userid:password" (including the colon). If the tenant name
     * is unknown or the user ID/password is not valid for the tenant, an
     * {@link UnauthorizedException} exception is thrown.
     *  
     * @param  tenantName               Name of a tenant.
     * @param  authorizationHeader      Value from "Authorization" header or null.
     * @return                          {@link Tenant} that represents specified tenant.
     * @throws UnauthorizedException    If the tenant name is unknown or the userid or
     *                                  password is not valid.
     */
    public Tenant validateTenant(String tenantName, String authorizationHeader) throws UnauthorizedException {
        checkServiceState();
        Utils.require(ServerConfig.getInstance().multitenant_mode,
                      "This command is not valid in single-tenant mode");
        assert !Utils.isEmpty(tenantName);
        Tenant tenant = validateAuthString(tenantName, authorizationHeader);
        if (tenant == null) {
            throw new UnauthorizedException("Unrecognized tenant user id/password");
        }
        return tenant;
    }   // validateTenant
    
    /**
     * Return a {@link Tenant} that represents the default keyspace. This method performs
     * no validation of any kind.
     * 
     * @return  A Tenant object that can be used to access applications in the default
     *          keyspace.
     */
    public Tenant getDefaultTenant() {
        checkServiceState();
        return new Tenant(ServerConfig.getInstance().keyspace);
    }   // getDefaultTenant

    /**
     * Get the {@link TenantDefinition} of the tenant with the given name, if it exists.
     * 
     * @param tenantName    Candidate tenant name.
     * @return              Definition of tenant if it exists, otherwise null.
     */
    public TenantDefinition getTenantDefinition(String tenantName) {
        checkServiceState();
        Utils.require(ServerConfig.getInstance().multitenant_mode,
                      "This command is not valid in single-tenant mode");
        Tenant tenant = new Tenant(tenantName);
        return getTenantDefinition(tenant);
    }   // getTenantDefinition

    //----- Private methods

    // Define a new tenant
    private void defineNewTenant(TenantDefinition tenantDef) {
        DBService dbService = DBService.instance();
        Tenant tenant = new Tenant(tenantDef.getName());
        dbService.createTenant(tenant, tenantDef.getOptions());
        addTenantUsers(tenantDef);
        dbService.createStoreIfAbsent(tenant, SchemaService.APPS_STORE_NAME, false);
        dbService.createStoreIfAbsent(tenant, TaskManagerService.TASKS_STORE_NAME, false);
        storeTenantDefinition(tenantDef);
    }   // defineNewTenant

    // Add all users in the given tenant definition to Cassandra and authorize them to use
    // the corresponding keyspace.
    private void addTenantUsers(TenantDefinition tenantDef) {
        if (tenantDef.getUsers().size() == 0) {
            addDefaultUser(tenantDef);
        }
        // Prefix all user IDs with the "{tenant}_"
        String tenantName = tenantDef.getName();
        Tenant tenant = new Tenant(tenantName);
        Map<String, String> definedUsers = tenantDef.getUsers();
        Map<String,String> tenantUserMap = new HashMap<>();
        for (String user : definedUsers.keySet()) {
            String newUserID = tenantName + "_" + user;
            tenantUserMap.put(newUserID, definedUsers.get(user));
        }
        DBService.instance().addUsers(tenant, tenantUserMap);
    }   // addTenantUsers

    // Add a default user account for the given tenant definition.
    private void addDefaultUser(TenantDefinition tenantDef) {
        tenantDef.getUsers().put("U" + generateRandomID(), generateRandomID());
    }   // addDefaultUser

    // Generate a unique ID by choosing a positive random long value and converting into a
    // String in the maximum radix allowed (36). Hence a value such as 1364581319703000
    // becomes "dfpc05b00o". Note that these IDs might begin with a digit.
    private String generateRandomID() {
        return Long.toString(Math.abs(new Random().nextLong()), Character.MAX_RADIX);
    }

    // Store the given tenant definition in the "_tenants" row of the Applications table.
    private void storeTenantDefinition(TenantDefinition tenantDef) {
        Tenant tenant = new Tenant(tenantDef.getName());
        String tenantDefJSON = tenantDef.toDoc().toJSON();
        DBTransaction dbTran = DBService.instance().startTransaction(tenant);
        dbTran.addColumn(SchemaService.APPS_STORE_NAME, TENANT_ROW_KEY, TENANT_DEF_COL_NAME, tenantDefJSON);
        DBService.instance().commit(dbTran);
    }   // storeTenantDefinition

    // Validate the given Authorization header string as a system user.  
    private Tenant validateSystemAuthString(String authString) {
        Tenant tenant = null;
        if (Utils.isEmpty(authString)) {
            m_logger.debug("Validation failed for system user: no Authorization header");
        } else if (!authString.toLowerCase().startsWith("basic ")) {
            m_logger.debug("Validation failed for system user: unknown/unsupported authorization type: {}",
                           authString);
        } else {
            String decoded = Utils.base64ToString(authString.substring("basic ".length()));
            int inx = decoded.indexOf(':');
            String userid = inx < 0 ? decoded : decoded.substring(0, inx);
            String password = inx < 0 ? "" : decoded.substring(inx + 1);
            tenant = validateSystemUserPassword(userid, password);
        }
        return tenant;
    }   // validateSystemAuthString
    
    // Validate the given Authorization header string.  
    private Tenant validateAuthString(String tenantName, String authString) {
        Tenant tenant = null;
        if (Utils.isEmpty(authString)) {
            m_logger.debug("Validation failed for tenant '{}': no Authorization header", tenantName);
        } else if (!authString.toLowerCase().startsWith("basic ")) {
            m_logger.debug("Validation failed for tenant '{}': unknown/unsupported authorization type: {}",
                           tenantName, authString);
        } else {
            String decoded = Utils.base64ToString(authString.substring("basic ".length()));
            int inx = decoded.indexOf(':');
            String userid = inx < 0 ? decoded : decoded.substring(0, inx);
            String password = inx < 0 ? "" : decoded.substring(inx + 1);
            tenant = validateTenantUserPassword(tenantName, userid, password);
        }
        return tenant;
    }   // validateAuthString
    
    // Validate the given system user ID and password.
    private Tenant validateSystemUserPassword(String userid, String password) {
        if (isValidSystemCredentials(userid, password)) {
            return getDefaultTenant();
        }
        m_logger.debug("Validation failed for system user: invalid userid/password");
        return null;
    }   // validateSystemUserPassword
    
    // Return true if the given userid/password are valid system credentials.
    private boolean isValidSystemCredentials(String userid, String password) {
        // TODO: For now, use dbuser and dbpassword to access the default keyspace.
        return userid.equals(ServerConfig.getInstance().dbuser) &&
               password.equals(ServerConfig.getInstance().dbpassword);
    }   // isValidSystemCredentials
    
    // Validate the given user ID and password.
    private Tenant validateTenantUserPassword(String tenantName, String userid, String password) {
        // TODO: Probably temporary: allow system users to access all tenants
        if (isValidSystemCredentials(userid, password)) {
            return new Tenant(tenantName);
        }
        TenantDefinition tenantDef = getTenantDef(tenantName);
        if (tenantDef == null) {
            m_logger.debug("Validation failed for tenant '{}': unknown tenant", tenantName);
            return null;
        }
        if (!tenantDef.getUsers().containsKey(userid) ||
            !tenantDef.getUsers().get(userid).equals(password)) {
            m_logger.debug("Validation failed for tenant '{}': invalid userid/password", tenantName);
            return null;
        }
        return new Tenant(tenantDef.getName());
    }   // validateTenantUserPassword

    // Get the TenantDefinition for the given tenant. Use the cached tenant map but
    // refresh it if the tenant is unknown. If it's stil unknown, return null. 
    private TenantDefinition getTenantDef(String tenantName) {
        TenantDefinition tenantDef = null;
        synchronized (m_tenantMap) {
            tenantDef = m_tenantMap.get(tenantName);
            if (tenantDef == null) {
                refreshTenantMap();
                tenantDef = m_tenantMap.get(tenantName);
            }
        }
        return tenantDef;
    }   // getTenantInfo

    // Refresh the tenant name-to-definition map.
    private void refreshTenantMap() {
        synchronized (m_tenantMap) {
            m_tenantMap.clear();
            for (Tenant tenant : DBService.instance().getTenants()) {
                TenantDefinition tenantDef = null;
                if (tenant.getKeyspace().equals(ServerConfig.getInstance().keyspace)) {
                    tenantDef = new TenantDefinition();
                    tenantDef.setName(tenant.getKeyspace());
                } else {
                    tenantDef = getTenantDefinition(tenant);
                }
                if (tenantDef != null) {
                    m_tenantMap.put(tenant.getKeyspace(), tenantDef);
                }
            }
        }
    }   // refreshTenantMap

    // Get the TenantDefinition for the given tenant from the database.
    private TenantDefinition getTenantDefinition(Tenant tenant) {
        DColumn tenantDefCol =
            DBService.instance().getColumn(tenant, SchemaService.APPS_STORE_NAME, TENANT_ROW_KEY, TENANT_DEF_COL_NAME);
        if (tenantDefCol == null) {
            return null;    // Not a valid Doradus tenant.
        }
        String tenantDefJSON = tenantDefCol.getValue();
        TenantDefinition tenantDef = new TenantDefinition();
        try {
            tenantDef.parse(UNode.parseJSON(tenantDefJSON));
        } catch (Exception e) {
            m_logger.warn("Skipping malformed tenant definition; tenant=" + tenant.getKeyspace(), e);
            return null;
        }
        return tenantDef;
    }   // queryTenantDefinition

}   // class TenantService
