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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.dell.doradus.common.TenantDefinition;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.UserDefinition;
import com.dell.doradus.common.UserDefinition.Permission;
import com.dell.doradus.common.Utils;
import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.service.Service;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.DuplicateException;
import com.dell.doradus.service.db.Tenant;
import com.dell.doradus.service.db.UnauthorizedException;
import com.dell.doradus.service.rest.NotFoundException;
import com.dell.doradus.service.rest.RESTCallback;
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
    
    // Tenant name (keyspace) to TenantDefinition cache:
    private final Map<String, TenantDefinition> m_tenantMap = new HashMap<>();
    
    // REST commands supported by the TenantService:
    private static final List<Class<? extends RESTCallback>> CMD_CLASSES = Arrays.asList(
        ListTenantsCmd.class,
        ListTenantCmd.class,
        DefineTenantCmd.class,
        ModifyTenantCmd.class,
        DeleteTenantCmd.class
    );
    
    // Singleton creation only.
    private TenantService() {};

    /**
     * Simple interface used for filtered searching for a {@link TenantDefinition}.
     * 
     * @see TenantService#searchForTenant(TenantFilter)
     */
    public interface TenantFilter {
        /**
         * Indicate if the given TenantDefinition should be returned.
         * 
         * @param tenantDef Candidate {@link TenantDefinition}.
         * @return          True if the tenant definition should be returned.
         */
        boolean selectTenant(TenantDefinition tenantDef);
    }
    
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
        RESTService.instance().registerCommands(CMD_CLASSES);
    }

    @Override
    protected void startService() {
        DBService.instance().waitForFullService();
        refreshTenantMap();
    }

    @Override
    protected void stopService() { }

    //----- Tenant management methods

    /**
     * Get the list of all known tenants. This list *might* not contain new tenants
     * created by another Doradus instance but not yet accessed by this instance.
     * 
     * @return  List of all known {@link Tenant}.
     */
    public Collection<Tenant> getTenants() {
        checkServiceState();
        List<Tenant> result = new ArrayList<>();
        for (String tenantName : m_tenantMap.keySet()) {
            result.add(new Tenant(tenantName));
        }
        return result;
    }
    
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
        return getTenantDefFromCache(tenantName);
    }   // getTenantDefinition

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
        if (getTenantDefFromCache(tenantName) != null) {
            throw new DuplicateException("Tenant already exists: " + tenantName);
        }
        Utils.require(!tenantName.equals(ServerConfig.getInstance().keyspace),
                      "Cannot create a tenant with the default keyspace name: " + tenantName);
        defineNewTenant(tenantDef);
        TenantDefinition updatedTenantDef = getTenantDefFromCache(tenantName);
        if (updatedTenantDef == null) {
            throw new RuntimeException("Tenant definition could not be retrieved after creation: " + tenantName);
        }
        return updatedTenantDef;
    }   // defineTenant

    /**
     * Modify the tenant with the given name to match the given definition, and return the
     * updated definition.
     * 
     * @param tenantName    Name of tenant to be modified.
     * @param newTenantDef  Updated {@link TenantDefinition} to apply to tenant.
     * @return              Updated {@link TenantDefinition}.
     */
    public TenantDefinition modifyTenant(String tenantName, TenantDefinition newTenantDef) {
        checkServiceState();
        Utils.require(ServerConfig.getInstance().multitenant_mode,
                      "This command is not valid in single-tenant mode");
        TenantDefinition oldTenantDef = getTenantDefFromCache(tenantName);
        Utils.require(oldTenantDef != null, "Tenant '%s' does not exist", tenantName);
        modifyTenantProperties(oldTenantDef, newTenantDef);
        validateTenantUpdate(oldTenantDef, newTenantDef);
        modifyTenantDefinition(oldTenantDef, newTenantDef);
        TenantDefinition updatedTenantDef = getTenantDefFromCache(tenantName);
        if (updatedTenantDef == null) {
            throw new RuntimeException("Tenant definition could not be retrieved after creation: " + tenantName);
        }
        return updatedTenantDef; 
    }   // modifyTenant
    
    
    /**
     * Copying existing Tenant Definition properties to the new Tenant Definition properties if any given Tenant Definition property hasn't been set during Tenant modification process
     * 
     * @param   oldTenantDef    Old {@link TenantDefinition}.
     * @param   newTenantDef  New {@link TenantDefinition}.
     */
    private void modifyTenantProperties(TenantDefinition oldTenantDef, TenantDefinition newTenantDef) {
    	if (newTenantDef.getProperties().get("_CreatedOn") == null && oldTenantDef.getProperties().get("_CreatedOn") != null) {
    		newTenantDef.setProperty("_CreatedOn", oldTenantDef.getProperties().get("_CreatedOn"));
    	} 
    }
    
    /**
     * Delete an existing tenant. The tenant's keyspace is dropped, which deletes all user
     * and system tables, and the tenant's users are deleted.
     * 
     * @param tenantName    Name of tenant to delete.
     */
    public void deleteTenant(String tenantName) {
        checkServiceState();
        Utils.require(ServerConfig.getInstance().multitenant_mode,
                      "This command is not valid in single-tenant mode");
        TenantDefinition tenantDef = getTenantDefFromCache(tenantName);
        if (tenantDef == null) {
            return; // allow idempotent deletes
        }
        Tenant tenant = new Tenant(tenantName);
        DBService.instance().dropTenant(tenant);
        deleteTenantFromCache(tenantName);
        deleteAllUsers(tenantDef);
    }   // deleteTenant

    /**
     * Search for the first {@link TenantDefinition} that is selected by the given filter.
     * This method first searches for cached TenantDefinitions, which have been recently
     * used. However, if the filter does not select any of these, the tenant cache is
     * refreshed and searched again. This ensures performant access to recently used
     * tenants without missing any just-created tenants. 
     *  
     * @param   filter  {@link TenantFilter} that decides selection criteria.
     * @return          First {@link TenantDefinition} selected by the filter or null if
     *                  the search exhausts all known tenants without a selection.
     */
    public TenantDefinition searchForTenant(TenantFilter filter) {
        checkServiceState();
        synchronized (m_tenantMap) {
            for (int attempt = 1; attempt <= 2; attempt++) {
                for (TenantDefinition tenantDef : m_tenantMap.values()) {
                    if (filter.selectTenant(tenantDef)) {
                        return tenantDef;
                    }
                }
                refreshTenantMap();
            }
        }
        return null;
    }   // searchForTenant
    
    //----- Tenant authorization
    
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
     * Return true if the given tenant is the default tenant.
     * 
     * @param   tenant  Candidate {@link Tenant}.
     * @return          True if the tenant's name matches the configured default tenant.
     */
    public boolean isDefaultTenant(Tenant tenant) {
        return tenant.getKeyspace().equals(ServerConfig.getInstance().keyspace);
    }
    
    /**
     * Verify that the given tenant and command can be accessed using by the user
     * represented by the given userID and password. A {@link UnauthorizedException} is
     * thrown if the given credentials are invalid for the given tenant/command or if the
     * user has insufficient rights for the command being accessed.
     * <p>
     * If the given tenant does not exist, an {@link NotFoundException} is thrown.
     * 
     * @param tenant        {@link Tenant} being accessed.
     * @param userid        User ID of caller. May be null.
     * @param password      Password of caller. May be null.
     * @param permNeeded    {@link Permission} needed by the invoking command.
     * @param isPrivileged  True if the command being accessed is privileged.
     * @throws              NotFoundException if the given tenant does not exist.
     * @throws              UnauthorizedException if the given credentials are invalid or
     *                      have insufficient rights for the given tenant and command.
     */
    public void validateTenantAuthorization(Tenant tenant, String userid, String password,
                                            Permission permNeeded, boolean isPrivileged)
            throws UnauthorizedException, NotFoundException {
        if (ServerConfig.getInstance().multitenant_mode) {
            if (!tenantExists(tenant)) {
                throw new NotFoundException("Unknown tenant: " + tenant);
            }
            if (isPrivileged) {
                if (!isValidSystemCredentials(userid, password)) {
                    throw new UnauthorizedException("Unrecognized system user id/password");
                }
            } else if (isDefaultTenant(tenant)) {
                if (ServerConfig.getInstance().disable_default_keyspace) {
                    throw new UnauthorizedException("Access to the default tenant is disabled");
                } else {
                    // All credentials are allowed for non-priv commands to default tenant
                }
            } else {
                // From here on, the DB connection must be established.
                checkServiceState();
                if (!isValidTenantUserAccess(tenant.getKeyspace(), userid, password, permNeeded)) {
                    throw new UnauthorizedException("Invalid tenant credentials or insufficient permission");
                }
            }
        } else if (!isDefaultTenant(tenant)) {
            throw new UnauthorizedException("This command is not valid in single-tenant mode");
        }
    }   // validateTenantAuthorization
    
    /**
     * Return a {@link Tenant} that represents the default keyspace. This method performs
     * no validation of any kind.
     * 
     * @return  A Tenant object that can be used to access applications in the default
     *          keyspace.
     */
    public Tenant getDefaultTenant() {
        // No checkServiceState() required because no DB access is needed.
        return new Tenant(ServerConfig.getInstance().keyspace);
    }   // getDefaultTenant

    //----- Private methods

    // Define a new tenant
    private void defineNewTenant(TenantDefinition tenantDef) {
        DBService dbService = DBService.instance();
        Tenant tenant = new Tenant(tenantDef.getName());
        dbService.createTenant(tenant, tenantDef.getOptions());
        addTenantUsers(tenantDef);
        addTenantProperties(tenantDef);
        dbService.createStoreIfAbsent(tenant, SchemaService.APPS_STORE_NAME, false);
        dbService.createStoreIfAbsent(tenant, TaskManagerService.TASKS_STORE_NAME, false);
        storeTenantDefinition(tenantDef);
    }
    
    private void addTenantProperties(TenantDefinition tenantDef) {
    	tenantDef.setProperty("_CreatedOn", Utils.formatDate(new Date().getTime()));
    }

    // Add all users in the given tenant definition to Cassandra and authorize them to use
    // the corresponding keyspace.
    private void addTenantUsers(TenantDefinition tenantDef) {
        if (tenantDef.userCount() == 0) {
            addDefaultUser(tenantDef);
        }
        // Prefix all user IDs with the "{tenant}_"
        String tenantName = tenantDef.getName();
        Tenant tenant = new Tenant(tenantName);
        List<UserDefinition> dbUserList = new ArrayList<UserDefinition>();
        for (UserDefinition userDef : tenantDef.getUsers().values()) {
            Utils.require(!Utils.isEmpty(userDef.getPassword()),
                          "Password is required; user ID=" + userDef.getID());
            String newUserID = tenantName + "_" + userDef.getID();
            UserDefinition dbUserDef = userDef.makeCopy(newUserID);
            dbUserList.add(dbUserDef);
        }
        DBService.instance().addUsers(tenant, dbUserList);
    }

    // Add a default user account for the given tenant definition.
    private void addDefaultUser(TenantDefinition tenantDef) {
        UserDefinition userDef = new UserDefinition("U" + generateRandomID());
        userDef.setPassword(generateRandomID());
        tenantDef.addUser(userDef);
    }

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
        updateCache(tenantDef);
    }

    // Return true if the given userid/password are valid system credentials.
    private boolean isValidSystemCredentials(String userid, String password) {
        return userid.equals(ServerConfig.getInstance().dbuser) &&
               password.equals(ServerConfig.getInstance().dbpassword);
    }
    
    // Validate the given user ID and password.
    private boolean isValidTenantUserAccess(String tenantName, String userid, String password, Permission permNeeded) {
        if (isValidSystemCredentials(userid, password)) {
            return true;
        }
        TenantDefinition tenantDef = getTenantDefFromCache(tenantName);
        if (tenantDef != null) {
            UserDefinition userDef = tenantDef.getUser(userid);
            if (userDef != null && userDef.getPassword().equals(password)) {
                return isValidUserAccess(userDef, permNeeded);
            }
        }
        return false;
    }

    // Validate user's permission vs. the given required permission.
    private boolean isValidUserAccess(UserDefinition userDef, Permission permNeeded) {
        Set<Permission> permList = userDef.getPermissions();
        if (permList.size() == 0 || permList.contains(Permission.ALL)) {
            return true;
        }
        switch (permNeeded) {
        case APPEND:
            return permList.contains(Permission.APPEND) || permList.contains(Permission.UPDATE);
        case READ:
            return permList.contains(Permission.READ);
        case UPDATE:
            return permList.contains(Permission.UPDATE);
        default:
            return false;
        }
    }

    // Add the given new or updated tenant definition to the cache.
    private void updateCache(TenantDefinition tenantDef) {
        synchronized (m_tenantMap) {
            m_tenantMap.put(tenantDef.getName(), tenantDef);
        }
    }
    
    // Get the TenantDefinition for the given tenant. Use the cached tenant map but
    // refresh it if the tenant is unknown. If it's still unknown, return null. 
    private TenantDefinition getTenantDefFromCache(String tenantName) {
        TenantDefinition tenantDef = null;
        synchronized (m_tenantMap) {
            tenantDef = m_tenantMap.get(tenantName);
            if (tenantDef == null) {
                refreshTenantMap();
                tenantDef = m_tenantMap.get(tenantName);
            }
        }
        return tenantDef;
    }

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
                    tenantDef = loadTenantDefinition(tenant);
                }
                if (tenantDef != null) {
                    m_tenantMap.put(tenantDef.getName(), tenantDef);
                }
            }
        }
    }

    // Delete the tenant definition with the given name from the cache, if present.
    private void deleteTenantFromCache(String tenantName) {
        synchronized (m_tenantMap) {
            m_tenantMap.remove(tenantName);
        }
    }
    
    // Load a TenantDefinition from the Applications table.
    private TenantDefinition loadTenantDefinition(Tenant tenant) {
        m_logger.debug("Loading definition for tenant: {}", tenant.getKeyspace());
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
    }   // loadTenantDefinition

    // Validate that the given modifications are allowed; throw any transgressions found.
    private void validateTenantUpdate(TenantDefinition oldTenantDef,
                                      TenantDefinition newTenantDef) {
        Utils.require(oldTenantDef.getName().equals(newTenantDef.getName()),
                      "Tenant name cannot be changed: %s", newTenantDef.getName());
        Utils.require(oldTenantDef.getProperties().get("_CreatedOn").equals(newTenantDef.getProperties().get("_CreatedOn")),
        		      "Tenant _CreatedOn property cannot be changed: %s", newTenantDef.getProperties().get("_CreatedOn"));
        // All other changes are allowed
    }

    // Modify an existing tenant with the changes given in the new tenant definition,
    // copying preserved options to the new definition and then storing it.
    private void modifyTenantDefinition(TenantDefinition oldTenantDef,
                                                    TenantDefinition newTenantDef) {
        modifyTenantOptions(oldTenantDef, newTenantDef);
        modifyTenantUsers(oldTenantDef, newTenantDef);
        storeTenantDefinition(newTenantDef);
    }

    // Implement option changes, if any, in the new tenant definition.
    private void modifyTenantOptions(TenantDefinition oldTenantDef,
                                     TenantDefinition newTenantDef) {
        Tenant tenant = new Tenant(oldTenantDef.getName());
        Map<String, String> oldOptions = oldTenantDef.getOptions();
        Map<String, String> newOptions = newTenantDef.getOptions();
        if (!sameProperties(oldOptions, newOptions)) {
            DBService.instance().modifyTenant(tenant, newOptions);
        }
    }
    
    // Add, delete, or modify users as specified in the new tenant defintions. Copy
    // passwords from the old to the new tenant definitions.
    private void modifyTenantUsers(TenantDefinition oldTenantDef,
                                   TenantDefinition newTenantDef) {
        deleteObsoleteUsers(oldTenantDef, newTenantDef);
        addNewUsers(oldTenantDef, newTenantDef);
        modifyUpdatedUsers(oldTenantDef, newTenantDef);
    }

    // Delete obsolete users missing in the given new tenant definition.
    private void deleteObsoleteUsers(TenantDefinition oldTenantDef,
                                     TenantDefinition newTenantDef) {
        String tenantName = oldTenantDef.getName();
        Tenant tenant = new Tenant(tenantName);
        List<UserDefinition> deletedUsers = new ArrayList<>();
        for (UserDefinition userDef : oldTenantDef.getUsers().values()) {
            if (newTenantDef.getUser(userDef.getID()) == null) {
                String userID = tenantName + "_" + userDef.getID();
                UserDefinition dbUserDef = userDef.makeCopy(userID);
                deletedUsers.add(dbUserDef);
            }
        }
        if (deletedUsers.size() > 0) {
            DBService.instance().deleteUsers(tenant, deletedUsers);
        }
    }
    
    // Add new users in the given new tenant definition. 
    private void addNewUsers(TenantDefinition oldTenantDef,
                             TenantDefinition newTenantDef) {
        String tenantName = oldTenantDef.getName();
        Tenant tenant = new Tenant(tenantName);
        List<UserDefinition> newUsers = new ArrayList<>();
        for (UserDefinition userDef : newTenantDef.getUsers().values()) {
            if (oldTenantDef.getUser(userDef.getID()) == null) {
                Utils.require(!Utils.isEmpty(userDef.getPassword()),
                              "Password is required for new users; user ID=" + userDef.getID());
                String userID = tenantName + "_" + userDef.getID();
                UserDefinition dbUserDef = userDef.makeCopy(userID);
                newUsers.add(dbUserDef);
            }
        }
        if (newUsers.size() > 0) {
            DBService.instance().addUsers(tenant, newUsers);
        }
    }
    
    // Implement modified passwords as defined in the given new tenant definition. For
    // users merely respecified, copy the old password to the new definition.
    private void modifyUpdatedUsers(TenantDefinition oldTenantDef,
                                    TenantDefinition newTenantDef) {
        String tenantName = oldTenantDef.getName();
        Tenant tenant = new Tenant(tenantName);
        List<UserDefinition> modifiedUsers = new ArrayList<>();
        for (UserDefinition userDef : newTenantDef.getUsers().values()) {
            UserDefinition oldUserDef = oldTenantDef.getUser(userDef.getID());
            if (oldUserDef != null) {
                if (Utils.isEmpty(userDef.getPassword())) {
                    userDef.setPassword(oldUserDef.getPassword());
                } else if (!userDef.getPassword().equals(oldUserDef.getPassword())) {
                    String userID = tenantName + "_" + userDef.getID();
                    UserDefinition dbUserDef = userDef.makeCopy(userID);
                    modifiedUsers.add(dbUserDef);
                }
            }
        }
        if (modifiedUsers.size() > 0) {
            DBService.instance().modifyUsers(tenant, modifiedUsers);
        }
    }
    
    private void deleteAllUsers(TenantDefinition tenantDef) {
        String tenantName = tenantDef.getName();
        Tenant tenant = new Tenant(tenantName);
        List<UserDefinition> deletedUsers = new ArrayList<>();
        for (UserDefinition userDef : tenantDef.getUsers().values()) {
            String userID = tenantName + "_" + userDef.getID();
            UserDefinition dbUserDef = userDef.makeCopy(userID);
            deletedUsers.add(dbUserDef);
        }
        if (deletedUsers.size() > 0) {
            DBService.instance().deleteUsers(tenant, deletedUsers);
        }
    }
    
    private static boolean sameProperties(Map<String, String> map1, Map<String, String> map2) {
        for (String key : map1.keySet()) {
            if (!map2.containsKey(key) || !map1.get(key).equals(map2.get(key))) {
                return false;
            }
        }
        for (String key : map2.keySet()) {
            if (!map1.containsKey(key) || !map2.get(key).equals(map1.get(key))) {
                return false;
            }
        }
        return true;
    }

}   // class TenantService
