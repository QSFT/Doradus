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
import java.util.Set;

import com.dell.doradus.common.TenantDefinition;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.UserDefinition;
import com.dell.doradus.common.UserDefinition.Permission;
import com.dell.doradus.common.Utils;
import com.dell.doradus.core.ServerParams;
import com.dell.doradus.service.Service;
import com.dell.doradus.service.db.DBManagerService;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.DRow;
import com.dell.doradus.service.db.DuplicateException;
import com.dell.doradus.service.db.Tenant;
import com.dell.doradus.service.db.UnauthorizedException;
import com.dell.doradus.service.db.cql.CQLService;
import com.dell.doradus.service.db.thrift.ThriftService;
import com.dell.doradus.service.rest.NotFoundException;
import com.dell.doradus.service.rest.RESTCallback;
import com.dell.doradus.service.rest.RESTService;
import com.dell.doradus.service.schema.SchemaService;
import com.dell.doradus.service.taskmanager.TaskManagerService;
import com.dell.doradus.utilities.PasswordManager;

/**
 * Provides tenant management services such as creating new tenants, listing tenants, and
 * validating a user id/password for a given tenant.
 */
public class TenantService extends Service {
    // Tenant registry table name:
    public static final String TENANTS_STORE_NAME = "Tenants";
    
    // System-required properties:
    public static final String CREATED_ON_PROP = "_CreatedOn";
    public static final String MODIFIED_ON_PROP = "_ModifiedOn";
    
    // Singleton object required for the Service pattern:
    private static final TenantService INSTANCE = new TenantService();

    // Tenant definition column name:
    private static final String TENANT_DEF_COL_NAME = "Definition";

    // Default tenant name from configured parameters:
    private String m_defaultTenantName;
    
    // REST commands supported by the TenantService:
    private static final List<Class<? extends RESTCallback>> CMD_CLASSES = Arrays.asList(
        ListTenantsCmd.class,
        ListTenantCmd.class,
        DefineTenantCmd.class,
        ModifyTenantCmd.class,
        DeleteTenantCmd.class,
        ActiveTenantsCmd.class
    );
    
    // Singleton creation only.
    private TenantService() {
        m_defaultTenantName = getParamString("default_tenant_name");
        if (Utils.isEmpty(m_defaultTenantName)) {
            throw new RuntimeException("Configuration parameter 'TenantService.default_tenant_name' must be defined");
        }
    };

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
        RESTService.instance().registerCommands(CMD_CLASSES);
    }

    @Override
    protected void startService() {
        DBManagerService.instance().waitForFullService();
        initializeDefaultTenant();
        migrateTenantDefinitions();
    }

    @Override
    protected void stopService() { }

    //----- Tenant management methods

    /**
     * Get the name of the default tenant.
     * 
     * @return Default tenant name.
     */
    public String getDefaultTenantName() {
        return m_defaultTenantName;
    }
    
    /**
     * Return a {@link Tenant} that represents the default keyspace. This is a convenience
     * method that calls <code>new Tenant(getDefaultTenantDef())</code>.
     * 
     * @return  A Tenant object that can be used to access the default tenant.
     */
    public Tenant getDefaultTenant() {
        return new Tenant(getDefaultTenantDef());
    }   // getDefaultTenant

    /**
     * Get the TenantDefinition for the default tenant. If the DBService has not yet
     * reached the running state, we cannot read the default tenant definition from the
     * database, in which case we create a TenantDefinition based on configuration
     * parameters. This allows certain system commands such as "GET /_logs" to succeed
     * before the initial DB connection is established. Once the DB connection has been
     * created, we read the tenant definition from the Tenants table.
     * 
     * @return  The {@link TenantDefinition} for the default tenant.
     */
    public TenantDefinition getDefaultTenantDef() {
        if (!getState().isRunning()) {
            return createDefaultTenantDefinition();
        }
        TenantDefinition tenantDef = getTenantDef(m_defaultTenantName);
        if (tenantDef == null) {
            throw new RuntimeException("Default tenant definition is missing: " + m_defaultTenantName);
        }
        return tenantDef;
    }
    
    /**
     * Get the {@link Tenant} object for the tenant with the given name. This is a
     * convenience method that calls {@link #getTenantDefinition(String)} and either
     * returns null or calls <code>new Teanant(tenantDef)</code> using the tenant
     * definition found.
     * 
     * @param tenantName    Name of a tenant.
     * @return              {@link Tenant} object for tenant, which also contains the
     *                      tenant's {@link TenantDefinition}.
     */
    public Tenant getTenant(String tenantName) {
        if (tenantName.equals(m_defaultTenantName)) {
            return getDefaultTenant();
        }
        checkServiceState();
        TenantDefinition tenantDef = getTenantDefinition(tenantName);
        if (tenantDef == null) {
            return null;
        }
        return new Tenant(tenantDef);
    }
    
    /**
     * Get the {@link TenantDefinition} of the tenant with the given name. If the given
     * tenant name is the default tenant, this method calls {@link #getDefaultTenantDef()}.
     * Otherwise, the DBService must be initialized and the tenant definition is read
     * from the database.
     * 
     * @param tenantName    Candidate tenant name.
     * @return              Definition of tenant if it exists, otherwise null.
     */
    public TenantDefinition getTenantDefinition(String tenantName) {
        // Allow access to default tenant if DBService is not yet initialized.
        if (tenantName.equals(m_defaultTenantName)) {
            return getDefaultTenantDef();
        }
        checkServiceState();
        return getTenantDef(tenantName);
    }   // getTenantDefinition

    /**
     * Get the list of all known tenants. Each {@link Tenant} object contains the
     * corresponding tenant's {@link TenantDefinition}.
     * 
     * @return  List of all known {@link Tenant}.
     */
    public Collection<Tenant> getTenants() {
        checkServiceState();
        Map<String, TenantDefinition> tenantMap = getAllTenantDefs();
        List<Tenant> result = new ArrayList<>();
        for (String tenantName : tenantMap.keySet()) {
            result.add(new Tenant(tenantMap.get(tenantName)));
        }
        return result;
    }

    /**
     * Search for the first {@link TenantDefinition} that is selected by the given filter.
     * All current tenant definitions are searched.
     *  
     * @param   filter  {@link TenantFilter} that decides selection criteria.
     * @return          First {@link TenantDefinition} selected by the filter or null if
     *                  the search exhausts all known tenants without a selection.
     */
    public TenantDefinition searchForTenant(TenantFilter filter) {
        checkServiceState();
        for (TenantDefinition tenantDef : getAllTenantDefs().values()) {
            if (filter.selectTenant(tenantDef)) {
                return tenantDef;
            }
        }
        return null;
    }   // searchForTenant
    
    /**
     * Create a new tenant with the given definition and return the updated definition.
     * Throw a {@link DuplicateException} if the tenant has already been defined.
     * 
     * @param tenantDef {@link TenantDefinition} of new tenant.
     * @return          Updated definition.
     */
    public TenantDefinition defineTenant(TenantDefinition tenantDef) {
        checkServiceState();
        String tenantName = tenantDef.getName();
        if (getTenantDef(tenantName) != null) {
            throw new DuplicateException("Tenant already exists: " + tenantName);
        }
        defineNewTenant(tenantDef);
        TenantDefinition updatedTenantDef = getTenantDef(tenantName);
        if (updatedTenantDef == null) {
            throw new RuntimeException("Tenant definition could not be retrieved after creation: " + tenantName);
        }
        removeUserHashes(updatedTenantDef);
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
        TenantDefinition oldTenantDef = getTenantDef(tenantName);
        Utils.require(oldTenantDef != null, "Tenant '%s' does not exist", tenantName);
        modifyTenantProperties(oldTenantDef, newTenantDef);
        validateTenantUsers(newTenantDef);
        validateTenantUpdate(oldTenantDef, newTenantDef);
        storeTenantDefinition(newTenantDef);
        DBManagerService.instance().updateTenantDef(newTenantDef);
        TenantDefinition updatedTenantDef = getTenantDef(tenantName);
        if (updatedTenantDef == null) {
            throw new RuntimeException("Tenant definition could not be retrieved after creation: " + tenantName);
        }
        removeUserHashes(updatedTenantDef);
        return updatedTenantDef; 
    }   // modifyTenant
    
    /**
     * Delete an existing tenant. The tenant's keyspace is dropped, which deletes all user
     * and system tables, and the tenant's users are deleted. This method is a no-op if the
     * given tenant does not exist.
     * 
     * @param tenantName    Name of tenant to delete.
     */
    public void deleteTenant(String tenantName) {
        deleteTenant(tenantName, null);
    }   // deleteTenant

    /**
     * Delete an existing tenant with the given options. The tenant's keyspace is dropped,
     * which deletes all user and system tables, and the tenant's users are deleted. The
     * given options are currently used for testing only. This method is a no-op if the
     * given tenant does not exist.
     * 
     * @param tenantName    Name of tenant to delete.
     */
    public void deleteTenant(String tenantName, Map<String, String> options) {
        checkServiceState();
        TenantDefinition tenantDef = getTenantDef(tenantName);
        if (tenantDef == null) {
            return;
        }
        
        Tenant tenant = new Tenant(tenantDef);
        try {
            DBService.instance(tenant).dropNamespace();
        } catch (RuntimeException e) {
            if (options == null || !"true".equalsIgnoreCase(options.get("ignoreTenantDBNotAvailable"))) {
                throw e;
            }
            m_logger.warn("Drop namespace skipped for tenant '{}'", tenantName);
        }
        
        // Delete tenant definition in default database.
        Tenant defaultTenant = getDefaultTenant();
        DBTransaction dbTran = new DBTransaction(defaultTenant);
        dbTran.deleteRow(TENANTS_STORE_NAME, tenantName);
        DBService.instance(defaultTenant).commit(dbTran);
        DBManagerService.instance().deleteTenantDB(tenant);
    }   // deleteTenant
    
    //----- Tenant authorization
    
    /**
     * Verify that the given tenant and command can be accessed using by the user
     * represented by the given userID and password. An {@link UnauthorizedException} is
     * thrown if the given credentials are invalid for the given tenant/command or if the
     * user has insufficient rights for the command being accessed.
     * <p>
     * If the given tenant does not exist, a {@link NotFoundException} is thrown.
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
    public void validateTenantAccess(Tenant tenant, String userid, String password,
                                     Permission permNeeded, boolean isPrivileged)
            throws UnauthorizedException, NotFoundException {
        // So we can allow /_logs and other commands before the DBService has initialized,
        // allow valid privileged commands without checking the tenant.
        if (isPrivileged) {
            if (!isValidSystemCredentials(userid, password)) {
                throw new UnauthorizedException("Unrecognized system user id/password");
            }
            return;
        }
        
        // Here the DB connection must be established.
        checkServiceState();
        if (!isValidTenantUserAccess(tenant, userid, password, permNeeded)) {
            throw new UnauthorizedException("Invalid tenant credentials or insufficient permission");
        }
    }
    
    //----- Private methods

    // Ensure that the default tenant and its required metadata tables exist.
    private void initializeDefaultTenant() {
        DBService dbService = DBService.instance();
        dbService.createNamespace();
        dbService.createStoreIfAbsent(SchemaService.APPS_STORE_NAME, false);
        dbService.createStoreIfAbsent(TaskManagerService.TASKS_STORE_NAME, false);
        dbService.createStoreIfAbsent(TENANTS_STORE_NAME, false);
        storeInitialDefaultTenantDef();
    }
    
    /**
     * Set required properties in a new Tenant Definition.
     * 
     * @param   oldTenantDef  Old {@link TenantDefinition}.
     * @param   newTenantDef  New {@link TenantDefinition}.
     */
    private void modifyTenantProperties(TenantDefinition oldTenantDef, TenantDefinition newTenantDef) {
        // _CreatedOn must be the same. _ModifiedOn must be updated.
        newTenantDef.setProperty(CREATED_ON_PROP, oldTenantDef.getProperty(CREATED_ON_PROP));
        newTenantDef.setProperty(MODIFIED_ON_PROP, Utils.formatDate(new Date().getTime()));
    }
    
    // Create a TenantDefinition that represents the default tenant. This can be used
    // when we don't want to depend on the definition existing in the Tenants table.
    private TenantDefinition createDefaultTenantDefinition() {
        TenantDefinition tenantDef = new TenantDefinition();
        tenantDef.setName(m_defaultTenantName);
        return tenantDef;
    }

    // Store a tenant definition for the default tenant if one doesn't already exist.
    private void storeInitialDefaultTenantDef() {
        TenantDefinition tenantDef = getTenantDef(m_defaultTenantName);
        if (tenantDef == null) {
            tenantDef = createDefaultTenantDefinition();
            storeTenantDefinition(tenantDef);
        }
    }
    
    // Migrate "_tenant" rows in non-default keyspaces to to the Tenants table in the
    // default database.
    private void migrateTenantDefinitions() {
        DBService dbservice = DBService.instance();
        List<String> keyspaces = null;
        if (dbservice instanceof ThriftService) {
            keyspaces = ((ThriftService)dbservice).getDoradusKeyspaces();
        } else if (dbservice instanceof CQLService) {
            keyspaces = ((CQLService)dbservice).getDoradusKeyspaces();
        } else {
            return;
        }
        for (String keyspace : keyspaces) {
            migrateTenantDefinition(keyspace);
        }
    }
    
    // Migrate legacy "_tenant" row, if it exists to the Tenants table. 
    private void migrateTenantDefinition(String keyspace) {
        TenantDefinition tempTenantDef = new TenantDefinition();
        tempTenantDef.setName(keyspace);
        Tenant migratingTenant = new Tenant(tempTenantDef);
        DColumn col = DBService.instance(migratingTenant).getColumn("Applications", "_tenant", "Definition");
        if (col == null) {
            return;
        }
        m_logger.info("Migrating tenant definition from keyspace {}", keyspace);
        TenantDefinition migratingTenantDef = new TenantDefinition();
        try {
            migratingTenantDef.parse(UNode.parseJSON(col.getValue()));
        } catch (Exception e) {
            m_logger.warn("Couldn't parse tenant definition; skipping migration of keyspace: " + keyspace, e);
            return;
        }
        storeTenantDefinition(migratingTenantDef);
        DBTransaction dbTran = new DBTransaction(migratingTenant);
        dbTran.deleteRow("Applications", "_tenant");
        DBService.instance(migratingTenant).commit(dbTran);
    }

    // Define a new tenant
    private void defineNewTenant(TenantDefinition tenantDef) {
        validateTenantUsers(tenantDef);
        addTenantOptions(tenantDef);
        addTenantProperties(tenantDef);
        
        Tenant tenant = new Tenant(tenantDef);
        DBService dbService = DBService.instance(tenant);
        dbService.createNamespace();
        initializeTenantStores(tenant);
        storeTenantDefinition(tenantDef);
    }

    // Ensure required tenant stores exist.
    private void initializeTenantStores(Tenant tenant) {
        DBService.instance(tenant).createStoreIfAbsent(SchemaService.APPS_STORE_NAME, false);
        DBService.instance(tenant).createStoreIfAbsent(TaskManagerService.TASKS_STORE_NAME, false);
    }
    
    // By default, each tenant's namespace is the same as their tenant name. 
    private void addTenantOptions(TenantDefinition tenantDef) {
        if (tenantDef.getOption("namespace") == null) {
            tenantDef.setOption("namespace", tenantDef.getName());
        }
    }
    
    // Set system-defined properties for a new tenant.
    private void addTenantProperties(TenantDefinition tenantDef) {
    	tenantDef.setProperty(CREATED_ON_PROP, Utils.formatDate(new Date().getTime()));
    	tenantDef.setProperty(MODIFIED_ON_PROP, tenantDef.getProperty(CREATED_ON_PROP));
    }

    // Verify that all user definitions have a password and convert the password into
    // hashed format.
    private void validateTenantUsers(TenantDefinition tenantDef) {
        for (UserDefinition userDef : tenantDef.getUsers().values()) {
            Utils.require(!Utils.isEmpty(userDef.getPassword()),
                          "Password is required; user ID=" + userDef.getID());
            userDef.setHash(PasswordManager.hash(userDef.getPassword()));
            userDef.setPassword(null);
        }
    }

    // Store the given tenant definition the Tenants table in the default database.
    private void storeTenantDefinition(TenantDefinition tenantDef) {
        String tenantDefJSON = tenantDef.toDoc().toJSON();
        DBTransaction dbTran = DBService.instance().startTransaction();
        dbTran.addColumn(TENANTS_STORE_NAME, tenantDef.getName(), TENANT_DEF_COL_NAME, tenantDefJSON);
        DBService.instance().commit(dbTran);
    }

    // Return true if the given userid/password are valid system credentials.
    private boolean isValidSystemCredentials(String userid, String password) {
        String superUser = ServerParams.instance().getModuleParamString("DoradusServer", "super_user");
        String superPassword = ServerParams.instance().getModuleParamString("DoradusServer", "super_password");
        return Utils.isEmpty(superUser) ||
               Utils.isEmpty(superPassword) ||
               (superUser.equals(userid) && superPassword.equals(password));
    }
    
    // Validate the given user ID and password.
    private boolean isValidTenantUserAccess(Tenant tenant, String userid, String password, Permission permNeeded) {
        TenantDefinition tenantDef = tenant.getDefinition();
        assert tenantDef != null;
        if (tenantDef.getUsers().size() == 0) {
            // We allow access to the default tenant when it has no users defined.
            return tenant.getName().equals(m_defaultTenantName);
        }
        UserDefinition userDef = tenantDef.getUser(userid);
        if (userDef == null || Utils.isEmpty(password)) {
            return false;   // no such user or no password given
        }
        if (userDef.getHash() != null) {
            if (!PasswordManager.checkPassword(password, userDef.getHash())) {
                return false;   // password is hashed but didn't match
            }
        } else {
            if (!password.equals(userDef.getPassword())) {
                return false;   // password is plaintext but didn't match
            }
        }
        return isValidUserAccess(userDef, permNeeded);
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

    // Get the TenantDefinition for the given tenant. Return null if unknown. 
    private TenantDefinition getTenantDef(String tenantName) {
        DRow tenantDefRow = DBService.instance().getRow(TENANTS_STORE_NAME, tenantName);
        if (tenantDefRow == null) {
            return null;
        }
        return loadTenantDefinition(tenantDefRow);
    }

    // Get all tenants, including the default tenant.
    private Map<String, TenantDefinition> getAllTenantDefs() {
        Map<String, TenantDefinition> tenantMap = new HashMap<>();
        Iterable<DRow> rowIter = DBService.instance().getAllRows(TENANTS_STORE_NAME);
        for (DRow row : rowIter) {
            TenantDefinition tenantDef = loadTenantDefinition(row);
            if (tenantDef != null) {
                tenantMap.put(tenantDef.getName(), tenantDef);
            }
        }
        return tenantMap;
    }

    // Load a TenantDefinition from the Applications table.
    private TenantDefinition loadTenantDefinition(DRow tenantDefRow) {
        String tenantName = tenantDefRow.getKey();
        m_logger.debug("Loading definition for tenant: {}", tenantName);
        DColumn tenantDefCol = tenantDefRow.getColumn(TENANT_DEF_COL_NAME);
        if (tenantDefCol == null) {
            return null;    // Not a valid Doradus tenant.
        }
        String tenantDefJSON = tenantDefCol.getValue();
        TenantDefinition tenantDef = new TenantDefinition();
        try {
            tenantDef.parse(UNode.parseJSON(tenantDefJSON));
            Utils.require(tenantDef.getName().equals(tenantName),
                          "Tenant definition (%s) did not match row name (%s)",
                          tenantDef.getName(), tenantName);
        } catch (Exception e) {
            m_logger.warn("Skipping malformed tenant definition; tenant=" + tenantName, e);
            return null;
        }
        return tenantDef;
    }   // loadTenantDefinition

    // Validate that the given modifications are allowed; throw any transgressions found.
    private void validateTenantUpdate(TenantDefinition oldTenantDef,
                                      TenantDefinition newTenantDef) {
        Utils.require(oldTenantDef.getName().equals(newTenantDef.getName()),
                      "Tenant name cannot be changed: %s", newTenantDef.getName());
        Map<String, Object> oldDBServiceOpts = oldTenantDef.getOptionMap("DBService");
        String oldDBService = oldDBServiceOpts == null ? null : (String)oldDBServiceOpts.get("dbservice");
        Map<String, Object> newDBServiceOpts = newTenantDef.getOptionMap("DBService");
        String newDBService = newDBServiceOpts == null ? null : (String)newDBServiceOpts.get("dbservice");
        Utils.require((oldDBService == null && newDBService == null) || oldDBService.equals(newDBService),
                      "'DBService.dbservice' parameter cannot be changed: tenant=%s, previous=%s, new=%s",
                      newTenantDef.getName(), oldDBService, newDBService);
    }

    // Remove hash value from user definitions.
    private void removeUserHashes(TenantDefinition tenantDef) {
        for (UserDefinition userDef : tenantDef.getUsers().values()) {
            userDef.setHash(null);
        }
    }
    
}   // class TenantService
