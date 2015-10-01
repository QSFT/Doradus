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

import java.util.HashMap;
import java.util.Map;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.TenantDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.core.ServerParams;
import com.dell.doradus.service.tenant.TenantService;

/**
 * Represents a Doradus tenant. This class holds the tenant's name, it's
 * TenantDefinition, and any server-side state used for the tenant.
 */
public class Tenant implements Comparable<Tenant> {
    private final String m_name;
    private final TenantDefinition m_tenantDef;
    private final Map<String, Object> m_dbParamMap = new HashMap<>();
    
    /**
     * Create a Tenant object from the given application definition. If the application
     * does not define a tenant, the Tenant for the default database is returned.
     * 
     * @param appDef    {@link ApplicationDefinition}
     * @return          {@link Tenant} in which application resides.
     */
    public static Tenant getTenant(ApplicationDefinition appDef) {
        String tenantName = appDef.getTenantName();
        if(Utils.isEmpty(tenantName)) return TenantService.instance().getDefaultTenant();
        Utils.require(!Utils.isEmpty(tenantName), "Application definition is missing tenant name: " + appDef);
        TenantDefinition tenantDef = TenantService.instance().getTenantDefinition(tenantName);
        Utils.require(tenantDef != null, "Tenant definition does not exist: %s", tenantName);
        return new Tenant(tenantDef);
    }   // getTenant

    /**
     * Convenience method which calls {@link #getTenant(ApplicationDefinition)} using
     * tableDef.getAppDef().
     * 
     * @param tableDef  {@link TableDefinition}
     * @return          {@link Tenant} in which application resides.
     */
    public static Tenant getTenant(TableDefinition tableDef) {
        return getTenant(tableDef.getAppDef());
    }   // getTenant

    /**
     * 
     * @param tenantDef
     */
    public Tenant(TenantDefinition tenantDef) {
        m_name = tenantDef.getName();
        m_tenantDef = tenantDef;
        loadDBServiceParams();
    }

    private void loadDBServiceParams() {
        if (m_tenantDef.getOption("DBService") == null) {
            copyDefaultDBParams();
        } else {
            copyTenantDBParams();
        }
    }
    
    @SuppressWarnings("unchecked")
    private void copyDefaultDBParams() {
        Object dbServiceParams = ServerParams.instance().getModuleParams("DBService");
        Utils.require(dbServiceParams != null, "'DBService' parameter has not been defined");
        Map<String, Object> defaultDBParamMap = (Map<String, Object>)dbServiceParams;
        m_dbParamMap.putAll(defaultDBParamMap);
        
        String dbservice = (String)m_dbParamMap.get("dbservice");
        Utils.require(dbservice != null, "'DBService.dbservice' parameter has not been defined");
        
        try {
            Class<?> dbServiceClass = Class.forName(dbservice);
            String serviceName = dbServiceClass.getSimpleName();
            while (serviceName.endsWith("Service")) {
                dbServiceParams = ServerParams.instance().getModuleParams(serviceName);
                if (dbServiceParams != null && dbServiceParams instanceof Map) {
                    m_dbParamMap.putAll((Map<String, Object>)dbServiceParams);
                }
                dbServiceClass = dbServiceClass.getSuperclass();
                serviceName = dbServiceClass.getSimpleName();
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Could not validate dbservice '" + dbservice + "'", e);
        }
    }

    @SuppressWarnings("unchecked")
    private void copyTenantDBParams() {
        Object dbServiceParams = m_tenantDef.getOption("DBService");
        Map<String, Object> defaultDBParamMap = (Map<String, Object>)dbServiceParams;
        m_dbParamMap.putAll(defaultDBParamMap);
        
        String dbservice = (String)m_dbParamMap.get("dbservice");
        Utils.require(dbservice != null, "'DBService.dbservice' must be defined for Tenant: %s", m_name);
        
        try {
            Class<?> dbServiceClass = Class.forName(dbservice);
            String serviceName = dbServiceClass.getSimpleName();
            while (serviceName.endsWith("Service")) {
                dbServiceParams = m_tenantDef.getOption(serviceName);
                if (dbServiceParams != null && dbServiceParams instanceof Map) {
                    m_dbParamMap.putAll((Map<String, Object>)dbServiceParams);
                }
                dbServiceClass = dbServiceClass.getSuperclass();
                serviceName = dbServiceClass.getSimpleName();
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Could not validate dbservice '" + dbservice + "'", e);
        }
    }

    /**
     * Get this tenant's name.
     * 
     * @return  This tenant's unique name within the cluster.
     */
    public String getName() {
        return m_name;
    }

    /**
     * Get the Tenant definition for this tenant.
     * 
     * @return  This tenant's {@link TenantDefinition}.
     */
    public TenantDefinition getDefinition() {
        return m_tenantDef;
    }
    
    /**
     * Get the database parameter with the given name. For example, if this tenant's
     * definiton has the option:
     * <pre>
     *      DBService:
     *          dbhost: 12.34.56.78
     * </pre>
     * The call <code>getDBParam("dbhost")</code> returns the value "12.34.56.78" as an
     * Object.
     *  
     * @param paramName Name of DBService parameter.
     * @return          Parameter value or null if not defined.
     */
    public Object getDBParam(String paramName) {
        return m_dbParamMap.get(paramName);
    }

    /**
     * Get the given Boolean database parameter, returning false if it is not defined.
     * 
     * @param paramName     Database parameter name.
     * @return              Defined parameter value or false.
     */
    public boolean getDBParamBoolean(String paramName) {
        Object dbParam = m_dbParamMap.get(paramName);
        if (dbParam == null) {
            return false;
        }
        return Boolean.parseBoolean(dbParam.toString());
    }
    
    /**
     * Get the given integer database parameter, returning the given default value if it
     * is not defined.
     * 
     * @param paramName     Database parameter name.
     * @param defaultValue  Default value returned if parameter is not defined.
     * @return              Defined or default parameter value.
     */
    public int getDBParamInt(String paramName, int defaultValue) {
        Object dbParam = m_dbParamMap.get(paramName);
        if (dbParam == null) {
            return defaultValue;
        }
        return Integer.parseInt(dbParam.toString());
    }

    /**
     * Get the database parameter with the given name. This method calls
     * {@link #getDBParam(String)} and calls toString() on the result.
     *  
     * @param paramName Name of DBService parameter.
     * @return          Parameter value as a String or null if not defined.
     */
    public String getDBParamString(String paramName) {
        Object dbParam = m_dbParamMap.get(paramName);
        if (dbParam != null) {
            return dbParam.toString();
        }
        return null;
    }
    
    // So we can be used as a collection key.
    @Override
    public int compareTo(Tenant o) {
        return this.m_name.compareTo(o.m_name);
    }
    
    // So we can be used as a collection key.
    @Override
    public int hashCode() {
        return m_name.hashCode();
    }
    
    @Override
    public String toString() {
        return m_name;
    }   // toString

}   // class Tenant
