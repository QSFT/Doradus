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
    
    /**
     * Create a Tenant object from the given application definition. If the application
     * does not define a tenant, the Tenant for the default database is returned.
     * 
     * @param appDef    {@link ApplicationDefinition}
     * @return          {@link Tenant} in which application resides.
     */
    public static Tenant getTenant(ApplicationDefinition appDef) {
        String tenantName = appDef.getTenantName();
        if (Utils.isEmpty(tenantName)) {
            return TenantService.instance().getDefaultTenant();
        }
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
     * Create a Tenant object from the given TenantDefinition.
     * 
     * @param tenantDef {@link TenantDefinition} that defines a tenant.
     */
    public Tenant(TenantDefinition tenantDef) {
        m_name = tenantDef.getName();
        m_tenantDef = tenantDef;
    }

    /**
     * Get all DBService parameters defined by this tenant, if any. If the tenant
     * definition's options do not define "DBService", null is returned. Otherwise, the
     * DBService.dbservice option is used to find and merge all options defined by the
     * tenant for the corresponding DBService. For example, suppose the tenant definition
     * includes these options:
     * <pre>
     *      {"options": {
     *          "DBService": {
     *              "dbservice": "com.dell.doradus.service.db.thrift.ThriftService"
     *          },
     *          "ThriftService": {
     *              "dbhost": 12.34.56.78,
     *              "dbport": 7532
     *          }
     *      }}
     * <pre>
     * This method inspects the class hierarchy of the ThriftService and loads all options
     * defined for it and its superclasses into a single map. In the example above, this
     * means options defined for both DBService and ThriftService are merged into the map.
     * 
     * @return  String-to-Object map of all DBService parameters defined by this tenant
     *          definition's options, if any, or null if there are none.
     */
    public Map<String, Object> getDBServiceParams() {
        Map<String, Object> tenantDBParamMap = m_tenantDef.getOptionMap("DBService");
        if (tenantDBParamMap == null) {
            return null;
        }
        String dbservice = (String)tenantDBParamMap.get("dbservice");
        Utils.require(!Utils.isEmpty(dbservice), "'DBService.dbservice' option must be defined for tenant: %s", m_name);
        Map<String, Object> dbParamMap = ServerParams.getAllModuleParams(dbservice, m_tenantDef.getOptions());
        return dbParamMap;
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
     * Get this tenant's namespace, which is the name in which its data is stored. This
     * value may be null for older tenants. For newer tenants, this name defaults to the
     * tenant name but can be explicitly defined in the tenant definition.
     * 
     * @return  This tenant's namespace, which may be null if undefined.
     */
    public String getNamespace() {
        return m_tenantDef.getOptionString("namespace");
    }
    
    /**
     * Get the Tenant definition for this tenant.
     * 
     * @return  This tenant's {@link TenantDefinition}.
     */
    public TenantDefinition getDefinition() {
        return m_tenantDef;
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
