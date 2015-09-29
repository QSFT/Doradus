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

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.TenantDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.service.tenant.TenantService;

/**
 * Represents a unique tenant. This class holds the tenant's name, it's
 * TenantDefinition, and any server-side state used for the tenant.
 */
public class Tenant implements Comparable<Tenant> {
    private final String m_name;
    private final TenantDefinition m_tenantDef;
    
    /**
     * Create a Tenant object from the given application definition, which must have the
     * 'Tenant' option defined.
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
    }
    
//    /**
//     * Create a Tenant that with the given name.
//     *  
//     * @param name  Tenant name, which must be unique within the cluster.
//     */
//    public Tenant(String name) {
//        m_name = name;
//    }

    /**
     * Get this tenant's name.
     * 
     * @return  This tenant's unique name within the cluster.
     */
    public String getName() {
        return m_name;
    }

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
