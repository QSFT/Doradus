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
import com.dell.doradus.common.Utils;

/**
 * Defines a tenant, which can hold one of more Doradus applications. In Cassandra,
 * each tenant is mapped to a unique keyspace.
 */
public class Tenant implements Comparable<Tenant> {
    private final String m_keyspace;
    
    /**
     * Create a Tenant object from the given application definition, which must have the
     * 'Tenant' option defined.
     * 
     * @param appDef    {@link ApplicationDefinition}
     * @return          {@link Tenant} in which application resides.
     */
    public static Tenant getTenant(ApplicationDefinition appDef) {
        String tenantName = appDef.getOption("Tenant");
        Utils.require(!Utils.isEmpty(tenantName), "Application definition is missing 'Tenant' option: " + appDef);
        return new Tenant(tenantName);
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
     * Create a Tenant that resides in the given keyspace.
     *  
     * @param keyspace  Cassandra keyspace name. Might not exist yet.
     */
    public Tenant(String keyspace) {
        m_keyspace = keyspace;
    }

    /**
     * This tenant's keyspace.
     * 
     * @param keyspace  Unquoted keyspace name.
     */
    public String getKeyspace() {
        return m_keyspace;
    }

    // So we can be used as a collection key.
    @Override
    public int compareTo(Tenant o) {
        return this.m_keyspace.compareTo(o.m_keyspace);
    }
    
    // So we can be used as a collection key.
    @Override
    public int hashCode() {
        return m_keyspace.hashCode();
    }
    
}   // class Tenant
