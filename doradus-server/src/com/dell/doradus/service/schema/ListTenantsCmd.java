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

import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;
import com.dell.doradus.service.db.Tenant;
import com.dell.doradus.service.rest.UNodeOutCallback;

/**
 * Implements REST commands: /_tenants and /_tenants/{tenant}
 */
public class ListTenantsCmd extends UNodeOutCallback {

    @Override
    public UNode invokeUNodeOut(UNode inNode) {
        Utils.require(inNode == null, "No input entity allowed for this command");
        String tenantParam = m_request.getVariableDecoded("tenant");
        SortedMap<Tenant, SortedSet<String>> tenantMap = new TreeMap<>();
        for (ApplicationDefinition appDef : SchemaService.instance().getAllApplications()) {
            Tenant tenant = Tenant.getTenant(appDef);
            if (Utils.isEmpty(tenantParam) || tenant.getKeyspace().equals(tenantParam)) {
                SortedSet<String> appNameSet = tenantMap.get(tenant);
                if (appNameSet == null) {
                    appNameSet = new TreeSet<>();
                    tenantMap.put(tenant, appNameSet);
                }
                appNameSet.add(appDef.getAppName());
            }
        }
        UNode rootNode = UNode.createMapNode("tenants");
        for (Tenant tenant : tenantMap.keySet()) {
            UNode tenantNode = rootNode.addArrayNode(stripQuotes(tenant.getKeyspace()), "tenant");
            SortedSet<String> appNameSet = tenantMap.get(tenant);
            for (String appName : appNameSet) {
                tenantNode.addValueNode("value", appName);
            }
        }
        return rootNode;
    }

    private String stripQuotes(String keyspace) {
        if (keyspace.charAt(0) == '"' && keyspace.charAt(keyspace.length() - 1) == '"') {
            return keyspace.substring(1, keyspace.length() - 1);
        } else {
            return keyspace;
        }
    }
    
}   // class ListTenantsCmd
