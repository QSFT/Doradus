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

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.UNode;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.Tenant;
import com.dell.doradus.service.rest.UNodeOutCallback;
import com.dell.doradus.service.schema.SchemaService;

/**
 * Implements the system REST commands: /_tenants
 */
public class ListTenantsCmd extends UNodeOutCallback {

    @Override
    public UNode invokeUNodeOut() {
        UNode rootNode = UNode.createMapNode("tenants");
        for (Tenant tenant : DBService.instance().getTenants()) {
            UNode tenantNode = rootNode.addMapNode(stripQuotes(tenant.getKeyspace()), "tenant");
            UNode appNode = tenantNode.addArrayNode("applications");
            for (ApplicationDefinition appDef : SchemaService.instance().getAllApplications(tenant)) {
                appNode.addValueNode("value", appDef.getAppName());
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
