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

import java.util.SortedMap;

import com.dell.doradus.common.HttpMethod;
import com.dell.doradus.common.UNode;
import com.dell.doradus.service.db.DBManagerService;
import com.dell.doradus.service.rest.UNodeOutCallback;
import com.dell.doradus.service.rest.annotation.Description;

// Command: GET /_activedbs
@Description (
    name = "ActiveTenants",
    summary = "Get information on active Tenants",
    methods = {HttpMethod.GET},
    uri = "/_tenants/_active",
    privileged = true,
    outputEntity = "active-tenants"
)
public class ActiveTenantsCmd extends UNodeOutCallback {

    @Override
    public UNode invokeUNodeOut() {
        UNode rootNode = UNode.createMapNode("active-tenants");
        SortedMap<String, SortedMap<String, Object>> tenantInfoMap = DBManagerService.instance().getActiveTenantInfo();
        for (String tenantName : tenantInfoMap.keySet()) {
            UNode tenantNode = rootNode.addMapNode(tenantName, "tenant");
            SortedMap<String, Object> tenantParamMap = tenantInfoMap.get(tenantName);
            for (String paramName : tenantParamMap.keySet()) {
                Object paramValue = tenantParamMap.get(paramName);
                if (paramName.contains("password") || paramName.contains("secret")) {
                    paramValue = "*****";
                } else if (paramValue == null) {
                    paramValue = "";
                }
                tenantNode.addValueNode(paramName, paramValue.toString(), "param");
            }
        }
        return rootNode;
    }

}
