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

import com.dell.doradus.common.HttpMethod;
import com.dell.doradus.common.UNode;
import com.dell.doradus.service.rest.UNodeInOutCallback;
import com.dell.doradus.service.rest.annotation.Description;

/**
 * Implements the system command: PUT /_tenants/{tenant}.
 */
@Description(
    name = "ModifyTenant",
    summary = "Modifies the configuration of an existing tenant.",
    methods = HttpMethod.PUT,
    uri = "/_tenants/{tenant}",
    privileged = true,
    inputEntity = "tenant",
    outputEntity = "tenant"
)
public class ModifyTenantCmd extends UNodeInOutCallback {

    @Override
    public UNode invokeUNodeInOut(UNode inNode) {
        String tenantName = m_request.getVariableDecoded("tenant");
        TenantDefinition tenantDef = new TenantDefinition();
        tenantDef.parse(inNode);
        tenantDef = TenantService.instance().modifyTenant(tenantName, tenantDef);
        return tenantDef.toDoc();
    }

}   // class ModifyTenantCmd
