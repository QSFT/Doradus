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

import com.dell.doradus.common.HttpCode;
import com.dell.doradus.common.HttpMethod;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.service.rest.RESTCallback;
import com.dell.doradus.service.rest.annotation.Description;

@Description(
    name = "DeleteTenant",
    summary = "Deletes a new tenant and its applications.",
    methods = HttpMethod.DELETE,
    uri = "/_tenants/{tenant}",
    privileged = true
)
public class DeleteTenantCmd extends RESTCallback {

    @Override
    public RESTResponse invoke() {
        String tenantParam = m_request.getVariableDecoded("tenant");
        TenantService.instance().deleteTenant(tenantParam);
        return new RESTResponse(HttpCode.OK);
    }

}   // class DeleteTenantCmd
