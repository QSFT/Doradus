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
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.Utils;
import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.service.rest.RESTCallback;

/**
 * Handles the system command: DELETE /_tenants/{tenant}. The user must be authorized as
 * a system user. 
 */
public class DeleteTenantCmd extends RESTCallback {

    @Override
    protected RESTResponse invoke() {
        Utils.require(ServerConfig.getInstance().multitenant_mode,
                      "This command is only allowed in multi-tenant mode; see 'multitenant_mode' parameter");
        String tenantParam = m_request.getVariableDecoded("tenant");
        assert tenantParam != null;
        
        // TODO: Ensure the user is a system user.
        
        TenantService.instance().deleteTenant(tenantParam);
        return new RESTResponse(HttpCode.OK);
    }

}   // class DeleteTenantCmd
