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

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.HttpMethod;
import com.dell.doradus.common.UNode;
import com.dell.doradus.service.db.Tenant;
import com.dell.doradus.service.rest.UNodeOutCallback;
import com.dell.doradus.service.rest.annotation.Description;

@Description(
    name = "ListApps",
    summary = "Returns the schema of all current applications.",
    methods = {HttpMethod.GET},
    uri = "/_applications",
    outputEntity = "applications"
)
public class ListApplicationsCmd extends UNodeOutCallback {

    @Override
    public UNode invokeUNodeOut() {
        Tenant tenant = m_request.getTenant();
        UNode rootNode = UNode.createMapNode("applications");
        for (ApplicationDefinition appDef : SchemaService.instance().getAllApplications(tenant)) {
            rootNode.addChildNode(appDef.toDoc());
        }
        return rootNode;
    }   // invokeUNodeOut

}   // class ListApplicationsCmd
