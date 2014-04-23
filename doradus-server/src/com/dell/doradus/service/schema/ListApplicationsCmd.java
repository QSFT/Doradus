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

import java.util.List;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.UNode;
import com.dell.doradus.service.rest.UNodeOutCallback;

/**
 * Provides the callback for the REST command: /_applications.
 */
public class ListApplicationsCmd extends UNodeOutCallback {

    @Override
    public UNode invokeUNodeOut(UNode inNode) {
        UNode rootNode = UNode.createMapNode("applications");
        List<ApplicationDefinition> appList = SchemaService.instance().getAllApplications();
        for (ApplicationDefinition appDef : appList) {
            rootNode.addChildNode(appDef.toDoc());
        }
        return rootNode;
    }   // invokeUNodeOut

}   // class ListApplicationsCmd
