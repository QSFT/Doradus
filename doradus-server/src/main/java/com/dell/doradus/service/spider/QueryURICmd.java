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

package com.dell.doradus.service.spider;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.HttpMethod;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.UNode;
import com.dell.doradus.service.rest.UNodeOutCallback;
import com.dell.doradus.service.rest.annotation.Description;

@Description(
    name = "QueryURI",
    summary = "Performs an object query on a specific application and table. " + 
              "This command passes all query parameters in the URI.",
    methods = HttpMethod.GET,
    uri = "/{application}/{table}/_query?{params}",
    visible = false,
    outputEntity = "results"
)
public class QueryURICmd extends UNodeOutCallback {

    @Override
    public UNode invokeUNodeOut() {
        ApplicationDefinition appDef = m_request.getAppDef();
        TableDefinition tableDef = m_request.getTableDef(appDef);
        String params = m_request.getVariable("params");    // leave encoded
        ObjectQuery objQuery = new ObjectQuery(tableDef, params);
        return SpiderService.instance().objectQuery(tableDef, objQuery).toDoc();
    }   // invoke

}   // class QueryURICmd
