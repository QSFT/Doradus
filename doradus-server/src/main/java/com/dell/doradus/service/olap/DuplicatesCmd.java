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

package com.dell.doradus.service.olap;

import java.util.Map;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.HttpMethod;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;
import com.dell.doradus.common.rest.CommandParameter;
import com.dell.doradus.search.SearchResultList;
import com.dell.doradus.service.rest.UNodeOutCallback;
import com.dell.doradus.service.rest.annotation.Description;
import com.dell.doradus.service.rest.annotation.ParamDescription;

/**
 * Handle the REST command: GET /{application}/{table}/_duplicates?{params}
 */
@Description(
    name = "Duplicates",
    summary = "Returns object IDs duplicated across a set of shards.",
    methods = HttpMethod.GET,
    uri = "/{application}/{table}/_duplicates?{params}",
    outputEntity = "results"
)
public class DuplicatesCmd extends UNodeOutCallback {
    @ParamDescription
    public static CommandParameter describeParams() {
        return new CommandParameter("params").add("range", "text", false);
    }

    @Override
    public UNode invokeUNodeOut() {
        ApplicationDefinition appDef = m_request.getAppDef();
        TableDefinition tableDef = m_request.getTableDef(appDef);
        String queryParams = m_request.getVariable("params");	// leave encoded
        Map<String, String> params = Utils.parseURIQuery(queryParams);
        String range = params.get("range");
        if (range == null) {
        	range = "";
        }

    	SearchResultList result = OLAPService.instance().getDuplicateIDs(appDef, tableDef.getTableName(), range);
        return result.toDoc();
    }   // invokeUNodeOut
}
