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

import com.dell.doradus.common.AggregateResult;
import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.HttpMethod;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.rest.CommandParameter;
import com.dell.doradus.service.rest.UNodeInOutCallback;
import com.dell.doradus.service.rest.annotation.Description;
import com.dell.doradus.service.rest.annotation.ParamDescription;

/**
 * Implements the REST commands: GET or PUT /{application}/{table}/_aggregate. The
 * aggregate query parameters are passed in an input entity.
 */
@Description(
    name="Aggregate",
    summary="Performs an aggregate query on the given application and table.",
    methods = HttpMethod.GET,
    uri = "/{application}/{table}/_aggregate",
    inputEntity = "aggregate-search",
    outputEntity = "results"
)
public class AggregateDocCmd extends UNodeInOutCallback {
    @ParamDescription
    public static CommandParameter describeParameter() {
        return new CommandParameter("aggregate-search")
                        .add("query", "text")
                        .add("grouping-fields", "text")
                        .add("metric", "text", true)
                        .add("shards", "text")
                        .add("shards-range", "text")
                        .add("pair", "text")
                        .add("x-shards", "text")
                        .add("x-shards-range", "text")
                        .add("composite-fields", "text")
                        .add("flat", "boolean");
    }
    
    @Override
    public UNode invokeUNodeInOut(UNode inNode) {
        ApplicationDefinition appDef = m_request.getAppDef();
        TableDefinition tableDef = m_request.getTableDef(appDef);
        UNode rootNode = UNode.parse(m_request.getInputBody(), m_request.getInputContentType());
        AggregateResult aggResult = OLAPService.instance().aggregateQueryDoc(tableDef, rootNode);
        return aggResult.toDoc();
    }   // invokeUNodeOut

}   // class AggregateDocCmd
