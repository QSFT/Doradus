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

package com.dell.doradus.service.db;

import java.util.Arrays;
import java.util.List;

import com.dell.doradus.common.HttpMethod;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;
import com.dell.doradus.common.rest.RESTParameter;
import com.dell.doradus.olap.ParsedQuery;
import com.dell.doradus.service.Service;
import com.dell.doradus.service.rest.RESTService;
import com.dell.doradus.service.rest.UNodeOutCallback;
import com.dell.doradus.service.rest.annotation.Description;
import com.dell.doradus.service.rest.annotation.ParamDescription;
import com.dell.doradus.service.schema.SchemaService;

public class RestDBService extends Service {
    private static final RestDBService INSTANCE = new RestDBService();

    // Singleton creation only
    private RestDBService() {}

    public static RestDBService instance() {
        return INSTANCE;
    }
    
    @Override
    protected void initService() {
        RESTService.instance().registerCommands(Arrays.asList(GetColumnsCmd.class, GetRowsCmd.class));
    }

    @Override
    protected void startService() {
        SchemaService.instance().waitForFullService();
    }

    @Override
    protected void stopService() { }

    //----- StorageService: Schema update methods
    
    /**
     * Command: GET /_columns?{params}
     * params:
     * store - store name
     * row - row name
     * start - start column name to get (default is null which means start with the first column)
     * end - end column to get, not inclusive (default is null which means up to the last column)
     * count - maximum number of columns to get, default is 1024
     * columns - comma-separated list of column names (without spaces) to get (cannot be used together with start/end parameters)
     */
    @Description(
        name = "Get columns",
        summary = "Gets physical contents of the underlying DB storage",
        methods = HttpMethod.GET,
        uri = "_columns?{params}",
        outputEntity = "results"
    )
    public static class GetColumnsCmd extends UNodeOutCallback {
        @ParamDescription
        public static RESTParameter describeParams() {
            return new RESTParameter("params")
                        .add("store", "text", true)
                        .add("row", "text", true)
                        .add("start", "text")
                        .add("end", "text")
                        .add("columns", "text")
                        .add("count", "integer");
        }
        
        @Override public UNode invokeUNodeOut() {
            Tenant tenant = m_request.getTenant();
            ParsedQuery parsedQuery = new ParsedQuery(m_request.getVariable("params"));
            String store = parsedQuery.getString("store");
            String row = parsedQuery.getString("row");
            String start = parsedQuery.get("start");
            String end = parsedQuery.get("start");
            String columns = parsedQuery.get("start");
            int count = parsedQuery.getInt("count", 1024);
            parsedQuery.checkInvalidParameters();
            Utils.require((start == null && end == null) || (columns == null), "Columns parameter cannot be used together with start/end parameters");
            List<DColumn> result = null;
            if(columns != null) {
                result = DBService.instance(tenant).getColumns(store, row, Utils.split(columns, ','));
            } else {
                result = DBService.instance(tenant).getColumns(store, row, start, end, count);
            }
            UNode node = UNode.createArrayNode("columns");
            for(DColumn c: result) {
                String value = c.getValue();
                if(value.length() > 100) value = value.substring(0, 100) + "...";
                UNode cnode = node.addMapNode("column");
                cnode.addValueNode("name", c.getName());
                cnode.addValueNode("value", value);
            }
            return node;
        }
    }

    /**
     * Command: GET _rows/{store}?{params}
     * params:
     * c - continuation token which is last row name from the previous call, or null (default) for the first call
     * count - maximum number of rows to get, default is 1024
     */
    @Description(
        name = "Get rows",
        summary = "Gets physical contents of the underlying DB storage",
        methods = HttpMethod.GET,
        uri = "_rows?{params}",
        outputEntity = "results"
    )
    public static class GetRowsCmd extends UNodeOutCallback {
        @ParamDescription
        public static RESTParameter describeParams() {
            return new RESTParameter("params")
                        .add("store", "text", true)
                        .add("c", "text")
                        .add("count", "integer");
        }
        
        @Override public UNode invokeUNodeOut() {
            Tenant tenant = m_request.getTenant();
            ParsedQuery parsedQuery = new ParsedQuery(m_request.getVariable("params"));
            String store = parsedQuery.getString("store");
            String c = parsedQuery.get("c");
            int count = parsedQuery.getInt("count", 1024);
            parsedQuery.checkInvalidParameters();
            List<String> result = DBService.instance(tenant).getRows(store, c, count);
            UNode node = UNode.createArrayNode("rows");
            for(String r: result) {
                node.addValueNode("row", r);            }
            return node;
        }
    }
    

}