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

package com.dell.doradus.core;

import java.util.Map;
import java.util.TreeMap;

import com.dell.doradus.common.HttpMethod;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;
import com.dell.doradus.service.rest.UNodeOutCallback;
import com.dell.doradus.service.rest.annotation.Description;

@Description(
    name = "Config",
    summary = "Retrieves configuration and runtime parameters for this server process.",
    methods = {HttpMethod.GET},
    uri = "/_config",
    privileged = true,
    outputEntity = "configuration"
)
public class GetConfigCmd extends UNodeOutCallback {

    @SuppressWarnings("unchecked")
    @Override
    public UNode invokeUNodeOut() {
        UNode rootNode = UNode.createMapNode("configuration");
        String version = DoradusServer.getDoradusVersion();
        if (!Utils.isEmpty(version)) {
        	rootNode.addValueNode("version", version);
        }
        String[] args = ServerParams.instance().getCommandLineArgs();
        if (args != null) { 
	        UNode cmdlineArgsNode = rootNode.addMapNode("command-line-args");
	        for (int inx = 0; inx < args.length; inx++) {
	            String name = args[inx].substring(1);
	            String value = args[++inx];
	            cmdlineArgsNode.addValueNode(name, value, "arg");
	        }
        }
        Map<String, Object> serverConfigMap = new TreeMap<>(ServerParams.instance().toMap());
        if (serverConfigMap != null) { 
	        UNode propsNode = rootNode.addMapNode("server-params");
	        for (String moduleName : serverConfigMap.keySet()) {
	            UNode moduleNode = propsNode.addMapNode(moduleName);
	            Map<String, Object> paramMap = (Map<String, Object>) serverConfigMap.get(moduleName);
	            for (String paramName : paramMap.keySet()) {
	                Object paramValue = paramName.contains("password") ? "*****" : paramMap.get(paramName);
	                if (paramValue != null) {
	                    moduleNode.addValueNode(paramName, paramValue.toString(), "param");
	                }
	            }
			}
        }
       
        return rootNode;
    }   // invokeUNodeOut	

}   // GetConfigCmd
