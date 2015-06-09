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

import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;
import com.dell.doradus.service.rest.UNodeOutCallback;

/**
 * Reply to a REST command such as: GET /_config. 
 */
public class GetConfigCmd extends UNodeOutCallback {


    @Override
    public UNode invokeUNodeOut() {
        UNode rootNode = UNode.createMapNode("configuration");
        String version = DoradusServer.instance().getDoradusVersion();
        if (!Utils.isEmpty(version)) {
        	rootNode.addValueNode("version", version);
        }
        String[] args = ServerConfig.commandLineArgs;
        if (args != null) { 
	        UNode cmdlineArgsNode = rootNode.addMapNode("command-line-args");
	        for (int inx = 0; inx < args.length; inx++) {
	            String name = args[inx].substring(1);
	            String value = args[++inx];
	            cmdlineArgsNode.addValueNode(name, value, "arg");
	        }
        }
        Map<String, Object> serverConfigMap = new TreeMap<>(ServerConfig.getInstance().toMap());
        if (serverConfigMap != null) { 
	        UNode propsNode = rootNode.addMapNode("server-params");
	        for (String key : serverConfigMap.keySet()) {
	        	String value = key.contains("password") ? "*****": "" + serverConfigMap.get(key);
	        	if (value != null) {
	        		propsNode.addValueNode(key, value, "param");
	        	}
			}
        }
       
        return rootNode;
    }   // invokeUNodeOut	

}   // GetConfigCmd
