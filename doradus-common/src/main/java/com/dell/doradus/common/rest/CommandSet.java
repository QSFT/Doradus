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

/**
 * Describes REST commands, organized by owner. System commands are owned by the special
 * owner name "_system". All other commands are application-specific and owned by a
 * storage service, whose name is the owner.  
 */
package com.dell.doradus.common.rest;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;

public class CommandSet {
    private final Map<String, Map<String, CommandDescription>> m_commandMap = new HashMap<>();

    public CommandSet() {}
    
    //----- From/to UNode and JSON
    
    public static CommandSet fromJSON(String json) {
        return fromUNode(UNode.parseJSON(json));
    }
    
    public static CommandSet fromUNode(UNode rootNode) {
        CommandSet cmdSet = new CommandSet();
        Utils.require(rootNode.getName().equals("commands"), "'commands' expected: " + rootNode);
        for (UNode childNode : rootNode.getMemberList()) {
            cmdSet.addOwner(childNode);
        }
        return cmdSet;
    }
    
    public UNode toUNode() {
        UNode rootNode = UNode.createMapNode("commands");
        for (String ownerName : m_commandMap.keySet()) {
            Map<String, CommandDescription> ownerMap = m_commandMap.get(ownerName);
            UNode ownerNode = rootNode.addMapNode(ownerName);
            for (String cmdName : ownerMap.keySet()) {
                CommandDescription cmd = ownerMap.get(cmdName);
                ownerNode.addChildNode(cmd.toDoc());
            }
        }
        return rootNode;
    }
    
    public String toJSON() {
        return toUNode().toJSON();
    }
    
    //----- Setters
    
    public void addOwnerCommands(String cmdOwner, Map<String, CommandDescription> ownerMap) {
        m_commandMap.put(cmdOwner, ownerMap);
    }
    
    //----- Getters
    
    public Set<String> getOwners() {
        return m_commandMap.keySet();
    }
    
    public Map<String, CommandDescription> getCommands(String owner) {
        return m_commandMap.get(owner);
    }
    
    //----- Private methods
    
    private void addOwner(UNode ownerNode) {
        String ownerName = ownerNode.getName();
        Map<String, CommandDescription> ownerMap = new HashMap<>();
        m_commandMap.put(ownerName, ownerMap);
        for (UNode cmdNode : ownerNode.getMemberList()) {
            CommandDescription cmd = CommandDescription.fromUNode(cmdNode);
            ownerMap.put(cmd.getName(), cmd);
        }
    }

}
