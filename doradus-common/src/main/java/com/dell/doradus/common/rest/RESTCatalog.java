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

package com.dell.doradus.common.rest;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;

/**
 * Describes a set of Doradus {@link RESTCommand}s and their owners. System commands are
 * invoked without an application context and are owned by the special owner name
 * {@value #SYSTEM_OWNER}. Application commands are owned by a storage service, whose name
 * is the owner.
 */
public class RESTCatalog {
    /**
     * Owner name used for system commands.
     */
    public static final String SYSTEM_OWNER = "_system";
    
    // Owner -> Name -> RESTCommand
    protected final Map<String, SortedMap<String, RESTCommand>> m_cmdsByOwnerMap = new HashMap<>();

    /**
     * Create an empty catalog.
     */
    public RESTCatalog() {}
    
    /**
     * Create a new catalog initialized with the given command map. Commands are
     * mapped as owner name -> command name -> {@link RESTCommand}.
     * 
     * @param ownerMap  Initial set of owners-to-commands.
     */
    public RESTCatalog(Map<String, SortedMap<String, RESTCommand>> ownerMap) {
        m_cmdsByOwnerMap.putAll(ownerMap);
    }
    
    //----- From/to UNode and JSON
    
    /**
     * Create a RESTCatalog from a serialized JSON document. This is a convenience
     * method that calls {@link UNode#parseJSON(String)} followed by
     * {@link #fromUNode(UNode)}.
     * 
     * @param json  Document originally created by {@link #toUNode()} followed by
     *              {@link UNode#toJSON()}. 
     * @return      New RESTCatalog parsed from the give JSON document.
     */
    public static RESTCatalog fromJSON(String json) {
        return fromUNode(UNode.parseJSON(json));
    }
    
    /**
     * Create a RESTCatalog serialized as a {@link UNode} tree. An exception is thrown if
     * a parsing error occurs.
     * 
     * @param rootNode  Root of a {@link UNode} true. 
     * @return          New RESTCatalog parsed from the give JSON document.
     */
    public static RESTCatalog fromUNode(UNode rootNode) {
        RESTCatalog cmdSet = new RESTCatalog();
        Utils.require(rootNode.getName().equals("commands"), "'commands' expected: " + rootNode);
        for (UNode childNode : rootNode.getMemberList()) {
            cmdSet.addOwner(childNode);
        }
        return cmdSet;
    }

    /**
     * Serialize this RESTCatalog into a UNode tree and return the root node. The UNode
     * tree can be serialized into into JSON or XML using {@link UNode#toJSON()} or
     * {@link UNode#toXML()}.
     * 
     * @return  Root node of the serialized {@link UNode} tree.
     */
    public UNode toUNode() {
        UNode rootNode = UNode.createMapNode("commands");
        for (String ownerName : m_cmdsByOwnerMap.keySet()) {
            Map<String, RESTCommand> ownerMap = m_cmdsByOwnerMap.get(ownerName);
            UNode ownerNode = rootNode.addMapNode(ownerName, "owner");
            for (String cmdName : ownerMap.keySet()) {
                RESTCommand cmd = ownerMap.get(cmdName);
                ownerNode.addChildNode(cmd.toDoc());
            }
        }
        return rootNode;
    }
    
    /**
     * Serialize this RESTCatalog as a JSON document. This is a convenience method that
     * calls {@link #toUNode()} followed by {@link UNode#toJSON()}.
     * 
     * @return  JSON document representing this RESTCatalog.
     */
    public String toJSON() {
        return toUNode().toJSON();
    }
    
    //----- Getters
    
    /**
     * Get the set of REST command owners used by this RESTCatalog. System commands are
     * owned by owner name {@value #SYSTEM_OWNER}. All other owners are storage service
     * names, which own application commands.  
     * 
     * @return  Set of owner names used by the RESTCatalog.
     */
    public Set<String> getOwners() {
        return m_cmdsByOwnerMap.keySet();
    }
    
    /**
     * Get the commands owned by a given owner as a map of command name to
     * {@link RESTCommand}. The given owner name should be one returned by
     * {@link #getOwners()}.
     * 
     * @param  owner    Name of a REST command owner.
     * @return          Map of command name to {@link RESTCommand}s owned by the given
     *                  owner, null if there are none.
     */
    public Map<String, RESTCommand> getCommands(String owner) {
        return m_cmdsByOwnerMap.get(owner);
    }
    
    //----- Private methods
    
    private void addOwner(UNode ownerNode) {
        String ownerName = ownerNode.getName();
        SortedMap<String, RESTCommand> ownerMap = new TreeMap<>();
        m_cmdsByOwnerMap.put(ownerName, ownerMap);
        for (UNode cmdNode : ownerNode.getMemberList()) {
            RESTCommand cmd = RESTCommand.fromUNode(cmdNode);
            ownerMap.put(cmd.getName(), cmd);
        }
    }

}
