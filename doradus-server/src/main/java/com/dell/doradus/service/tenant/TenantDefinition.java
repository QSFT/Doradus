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

package com.dell.doradus.service.tenant;

import java.util.HashMap;
import java.util.Map;

import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;

public class TenantDefinition {
    // Members:
    private String                    m_name;
    private final Map<String, String> m_users = new HashMap<>();
    private final Map<String, String> m_options = new HashMap<>();

    public void setName(String tenantName) {
        m_name = tenantName;
    }   // setName
    
    /**
     * Parse the tenant definition rooted at given UNode tree and update this object to
     * match. The root node is the "tenant" object, so its name is the tenant name and its
     * child nodes are tenant definitions such as "users" and "options". An exception is
     * thrown if the definition contains an error.
     *  
     * @param tenantNode    Root of a UNode tree that defines a tenant. The node must be a
     *                      MAP whose name is the tenantname.
     */
    public void parse(UNode tenantNode) {
        assert tenantNode != null;
        
        // Root object must be a MAP.
        Utils.require(tenantNode.isMap(), "'tenant' definition must be a map of unique names: " + tenantNode);
        m_name = tenantNode.getName();
        
        for (String childName : tenantNode.getMemberNames()) {
            UNode childNode = tenantNode.getMember(childName);
            switch (childNode.getName()) {
            case "options":
                Utils.require(childNode.isMap(), "'options' value must be a map of unique names: " + childNode);
                for (String optName : childNode.getMemberNames()) {
                    UNode optNode = childNode.getMember(optName);
                    Utils.require(optNode.isValue(), "'option' must be a value: " + optNode);
                    m_options.put(optNode.getName(), optNode.getValue());
                }
                break;

            case "users":
                Utils.require(childNode.isMap(), "'users' value must be a map of unique names: " + childNode);
                for (UNode userNode : childNode.getMemberList()) {
                    parseUser(userNode);
                }
                break;
                
            default:
                Utils.require(false, "Unknown tenant property: " + childNode);
            }
        }            
    }   // parse

    public String getName() {
        return m_name;
    }
    
    public Map<String, String> getOptions() {
        return m_options;
    }
    
    public Map<String, String> getUsers() {
        return m_users;
    }
    
    /**
     * Return this Tenant definition as a UNode tree, which can be serialized into text
     * via {@link UNode#toJSON()} or {@link UNode#toXML()}.
     * 
     * @return  This Tenant definition serialized as a UNode tree.
     */
    public UNode toDoc() {
        UNode tenantNode = UNode.createMapNode(m_name, "tenant");
        if (m_options.size() > 0) {
            UNode optsNode = tenantNode.addMapNode("options");
            for (String optName : m_options.keySet()) {
                optsNode.addValueNode(optName, m_options.get(optName), "option");
            }
        }
        if (m_users.size() > 0) {
            UNode usersNode = tenantNode.addMapNode("users");
            for (String userid : m_users.keySet()) {
                UNode userNode = usersNode.addMapNode(userid, "user");
                String password = m_users.get(userid);
                if (!Utils.isEmpty(password)) {
                    userNode.addValueNode("password", password, true);
                }
            }
        }
        return tenantNode;
    }   // toDoc
    
    // For debugging
    @Override
    public String toString() {
        return "Tenant: " + m_name;
    }
    
    //----- Private methods
    
    // Parse the given "user" node in a Tenant definition
    private void parseUser(UNode userNode) {
        Utils.require(userNode.isMap(), "User node should be a map of unique values: " + userNode);
        String userID = userNode.getName();
        String password = null;
        for (String childName : userNode.getMemberNames()) {
            UNode childNode = userNode.getMember(childName);
            switch (childNode.getName()) {
            case "password":
                Utils.require(childNode.isValue(), "'password' must be a simple value: " + childNode);
                password = childNode.getValue();
                break;
                
            default:
                Utils.require(false, "Unknown 'user' property: " + childNode);
            }
        }
        Utils.require(!Utils.isEmpty(password), "Password cannot be empty; user=" + userID);
        m_users.put(userID, password);
    }   // parseUser
    
}   // class TenantDefinition
