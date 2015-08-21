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

package com.dell.doradus.common;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;

/**
 * Represents a defined user for a specific tenant. Holds the user's ID (name), password,
 * and defined permissions.
 */
public class UserDefinition {
    /**
     * The permissions that can be assigned to a user. A user with no explicit permission
     * is considered to have the ALL permission.
     */
    public enum Permission {
        /**
         * Can read and write data and add/update schemas.
         */
        ALL,
        
        /**
         * Can only append (POST) data. 
         */
        APPEND,
        
        /**
         * Can append, modify, or delete data.
         */
        UPDATE,
        
        /**
         * Can read (query) data.
         */
        READ,
    }
    private String m_userID;
    private String m_password;
    private final Set<Permission> m_permissions = new HashSet<Permission>();

    /**
     * Create a new user with all null values.
     */
    public UserDefinition() {}
    
    /**
     * Create a new object setting the user ID only.
     * 
     * @param userID    User ID (name) of new user.
     */
    public UserDefinition(String userID) {
        m_userID = userID;
    }
    
    //----- Getters
    
    /**
     * Get this user definition's ID, which is the logon name.
     * 
     * @return  This user definition's ID, if defined.
     */
    public String getID() {
        return m_userID;
    }
    
    /**
     * Get this user definition's password, if defined.
     * 
     * @return  The current password, if defined.
     */
    public String getPassword() {
        return m_password;
    }
    
    /**
     * Get this user definition's currently defined permissions, if any.
     * 
     * @return  Set of {@link Permission}s defined for this user. The set returned is
     *          read-only and may be empty but will not be null.
     */
    public Set<Permission> getPermissions() {
        return Collections.unmodifiableSet(m_permissions);
    }
    
    //----- Setters
    
    /**
     * Set this user's ID (name).
     * 
     * @param userID    New user ID.
     */
    public void setID(String userID) {
        m_userID = userID;
    }
    
    /**
     * Set this user definition's password.
     * 
     * @param password  New password.
     */
    public void setPassword(String password) {
        m_password = password;
    }
    
    /**
     * Add the given permission to this user definition. Permissions are stored as a
     * set, so setting the same permission twice is a no-op.
     * 
     * @param permission    New {@link Permission} to add for this user.
     */
    public void addPermission(Permission permission) {
        m_permissions.add(permission);
    }

    /**
     * Set this user's permissions to the given set. The current permissions, if any, are
     * replaced by the new set.
     * 
     * @param permissions   Set of new {@link Permission}s for the user.
     */
    public void setPermissions(Set<Permission> permissions) {
        m_permissions.clear();
        m_permissions.addAll(permissions);
    }

    /**
     * Make a copy of this user definition using the given ID for the copy.
     * 
     * @param newUserID New user ID for the copied user.
     * @return          New {@link UserDefinition} with this definition's password and
     *                  permissions but with the given user ID.
     */
    public UserDefinition makeCopy(String newUserID) {
        UserDefinition newUserDef = new UserDefinition(newUserID);
        newUserDef.setPassword(this.m_password);
        newUserDef.setPermissions(this.m_permissions);
        return newUserDef;
    }

    /**
     * Parse a user definition expressed as a UNode tree.
     * 
     * @param userNode  Root {@link UNode}, which must be a "user" node.
     */
    public void parse(UNode userNode) {
        m_userID = userNode.getName();
        m_password = null;
        m_permissions.clear();
        
        for (String childName : userNode.getMemberNames()) {
            UNode childNode = userNode.getMember(childName);
            switch (childNode.getName()) {
            case "password":
                Utils.require(childNode.isValue(), "'password' must be a simple value: " + childNode);
                m_password = childNode.getValue();
                break;
                
            case "permissions":
                Utils.require(childNode.isValue(), "'permissions' must be a list of values: " + childNode);
                String[] permissions = childNode.getValue().split(",");
                for (String permission : permissions) {
                    String permToken = permission.toUpperCase().trim();
                    try {
                        m_permissions.add(Permission.valueOf(permToken));
                    } catch (IllegalArgumentException e) {
                        Utils.require(false, "Unrecognized permission: %s; allowed values are: %s",
                                      permToken, Arrays.asList(Permission.values()).toString());
                    }
                }
                break;
                
            default:
                Utils.require(false, "Unknown 'user' property: " + childNode);
            }
        }
    }   // parse
    
    /**
     * Serialize this user definition into a UNode tree.
     * 
     * @return  Root {@link UNode} of the serialized tree.
     */
    public UNode toDoc() {
        UNode userNode = UNode.createMapNode(m_userID, "user");
        if (!Utils.isEmpty(m_password)) {
            userNode.addValueNode("password", m_password, true);
        }
        String permissions = "ALL";
        if (m_permissions.size() > 0) {
            permissions = Utils.concatenate(m_permissions, ",");
        }
        userNode.addValueNode("permissions", permissions);
        return userNode;
    }   // toDoc

}   // class UserDefinition
