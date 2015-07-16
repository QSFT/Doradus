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

import java.util.ArrayList;
import java.util.List;

import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;

public class CommandParameter {
    private String m_name;
    private String m_type;
    private boolean m_isRequired;
    private final List<CommandParameter> m_parameters = new ArrayList<>();
    
    public CommandParameter() { }

    public CommandParameter(String name) {
        m_name = name;
    }
    
    public CommandParameter(String name, String type) {
        m_name = name;
        m_type = type;
    }
    
    public CommandParameter(String name, String type, boolean isRequired) {
        m_name = name;
        m_type = type;
        m_isRequired = isRequired;
    }
    
    public CommandParameter add(String childParamName, String childParamType) {
        addParameter(new CommandParameter(childParamName, childParamType));
        return this;
    }
    
    public CommandParameter add(String childParamName, String childParamType, boolean isRequired) {
        addParameter(new CommandParameter(childParamName, childParamType, isRequired));
        return this;
    }
    
    //----- From/to UNode and JSON
    
    public static CommandParameter fromJSON(String json) {
        return fromUNode(UNode.parseJSON(json));
    }
    
    public static CommandParameter fromUNode(UNode paramNode) {
        CommandParameter param = new CommandParameter();
        String name = paramNode.getName();
        Utils.require(!Utils.isEmpty(name), "Missing parameter name: " + paramNode);
        param.setName(name);
        
        for (UNode childNode : paramNode.getMemberList()) {
            String value = childNode.getValue();
            switch (value) {
            case "children":
                for (UNode subParamNode : childNode.getMemberList()) {
                    CommandParameter subParam = CommandParameter.fromUNode(subParamNode);
                    param.addParameter(subParam);
                }
                break;
            case "required":
                param.setRequired(Boolean.parseBoolean(value));
                break;
            case "type":
                param.setType(value);
                break;
            default:
                Utils.require(false, "Unknown parameter property: " + childNode);
            }
        }
        return param;
    }
    
    public UNode toDoc() {
        UNode paramNode = UNode.createMapNode(m_name);
        if (!Utils.isEmpty(m_type)) {
            paramNode.addValueNode("type", m_type);
        }
        if (m_isRequired) {
            paramNode.addValueNode("required", Boolean.toString(m_isRequired));
        }
        if (m_parameters.size() > 0) {
            UNode paramsNode = paramNode.addMapNode("children");
            for (CommandParameter param : m_parameters) {
                paramsNode.addChildNode(param.toDoc());
            }
        }
        return paramNode;
    }
    
    public String toJSON() {
        return toDoc().toJSON();
    }
    
    //----- Setters
    
    public void addParameter(CommandParameter subParam) {
        m_parameters.add(subParam);
    }
    
    public void setName(String name) {
        m_name = name;
    }
    
    public void setType(String type) {
        m_name = type;
    }
    
    public void setRequired(boolean isRequired) {
        m_isRequired = isRequired;
    }
    
    //----- Getters
    
    public String getName() {
        return m_name;
    }
    
    public String getType() {
        return m_type;
    }
    
    public Iterable<CommandParameter> getChildren() {
        return m_parameters;
    }
    
}
