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
import java.util.Collection;
import java.util.List;

import com.dell.doradus.common.HttpMethod;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;

/**
 * Describes a Doradus REST command.
 */
public class CommandDescription {
    private String m_name;
    private String m_uri;
    private final List<HttpMethod> m_methods = new ArrayList<>();
    private boolean m_isPrivileged;
    private boolean m_isVisible;
    private String m_inputEntity;
    private String m_outputEntity;
    private final List<CommandParameter> m_parameters = new ArrayList<>();

    public CommandDescription() { }

    //----- From/to UNode and JSON
    
    public static CommandDescription fromJSON(String json) {
        return fromUNode(UNode.parseJSON(json));
    }
    
    public static CommandDescription fromUNode(UNode rootNode) {
        String name = rootNode.getName();
        Utils.require(!Utils.isEmpty(name), "Missing command name: " + rootNode.toString());
        CommandDescription cmd = new CommandDescription();
        cmd.setName(name);
        for (UNode childNode : rootNode.getMemberList()) {
            String nodeValue = childNode.getValue();
            switch (childNode.getName()) {
            case "input-entity":
                cmd.setInputEntity(nodeValue);
                break;
            case "methods":
                cmd.setMethods(nodeValue);
                break;
            case "output-entity":
                cmd.setOutputEntity(nodeValue);
                break;
            case "parameters":
                for (UNode paramNode : childNode.getMemberList()) {
                    CommandParameter param = CommandParameter.fromUNode(paramNode);
                    cmd.addParameter(param);
                }
            case "uri":
                cmd.setURI(nodeValue);
                break;
            default:
                Utils.require(false, "Uknown command property: " + childNode);
            }
        }
        return cmd;
    }
    
    public UNode toDoc() {
        UNode rootNode = UNode.createMapNode(m_name);
        rootNode.addValueNode("uri", m_uri);
        rootNode.addValueNode("methods", getMethodList());
        if (m_isPrivileged) {
            rootNode.addValueNode("privileged", Boolean.toString(m_isPrivileged));
        }
        if (!Utils.isEmpty(m_inputEntity)) {
            rootNode.addValueNode("input-entity", m_inputEntity);
        }
        if (!Utils.isEmpty(m_outputEntity)) {
            rootNode.addValueNode("output-entity", m_outputEntity);
        }
        if (m_parameters.size() > 0) {
            UNode paramsNode = rootNode.addMapNode("parameters");
            for (CommandParameter param : m_parameters) {
                paramsNode.addChildNode(param.toDoc());
            }
        }
        return rootNode;
    }
    
    //----- Setters
    
    public void setInputEntity(String inputEntity) {
        m_inputEntity = inputEntity;
    }
    
    public void setOutputEntity(String outputEntity) {
        m_outputEntity = outputEntity;
    }
    
    public void setName(String name) {
        m_name = name;
    }
    
    public void setURI(String uri) {
        m_uri = uri;
    }
    
    public void setPrivileged(boolean isPrivileged) {
        m_isPrivileged = isPrivileged;
    }
    
    public void setVisibility(boolean isVisible) {
        m_isVisible = isVisible;
    }
    
    public void addMethods(HttpMethod[] methods) {
        for (HttpMethod method : methods) {
            m_methods.add(method);
        }
    }
    
    public void addParameter(CommandParameter param) {
        m_parameters.add(param);
    }
    
    //----- Getters
    
    public String getName() {
        return m_name;
    }
    public String getURI() {
        return m_uri;
    }
    
    public Collection<HttpMethod> getMethods() {
        return m_methods;
    }
    
    public boolean isPrivileged() {
        return m_isPrivileged;
    }
    
    public boolean isVisible() {
        return m_isVisible;
    }
    
    public String getInputEntity() {
        return m_inputEntity;
    }
    
    public String getOutputEntity() {
        return m_outputEntity;
    }
    
    public Collection<CommandParameter> getParameters() {
        return m_parameters;
    }
    
    public String toJSON() {
        return toDoc().toJSON();
    }
    
    public String getMethodList() {
        List<String> methodList = new ArrayList<String>();
        for (HttpMethod method : m_methods) {
            methodList.add(method.name());
        }
        return Utils.concatenate(methodList, ",");
    }

    //----- Private methods
    
    private void setMethods(String methodList) {
        String[] methodNames = methodList.trim().split(",");
        for (String methodName : methodNames) {
            HttpMethod method = HttpMethod.valueOf(methodName.trim().toUpperCase());
            Utils.require(method != null, "Unknown REST method: " + methodName);
            m_methods.add(method);
        }
    }

}
