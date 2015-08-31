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

/**
 * Describes a parameter used by a REST command. A parameter may reside in a command's
 * URI or input entity. A parameter used in a URI or input entity that is not otherwise
 * described is considered a required text parameter. A RESTParameter is used to describe
 * a parameter that has a different type such as "integer" or "boolean" and/or that is
 * optional.
 * <p>
 * A parameter may be compound, in which case it's type is null but it has child
 * parameters. For example, in the following URI:
 * <pre>
 *      /{application}/_query?{params}
 * </pre>
 * The {params} parameter may be compound, allowing arguments such as:
 * <pre>
 *      /MyApp/_query?q=Smith&s=100
 * </pre>
 * In this case, {params} will have a child parameter called 'q', which may be text and
 * required, and a child parameter called 's' perhaps integer and not required. In a URI,
 * child parameters are joined via "&".
 * <p>
 * When an input entity is compound (which is the typical case), the child parameters are
 * subelements of the parent element. For example, if {params} was an input entity, in
 * JSON equivalent of the previous example would look like this:
 * <pre>
 *      {"params": {
 *          "q": "Smith",
 *          "s": 100,
 *      }}
 * </pre> 
 */
public class RESTParameter {
    private String m_name;
    private String m_type;
    private boolean m_isRequired;
    private final List<RESTParameter> m_parameters = new ArrayList<>();
    
    /**
     * Create parameter will no initial attributes.
     */
    public RESTParameter() { }

    /**
     * Create a parameter with the given name. Other attributes will be null.
     * 
     * @param name  Parameter's name, which matches the name used in the corresponding
     *              command's URI or input entity.
     */
    public RESTParameter(String name) {
        m_name = name;
    }
    
    /**
     * Create a parameter with the given name and type. Other attributes will be null.
     * 
     * @param name  Parameter's name, which matches the name used in the corresponding
     *              command's URI or input entity.
     * @param type  Parameter's type such as "integer" or "boolean".
     */
    public RESTParameter(String name, String type) {
        m_name = name;
        m_type = type;
    }
    
    /**
     * Create a parameter with the given name, type, and required attribute.
     * 
     * @param name          Parameter's name, which matches the name used in the
     *                      corresponding command's URI or input entity.
     * @param type          Parameter's type such as "integer" or "boolean".
     * @param isRequired    True if the parameter is considered required.
     */
    public RESTParameter(String name, String type, boolean isRequired) {
        m_name = name;
        m_type = type;
        m_isRequired = isRequired;
    }
    
    //----- From/to UNode and JSON
    
    /**
     * Create a new RESTParameter object from the given JSON string. This is a convenience
     * method that calls {@link UNode#parseJSON(String)} followed by
     * {@link #fromUNode(UNode)}.
     * 
     * @param json  JSON text of a serialized RESTParameter.
     * @return      New RESTParameter created from text.
     */
    public static RESTParameter fromJSON(String json) {
        return fromUNode(UNode.parseJSON(json));
    }
    
    /**
     * Create a new RESTParameter object from the given UNode tree.
     * 
     * @param paramNode Root {@link UNode} of a parameter description.
     * @return          New RESTParameter created from UNode tree.
     */
    public static RESTParameter fromUNode(UNode paramNode) {
        RESTParameter param = new RESTParameter();
        String name = paramNode.getName();
        Utils.require(!Utils.isEmpty(name), "Missing parameter name: " + paramNode);
        param.setName(name);
        
        for (UNode childNode : paramNode.getMemberList()) {
            switch (childNode.getName()) {
            case "_required":
                param.setRequired(Boolean.parseBoolean(childNode.getValue()));
                break;
            case "_type":
                param.setType(childNode.getValue());
                break;
            default:
                // Ignore system properties we don't recognize.
                if (childNode.getName().charAt(0) != '_') {
                    param.add(RESTParameter.fromUNode(childNode));
                }
                break;
            }
        }
        return param;
    }
    
    /**
     * Serialize this RESTParameter into a UNode tree and return the root node.
     * 
     * @return  Root {@link UNode} of the serialized tree.
     */
    public UNode toDoc() {
        UNode paramNode = UNode.createMapNode(m_name);
        if (!Utils.isEmpty(m_type)) {
            paramNode.addValueNode("_type", m_type);
        }
        if (m_isRequired) {
            paramNode.addValueNode("_required", Boolean.toString(m_isRequired));
        }
        if (m_parameters.size() > 0) {
            for (RESTParameter param : m_parameters) {
                paramNode.addChildNode(param.toDoc());
            }
        }
        return paramNode;
    }
    
    /**
     * Serialize this RESTParameter into a JSON string. This is a convenience method that
     * calls {@link #toDoc()} followed by {@link UNode#toJSON()}.
     * 
     * @return  This RESTParameter serialized into a JSON string.
     */
    public String toJSON() {
        return toDoc().toJSON();
    }
    
    //----- Setters
    
    /**
     * Add the given RESTParameter as a child of this parameter. This parameter is
     * returned to support builder syntax.
     * 
     * @param childParam    New child RESTParameter of this parameter. The child parameter
     *                      name must be unique.
     * @return              This parameter object.
     */
    public RESTParameter add(RESTParameter childParam) {
        Utils.require(!Utils.isEmpty(childParam.getName()), "Child parameter name cannot be empty");
        m_parameters.add(childParam);
        return this;
    }
    
    /**
     * Add a new child parameter with the given name and type to this parameter. The child
     * parameter will not be marked as required. This parameter is returned to support
     * builder syntax.
     * 
     * @param childParamName    Name of new child parameter.
     * @param childParamType    Type of new child parameter.
     * @return                  This parameter object.
     */
    public RESTParameter add(String childParamName, String childParamType) {
        return add(new RESTParameter(childParamName, childParamType));
    }
    
    /**
     * Add a child parameter with the given name, type, and required status to this
     * parameter. This parameter is returned to support builder syntax.
     * 
     * @param childParamName    Name of new child parameter.
     * @param childParamType    Type of new child parameter.
     * @param isRequired        True if the child parameter is required.
     * @return                  This parameter object.
     */
    public RESTParameter add(String childParamName, String childParamType, boolean isRequired) {
        return add(new RESTParameter(childParamName, childParamType, isRequired));
    }
    
    /**
     * Set the name of this RESTParameter.
     * 
     * @param name  New parameter name. It should match a name used in the command's URI
     *              or input entity.
     */
    public void setName(String name) {
        m_name = name;
    }
    
    /**
     * Set the type of this RESTParameter.
     * 
     * @param type  RESTParameter's type. It should be a simple scalar name such as "text"
     *              or "integer". A compound parameter's type should be null.
     */
    public void setType(String type) {
        m_type = type;
    }
    
    /**
     * Set this RESTParameter's required attribute.
     * 
     * @param isRequired    True if the parameter is required.
     */
    public void setRequired(boolean isRequired) {
        m_isRequired = isRequired;
    }
    
    //----- Getters
    
    /**
     * Get this parameter's name.
     * 
     * @return  Parameter name.
     */
    public String getName() {
        return m_name;
    }
    
    /**
     * Get this parameter's type.
     * 
     * @return  Parameter type such as "text" or "integer". Will be empty for compound
     *          parameters.
     */
    public String getType() {
        return m_type;
    }
    
    /**
     * Get this parameter's children as an Iterable RESTParameter. If this parameter is
     * not compound, the iterator will be empty but not null.
     * 
     * @return  This parameter's children, none if it is not compound.
     */
    public Iterable<RESTParameter> getChildren() {
        return m_parameters;
    }
    
}
