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
 * Describes a Doradus REST command. Each command has the following attributes:
 * <ul>
 * <li><b>Name</b>: A simple name such as "Add" or "Query" that identifies the command.
 *     Names owned by the same owner ("_system" or storage service) are unique.
 * <li><b>Summary</b>: A human-readable description of the command.
 * <li><b>Methods</b>: The {@link HttpMethod}s that can be used to call the command. For
 *     example, some commands can be called using by either PUT or POST.
 * <li><b>URI</b>: This is the commands HTTP URI. Parameters are denoted by names enclosed
 *     in curly braces. Example: /{application}/_query?{params}.
 * <li><b>Privileged</b>: This Boolean attribute indicates if the command is privileged.
 *     When true, the command <i>may</i> require special credentials to execute depending
 *     on Doradus server configuration.
 * <li><b>Visible</b>: This Boolean attribute indicates if the command should be visible
 *     to client-level interfaces. When true, the command is hidden, e.g., because there
 *     is an alternate command that is preferable.
 * <li><b>Input entity</b>: When present, this attribute indicates that the command
 *     expects an input message. Its value is the root node name of the input message,
 *     e.g., "batch". The parameter may be formally described in the parameter list.
 * <li><b>Output entity</b>: When present, this attribute indicates that the command
 *     returns an output message. Its value is the root node name of output message, e.g.,
 *     "batch-results".
 * <li><b>Parameters</b>: When a command has input parameters in the URI and/or an input
 *     entity, they may be described via {@link RESTParameter}s. If a parameter is not
 *     described, it is considered text such as an application name. When a parameter is
 *     defined, its {@link RESTParameter} indicates its type and other attributes.
 * </ul> 
 */
public class RESTCommand {
    // Command attributes:
    private String m_name;
    private String m_summary;
    private final List<HttpMethod> m_methods = new ArrayList<>();
    private String m_uri;
    private boolean m_isPrivileged;
    private boolean m_isVisible;
    private String m_inputEntity;
    private String m_outputEntity;
    private final List<RESTParameter> m_parameters = new ArrayList<>();

    /**
     * Create a new RESTCommand with all null attributes.
     */
    public RESTCommand() { }

    //----- From/to UNode and JSON
    
    /**
     * Create a RESTCommand from a serialized JSON message. This is a convenience method
     * that calls {@link UNode#parseJSON(String)} followed by {@link #fromUNode(UNode)}.
     * 
     * @param json  Serialized RESTCommand as a JSON message.
     * @return      New RESTCommand object.
     */
    public static RESTCommand fromJSON(String json) {
        return fromUNode(UNode.parseJSON(json));
    }
    
    /**
     * Create a RESTCommand from a UNode tree using the given root node. The UNode tree
     * should come from a RESTCommand that was serialized from a {@link #toJSON()} call
     * that was then parsed using {@link UNode} parse method. An exception is thrown if a
     * parsing error occurs. 
     *  
     * @param rootNode  Root node of the deserialized {@link UNode} tree.
     * @return          New RESTCommand object.
     */
    public static RESTCommand fromUNode(UNode rootNode) {
        String name = rootNode.getName();
        Utils.require(!Utils.isEmpty(name), "Missing command name: " + rootNode.toString());
        RESTCommand cmd = new RESTCommand();
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
                    RESTParameter param = RESTParameter.fromUNode(paramNode);
                    cmd.addParameter(param);
                }
                break;
            case "privileged":
                cmd.setPrivileged(Boolean.parseBoolean(nodeValue));
                break;
            case "summary":
                cmd.setSummary(nodeValue);
                break;
            case "uri":
                cmd.setURI(nodeValue);
                break;
            default:
                Utils.require(false, "Unknown command property: " + childNode);
            }
        }
        return cmd;
    }
    
    /**
     * Serialize this RESTCommand into a {@link UNode} tree and return the root node.
     * The tree can be serialized into JSON or XML using {@link UNode#toJSON()} or
     * {@link UNode#toXML()}.
     * 
     * @return  Root node of a {@link UNode} tree.
     */
    public UNode toDoc() {
        UNode rootNode = UNode.createMapNode(m_name);
        if (!Utils.isEmpty(m_summary)) {
            rootNode.addValueNode("summary", m_summary);
        }
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
            for (RESTParameter param : m_parameters) {
                paramsNode.addChildNode(param.toDoc());
            }
        }
        return rootNode;
    }
    
    /**
     * Serialize this command into a JSON document. This is a convenience method that
     * calls {@link #toDoc()} followed by {@link UNode#toJSON()}.
     * 
     * @return  This command serialized as a JSON document.
     */
    public String toJSON() {
        return toDoc().toJSON();
    }
    
    //----- Setters
    
    /**
     * Set this command's simple name.
     * 
     * @param name  A unique name within the owner such as "Add" or "Query".
     */
    public void setName(String name) {
        m_name = name;
    }
    
    /**
     * Set this command's summary text.
     * 
     * @param name  A short descriptive text such as "This command performs an object
     *              query against a given table."
     */
    public void setSummary(String summary) {
        m_summary = summary;
    }

    /**
     * Set this command's URI.
     * 
     * @param uri   A URI pattern such as "/{application}/_query?{params}/.
     */
    public void setURI(String uri) {
        m_uri = uri;
    }
    
    /**
     * Set this command's privileged flag.
     * 
     * @param isPrivileged  True if this command is considered privileged.
     */
    public void setPrivileged(boolean isPrivileged) {
        m_isPrivileged = isPrivileged;
    }
    
    /**
     * Set this command's visibility flag.
     * 
     * @param isVisible True if this command should be visible to client-level interfaces.
     *                  False means an alternate command is preferable over this one.
     */
    public void setVisibility(boolean isVisible) {
        m_isVisible = isVisible;
    }
    
    /**
     * Set this command's input entity name.
     * 
     * @param inputEntity   Name of the root node of the expected input entity (e.g.,
     *                      "batch". When null (or never set), the command does not
     *                      require an input entity.
     */
    public void setInputEntity(String inputEntity) {
        m_inputEntity = inputEntity;
    }
    
    /**
     * Set this command's output entity name.
     * 
     * @param outputEntity  Name of the root node of the output entity returned by this
     *                      command. When null (or never set), the command does not return
     *                      an output entity.
     */
    public void setOutputEntity(String outputEntity) {
        m_outputEntity = outputEntity;
    }
    
    /**
     * Set the {@link HttpMethod}s that can be used to invoke this command. Most commands
     * have a single HTTP method.
     * 
     * @param methods   One of more {@link HttpMethod}s that can be used to invoke this
     *                  command. 
     */
    public void setMethods(HttpMethod[] methods) {
        m_methods.clear();
        for (HttpMethod method : methods) {
            m_methods.add(method);
        }
    }
    
    /**
     * Add a {@link RESTParameter} that describes an input URI parameter or input entity
     * parameter used by this command.
     * 
     * @param param A {@link RESTParameter} that describes a URI parameter or the input
     *              entity.
     */
    public void addParameter(RESTParameter param) {
        m_parameters.add(param);
    }
    
    //----- Getters
    
    /**
     * Get this command's friendly name, which will be unique amoung command owners.
     * 
     * @return  A friendly name such as "Add" or "DefineApp".
     */
    public String getName() {
        return m_name;
    }
    
    /**
     * Get this command's description text.
     * 
     * @return  A short comment such as "This command returns statistics for a specified
     *          shard".
     */
    public String getSummary() {
        return m_summary;
    }
    
    /**
     * Get the methods with which this command can be invoked. Most commands have a single
     * method, but some allow both PUT and POST, for example.
     * 
     * @return  List of {@link HttpMethod}s that this command allows.
     */
    public Iterable<HttpMethod> getMethods() {
        return m_methods;
    }
    
    /**
     * Get this command's URI, which is a pattern that may include literals and
     * parameters.
     * 
     * @return  A URI pattern such as "/{application}/_query?{params}".
     */
    public String getURI() {
        return m_uri;
    }
    
    /**
     * Indicate if this command is considered privileged.
     * 
     * @return  True if this command is considered privileged and therefore may require
     *          superuser credentials to execute.
     */
    public boolean isPrivileged() {
        return m_isPrivileged;
    }
    
    /**
     * Indicate if this command is considered visble.
     * 
     * @return  True if this command is should be visible to client-level interfaces.
     *          When false, generally an alternate command is preferable.
     */
    public boolean isVisible() {
        return m_isVisible;
    }
    
    /**
     * Get this command's input entity attribute. When null, the command does not require
     * an input entity. When non-null, it describes the input entity's root node name.
     * 
     * @return  This command's input entity, possibly null.
     */
    public String getInputEntity() {
        return m_inputEntity;
    }
    
    /**
     * Get this command's output entity attribute. When null, the command does not return
     * an output entity. When non-null, it describes the output entity's root node name.
     * 
     * @return  This command's output entity, possibly null.
     */
    public String getOutputEntity() {
        return m_outputEntity;
    }
    
    /**
     * Get the {@link RESTParameter}s that describe this command's parameters, if any. Any
     * parameters described in the URI or input entity that are not represented with a
     * {@link RESTParameter} are considered required text fields. A {@link RESTParameter}
     * is used to define compound parameters, an alternate parameter type, and other
     * parameter attributes.
     * 
     * @return  This command's defined parameters as a Collection of
     *          {@link RESTParameter}s. The collection may be empty but not null.
     *          
     */
    public Collection<RESTParameter> getParameters() {
        return m_parameters;
    }
    
    /**
     * Get this command's {@link HttpMethod}s as a comma-separated list.
     * 
     * @return  A single name such as "GET" or a comma-separated list such as
     *          "PUT,POST".
     */
    public String getMethodList() {
        List<String> methodList = new ArrayList<String>();
        for (HttpMethod method : m_methods) {
            methodList.add(method.name());
        }
        return Utils.concatenate(methodList, ",");
    }

    //----- Private methods
    
    // Set methods from a CSV list.
    private void setMethods(String methodList) {
        String[] methodNames = methodList.trim().split(",");
        for (String methodName : methodNames) {
            HttpMethod method = HttpMethod.valueOf(methodName.trim().toUpperCase());
            Utils.require(method != null, "Unknown REST method: " + methodName);
            m_methods.add(method);
        }
    }

}
