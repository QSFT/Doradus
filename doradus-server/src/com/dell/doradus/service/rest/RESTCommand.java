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

package com.dell.doradus.service.rest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dell.doradus.common.Utils;

/**
 * This class manages the mapping from a REST request to the handler callback. The request
 * consists of an HTTP method and a URI. The URI is a pattern that defines fixed and
 * variable parts. For example, consider the following command:
 * <pre>
 *      GET /{application}/{table}/_query?{query}
 * </pre>
 * This command matches HTTP GET requests that have 3 nodes in the URI path and a query
 * (?) parameter. The first node can be anything and the actual value passed will be stored
 * in the variable "application". The second node can also be anything and its value will
 * be stored in the variable "table". The third node must be the literal "_query"
 * (case-sensitive). The value of query component will be stored in the variable "query".
 * <p>
 * For example, if this command matches the following request:
 * <pre>
 *      GET /Magellan/documents/_query?q=foo+f=body
 * </pre>
 * The command will match, and it will return the variables:
 * <pre>      
 *      application = "Magellan"
 *      table = "documents"
 *      query = "q=foo+f=body"
 * </pre>
 * Note that path nodes and the query component, if any, are not decoded in case they contain
 * any escaped characters.
 * <p>
 * RESTCommand implements the {@link Comparable} interface, using a {@link #compareTo(RESTCommand)}
 * method that sorts commands in the proper evaluation sequence.
 */
public final class RESTCommand implements Comparable<RESTCommand> {
    // Components of a REST command:
    private String               m_method;          // Value for <method>
    private List<String>         m_pathNodes;       // Value for path nodes
    private String               m_query;           // Value for <query>
    private Class<RESTCallback>  m_callbackClass;   // Callback class for this command
    private boolean              m_bSystemCmd;      // true for non-tenant commands.
    
    /**
     * Creates a RESTCommand from a "REST rule", which includes the HTTP method, URI, and
     * callback method. The REST rule must have 3 space-separated components in the
     * following format:
     * <pre>
     *      method URI callback
     * </pre>
     * where method is an HTTP method such as GET or PUT; URI is a path/query pattern,
     * optionally with variable components; and callback is the full package path of the
     * class that handles requests. For example, a typically rule string is:
     * <pre>
     *      GET /{application}/{table}/_query?{query} com.dell.doradus.service.foo.QueryCmd
     * </pre>
     * The command class must be derived from {@link RESTCallback}, and it must have a
     * zero-argument constructor. Using that constructor, an instance of the given command
     * class is created and used to process requests to the specified REST command.
     * <p>
     * This constructor creates a non-system command, which means it is executed in the
     * context of a specific tenant.
     * 
     * @param ruleString    REST request and command callback class in the form "method
     *                      URI callback".
     */
    public RESTCommand(String ruleString) {
        m_bSystemCmd = false;
        parseRuleString(ruleString);
    }   // constructor
    
    /**
     * Same as {@link #RESTCommand(String)} but optionally marks this command as a system
     * command, which means it is not executed in the context of a tenant.
     * 
     * @param ruleString        REST request and command callback class in the form
     *                          "method URI callback".
     * @param bSystemCommand    If true, marks this object as a non-system command.
     */
    public RESTCommand(String ruleString, boolean bSystemCommand) {
        m_bSystemCmd = bSystemCommand;
        parseRuleString(ruleString);
    }   // constructor

    /**
     * Indicate if this is a system command, which means it executes without a specific
     * tenant context.
     * 
     * @return  True if this is a system command.
     */
    public boolean isSystemCommand() {
        return m_bSystemCmd;
    }   // isSystemCommand
    
    /**
     * Return true if this object matches the given URI components. If it does, any
     * variables defined by the URI are extracted from the URL and placed in the given
     * map without decoding. If no match is found, the given map is unmodified.
     * 
     * @param method        HTTP method representing an HTTP verb.
     * @param pathNodeList  List of path nodes in order. For example, the path /A/B/C
     *                      should be passed as a size=3 list with the slashes removed.
     * @param query         Query parameter from HTTP request, if any. For example, if
     *                      the query parameter is "?foo=bar", the string "foo=bar" should
     *                      be passed for this parameter.
     * @param variableMap   Populated with *encoded* variable values, if any, if this
     *                      RESTCommand matches the given request line.
     * @return              True if this command the given request.
     */
    public boolean matchesURL(String                method,
                              List<String>          pathNodeList,
                              String                query,
                              Map<String, String>   variableMap) {
        if (!m_method.equalsIgnoreCase(method) || pathNodeList.size() != m_pathNodes.size()) {
            return false;
        }
        Map<String, String> matchedVarMap = new HashMap<>();
        for (int index = 0; index < pathNodeList.size(); index++) {
            if (!matches(pathNodeList.get(index), m_pathNodes.get(index), matchedVarMap)) {
                return false;
            }
        }
        if (matches(query, m_query, matchedVarMap)) {
            variableMap.putAll(matchedVarMap);
            return true;
        }
        return false;
    }   // matchesURL

    /**
     * Compare this RESTCommand to the given one. This method sorts commands by the proper
     * evaluation sequence. For example, consider the following two commands: 
     * <pre>
     *      GET /{application}/_statstatus
     *      GET /_applications/{application}
     * <pre>
     * The second command must appear before the first command because nodes with literal
     * values must appear before parameterized nodes. Similarly, a command with more nodes
     * but otherwise the same as another command must appear first.
     * 
     * @param  other Another {@link RESTCommand} to compaere to this one.
     * @return       A negative, zero, or positive value reflecting whether this object is
     *               less than, equal to, or greater than the given object.
     */
    @Override
    public int compareTo(RESTCommand other) {
        // Compare the node list for each object.
        for (int index = 0; index < Math.min(m_pathNodes.size(), other.m_pathNodes.size()); index++) {
            String node1 = m_pathNodes.get(index);
            String node2 = other.m_pathNodes.get(index);
            int diff = compareNodes(node1, node2);
            if (diff != 0) {
                return diff;    // this node decides it
            }
        }
        
        // Here, all common nodes are identical.
        if (m_pathNodes.size() < other.m_pathNodes.size()) {
            return 1;   // r2 has more nodes, so sort before r1
        }
        if (m_pathNodes.size() > other.m_pathNodes.size()) {
            return -1;  // r1 has more nodes, so sort before r2
        }
        
        // Path nodes are identical. It depends on the query parameter.
        int diff = compareNodes(this.getQuery(), other.getQuery());
        return diff;
    }   // compareTo

    /**
     * Returns a string representation of this RESTCommand in the form:
     * <pre>
     *      {method} /{path}/[?{query}] -> {command class}
     * </pre>
     * 
     * @return A string representation of this RESTCommand.
     */
    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append(m_method);
        buffer.append(" /");
        for (String node : m_pathNodes) {
            buffer.append(node);
            buffer.append("/");
        }
        if (m_query.length() > 0) {
            buffer.append("?");
            buffer.append(m_query);
        }
        buffer.append(" -> ");
        buffer.append(m_callbackClass.toString());
        return buffer.toString();
    }   // toString
    
    /**
     * Return true if the given object is a RESTCommand with the same method, path nodes,
     * and query component as this one.
     * 
     * @return  True if the given object is considered the same as this one.
     */
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof RESTCommand)) {
            return false;
        }

        // As we compare, a variable {foo} is considered identical to a variable {bar}
        // if they occur in the same spot.
        RESTCommand otherCmd = (RESTCommand)other;
        if (!m_method.equalsIgnoreCase(otherCmd.m_method)) {
            return false;   // Different method
        }
        if (m_pathNodes.size() != otherCmd.m_pathNodes.size()) {
            return false;   // Different # of nodes
        }
        for (int index = 0; index < m_pathNodes.size(); index++) {
            if (!samePattern(m_pathNodes.get(index), otherCmd.m_pathNodes.get(index))) {
                return false;   // This node is different
            }
        }
        return samePattern(m_query, otherCmd.m_query);
    }   // equals
    
    /**
     * Provide a hash code that corresponds to the {@link #equals(Object)} method,
     * allowing this object to be used in hash maps, for example.
     * 
     * @return A hashcode unique to this object.
     */
    @Override
    public int hashCode() {
    	int code = m_method.hashCode();
    	String query = m_query.length() > 0 && m_query.startsWith("{") ? "{" : m_query;
   		code ^= query.hashCode();
   		for (String partNode : m_pathNodes) {
   			if (partNode.length() > 0 && partNode.startsWith("{")) {
   				partNode = "{";
   			}
   			code ^= partNode.hashCode();
   		}
    	return code;
    }	// hashCode
    
    // Getters
    
    /**
     * Return this command's HTTP method (e.g., GET, PUT). It is always uppercase.
     * 
     * @return  This command's HTTP method.
     */
    public String getMethod() {
        return m_method;
    }   // getMethod
    
    /**
     * Return this command's URI path, which always begins with a '/' and consists of
     * path nodes separated by '/'. The query component is not included.
     * 
     * @return  This command's URI path.
     */
    public List<String> getPath() {
        return m_pathNodes;
    }   // getApplication
    
    /**
     * Return this command's query component, if any.
     * 
     * @return  This command's query component. It is empty (but not null) if this command
     *          has no query component.
     */
    public String getQuery() {
        return m_query;
    }   // getQuery
    
    /**
     * Create a new instance of this command's callback object, which is invoked to handle
     * requests to corresponding REST requests.
     * 
     * @return  A new instance of this command's callback object.
     */
    public RESTCallback getNewCallback(RESTRequest request) {
        try {
            RESTCallback callback = m_callbackClass.newInstance();
            callback.setRequest(request);
            return callback;
        } catch (Exception e) {
            throw new RuntimeException("Unable to invoke callback", e);
        }
    }   // getCallback
    
    ///// Private methods
    
    // Parse the given rule string and map to member variables.
    @SuppressWarnings("unchecked")
    private void parseRuleString(String ruleString) {
        String[] parts = ruleString.split(" +");
        Utils.require(parts.length == 3, "Invalid rule format: " + ruleString);
        
        // Method
        m_method = parts[0].toUpperCase();
        
        // URI and query
        List<String> pathNodeList = new ArrayList<String>();
        StringBuilder query = new StringBuilder();
        StringBuilder fragment = new StringBuilder();
        Utils.parseURI(parts[1], pathNodeList, query, fragment);
        if (pathNodeList.size() == 0) {
            throw new IllegalArgumentException("Invalid URI path: " + parts[1]);
        }
        m_pathNodes = new ArrayList<>();
        for (String encodedNode : pathNodeList) {
            m_pathNodes.add(Utils.urlDecode(encodedNode));
        }
        m_query = Utils.urlDecode(query.toString());
        
        // Command class
        String cmdClassPath = parts[2];
        try {
            m_callbackClass = (Class<RESTCallback>) Class.forName(cmdClassPath);
        } catch (Exception e) {
            throw new RuntimeException("Error instantiating callback object '" + cmdClassPath + "'", e);
        }
    }   // parseRuleString 
    
    // Extract the variable name from the given URI component value. For example, if the
    // value is {application}, the variable name "application" is returned. An error is
    // thrown if the trailing '}' is missing.
    private static String getVariableName(String value) {
        assert value.charAt(0) == '{';
        assert value.charAt(value.length() - 1) == '}';
        return value.substring(1, value.length() - 1);
    }   // getVariableName
    
    // When comparing path nodes or query parts, two values are considered equal if either
    // (1) they are both empty, (2) they both start with '{' or, (3) neither start with
    // '{' and they have the same value (case-sensitive).
    private static boolean samePattern(String value1, String value2) {
        if (value1.length() == 0) {
            return value2.length() == 0;
        }
        if (value1.charAt(0) == '{') {
            return value2.length() > 0 && value2.charAt(0) == '{';
        }
        return value1.equals(value2);
    }   // samePattern

    // Similar to samePattern(), except that we are matching a candidate URI component
    // value to a component. If a match is made and the component is a variable, the
    // variable value is extracted and added to the given map.
    private static boolean matches(String value, String component, Map<String, String> variableMap) {
        if (component.length() == 0) {
            return value.length() == 0;
        }
        if (component.charAt(0) == '{') {
            // The component is a variable, so it always matches.
            String varName = getVariableName(component);
            variableMap.put(varName, value);
            return true;
        }
        return value.equals(component);
    }   // matches
    
    // Compare the given nodes and return -1 if the first node should appear first, 1 if
    // the second should appear first, and 0 if they are identical. The nodes can be
    // path nodes are query parameters. Either node can be empty, but not null.
    private static int compareNodes(String node1, String node2) {
        assert node1 != null;
        assert node2 != null;
        
        if (node1.equals(node2)) {
            return 0;
        }
        if (node1.length() > 0 && node1.charAt(0) == '{') {
            if (node2.length() > 0 && node2.charAt(0) == '{') {
                return 0;  // Both nodes are parameters; names are irrelevant
            }
            return 1;  // r1 is a parameter but r2 is not, so r2 should come first
        }
        if (node2.length() > 0 && node2.charAt(0) == '{') {
            return -1;   // r2 is a parameter but r1 is not, so r1 should come first
        }
        return node1.compareTo(node2);  // neither node is a parameter
    }   // compareNodes

}   // class RESTCommand
