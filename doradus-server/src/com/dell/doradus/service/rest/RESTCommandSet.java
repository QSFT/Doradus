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
import java.util.SortedSet;
import java.util.TreeSet;

import com.dell.doradus.common.Utils;

/**
 * Holds a set of {@link RESTCommand}s and organizes them in the correct order for
 * evaluation. For example, if the command set contains the following two commands:
 * <pre>
 *      GET /{application}/_statstatus
 *      GET /_applications/{application}
 * <pre>
 * The second command must appear before the first command because nodes with literal
 * values must appear before parameterized nodes. Similarly, a command with more nodes
 * but otherwise the same as another command must appear first.
 * <p>
 * Commands are added by calling {@link #addCommand(RESTCommand)}. When
 * {@link #findMatch(String, String, String, Map)} is called, a search is made for a
 * matching command.
 */
public class RESTCommandSet {
    // RESTCommands organized by HTTP method. Note that RESTCommand's compareTo() method
    // does the work of sorting commands in the proper sequence.
    private final Map<String, SortedSet<RESTCommand>> m_cmdMap = new HashMap<>();

    /**
     * Create a new REST command set with no commands.
     */
    public RESTCommandSet() { }
    
    /**
     * Add the given command to this command set. An exception is thrown if another
     * command has already been added with the same method, nodes, and query parameter.
     * 
     * @param  command  {@link RESTCommand} to add to  this set.
     */
    public void addCommand(RESTCommand command) {
        assert command != null;
        
        // Get or create the map for this command's HTTP method.
        synchronized (m_cmdMap) {
            SortedSet<RESTCommand> cmdSet = m_cmdMap.get(command.getMethod());
            if (cmdSet == null) {
                cmdSet = new TreeSet<RESTCommand>();
                m_cmdMap.put(command.getMethod(), cmdSet);
            }
            Utils.require(!cmdSet.contains(command), "Duplicate REST command added: [" + command + "]");
            cmdSet.add(command);
        }
    }   // addCommand
    
    /**
     * Clear the registered REST command set.
     */
    public void clear() {
        synchronized (m_cmdMap) {
            m_cmdMap.clear();
        }
    }   // clear
    
    /**
     * Attempt to find a command that matches the given parameters. If a match is found,
     * parameters are extracted and added to the given parameter map. If no match is found.
     * null is returned and the given variableMap is left unmodified.
     * 
     * @param method        Request HTTP method (case-insensitive: GET, PUT, etc.)
     * @param uri           Request URI (case-sensitive: /A/B/C)
     * @param query         Optional query parameter. For example, if the query parameter
     *                      is "?foo=bar", the string "foo=bar".
     * @param variableMap   Populated with *encoded* variable values, if any, if a
     *                      command matches the given request parameters.
     * @return              Matching RESTCommand, if found, otherwise null.
     */
    public RESTCommand findMatch(String                method,
                                 String                uri,
                                 String                query, 
                                 Map<String, String>   variableMap) {
        // Find the sorted command set for the given HTTP method.
        synchronized (m_cmdMap) {
            SortedSet<RESTCommand> cmdSet = m_cmdMap.get(method.toUpperCase());
            if (cmdSet == null) {
                return null;
            }
            
            // Split uri into a list of non-empty nodes.
            List<String> pathNodeList = new ArrayList<>();
            String[] pathNodes = uri.split("/");
            for (String pathNode : pathNodes) {
                if (pathNode.length() > 0) {
                    pathNodeList.add(pathNode);
                }
            }
            
            // Attempt to match commands in this set in order.
            for (RESTCommand cmd : cmdSet) {
                if (cmd.matchesURL(method, pathNodeList, query, variableMap)) {
                    return cmd;
                }
            }
        }
        return null;
    }   // findMatch
    
    /**
     * Get all registered REST commands as a map of HTTP method names to a sorted set of
     * {@link RESTCommand}s. The commands are sorted in their evaluation order.
     * 
     * @return  Map of HTTP method-to-sorted commands. Example:
     * <pre>
     *          GET -> {"/foo/bar", "/foo"}
     *          PUT -> {"/foo/bar/bat", "/foo?{params}"}
     * </pre>
     */
    public Map<String, SortedSet<RESTCommand>> getCommands() {
        return m_cmdMap;
    }   // getCommands
    
}   // class RESTCommandSet

