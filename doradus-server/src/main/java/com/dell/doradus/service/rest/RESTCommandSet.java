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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import com.dell.doradus.common.HttpMethod;
import com.dell.doradus.common.Utils;
import com.dell.doradus.common.rest.CommandDescription;
import com.dell.doradus.common.rest.CommandSet;
import com.dell.doradus.service.StorageService;

/**
 * Holds a collection of {@link RESTCommand}s and organizes them in the correct order for
 * evaluation. Each command has an "owner", which is global or a storage service name.
 * Within each owner, commands are grouped by method and sorted by evaluation order, For
 * example, suppose the following commands are registered to the same owner:
 * <pre>
 *      GET /{application}/{shard}
 *      GET /{application}/_shards
 * <pre>
 * The second command must be matched before the first command so that "_shards" isn't
 * evaluated as a {shard} name.
 * <p>
 * Owners can be arranged hierarchically so that one owner "extends" the commands of
 * another owner. Commands are registered by calling {@link #addCommands(String, Iterable)}.
 * An owner is registered as extending another owner by calling
 * {@link #setParent(String, String)}. A REST request is matched to a RESTCommand by the
 * following process: 
 * <ul>
 * <li>{@link #findMatch()} is called passing the starting owner name plus other
 *      parameters about the REST request.
 * <li>If a matching RESTCommand is found belonging to the given owner, it is returned.
 * <li>If no match is found but the specified owner has a parent, the search continues in
 *     the owner's command set.
 * <li>If the hierarchy is searched and no match is found, {@link #findMatch()} returns
 *     null, indicating that no matching RESTCommand was found.
 * </ul>
 */
public class RESTCommandSet {
    private static final String SYSTEM_OWNER = "_";
    
    // Map of command owner -> HTTP method -> RESTCommand. Note that RESTCommand's
    // compareTo() method sorts commands in the proper sequence for each owner/method.
    private final Map<String, Map<String, SortedSet<RESTCommand>>> m_cmdMap = new HashMap<>();

    // Map of command owners to parent command owners.
    private final Map<String, String> m_parentMap = new HashMap<String, String>();
    
    // When this is true, new commands are not allowed. This allows us to search the
    // command set without a lock after it's built.
    private boolean m_bCmdSetIsFrozen;

    //----- New experimental maps -----
    // Commands by owner and name:
    private final Map<String, Map<String, Xyzzy>> m_cmdsByOwnerMap = new HashMap<>();
    
    // Commands by owner and HttpMethod, sorted by evaluation order:
    private final Map<String, Map<HttpMethod, SortedSet<Xyzzy>>> m_cmdEvalMap = new HashMap<>();
    
    /**
     * Create a new REST command set with no commands.
     */
    public RESTCommandSet() { }
    
    // Experimental
    public void addCommands(StorageService storageService,
                            Iterable<Class<? extends RESTCallback>> cmdClasses) {
        if (m_bCmdSetIsFrozen) {
            throw new RuntimeException("New commands cannot be added: command set is frozen");
        }
        
        String cmdOwner = storageService == null ? "_system" : storageService.getClass().getSimpleName();
        synchronized (m_cmdEvalMap) {
            for (Class<? extends RESTCallback> cmdClass : cmdClasses) {
                Xyzzy xyzzy = new Xyzzy(cmdClass);
                addCommand(cmdOwner, xyzzy);
            }
        }
    }
    
    public CommandSet describeCommands() {
        CommandSet cmdSet = new CommandSet();
        for (String cmdOwner : m_cmdsByOwnerMap.keySet()) {
            Map<String, Xyzzy> cmdMap = m_cmdsByOwnerMap.get(cmdOwner);
            SortedMap<String, CommandDescription> ownerMap = new TreeMap<>();
            for (String cmdName : cmdMap.keySet()) {
                Xyzzy cmd = cmdMap.get(cmdName);
                CommandDescription cmdDesc = cmd.getDescription();
                if (cmdDesc.isVisible()) {
                    ownerMap.put(cmdName, cmdDesc);
                }
            }
            cmdSet.addOwnerCommands(cmdOwner, ownerMap);
        }
        return cmdSet;
    }

    /**
     * Add the given commands belonging to the given owner to this command set. If the
     * given owner is null, the commands are added to the "system" owner, which means they
     * are global. An exception is thrown if another command has already been added for
     * the same owner with the same method, nodes, and query parameter.
     * 
     * @param commandOwner  Name of service that owns the RESTCommands. If null or empty,
     *                      the commands are registered as global
     * @param commands      One or more {@link RESTCommand}s to add for the owner.
     */
    public void addCommands(String commandOwner, Iterable<RESTCommand> commands) {
        if (m_bCmdSetIsFrozen) {
            throw new RuntimeException("New commands cannot be added: command set is frozen");
        }

        // Get or create the map for this command's HTTP method.
        synchronized (m_cmdMap) {
            for (RESTCommand command : commands) {
                List<String> pathList = command.getPath();  
                String cmdOwner = (pathList.size() > 0 && pathList.get(0).equals("{application}")) ? commandOwner : SYSTEM_OWNER;
                String ownerKey = Utils.isEmpty(cmdOwner) ? SYSTEM_OWNER : cmdOwner;
                Map<String, SortedSet<RESTCommand>> ownerCmdSet = m_cmdMap.get(ownerKey);
                if (ownerCmdSet == null) {
                    ownerCmdSet = new HashMap<>();
                    m_cmdMap.put(ownerKey, ownerCmdSet);
                }
                SortedSet<RESTCommand> cmdSet = ownerCmdSet.get(command.getMethod());
                if (cmdSet == null) {
                    cmdSet = new TreeSet<RESTCommand>();
                    ownerCmdSet.put(command.getMethod(), cmdSet);
                }
                Utils.require(!cmdSet.contains(command),
                              "Duplicate REST command: Owner=%s, command=%s", ownerKey, command.toString());
                cmdSet.add(command);
            }
        }
    }   // addCommands
    
    /**
     * Define the parent of the given command owner name to the given parent owner name.
     * This enables command owners to be arranged hierarchically so that an owner
     * "inherits" its owners command. {@link #findMatch()} searches the hierarchy
     * automatically.
     *   
     * @param cmdOwnerName          Name of a command owner.
     * @param parentCmdOwnerName    Name of parent command owner.
     */
    public void setParent(String cmdOwnerName, String parentCmdOwnerName) {
        m_parentMap.put(cmdOwnerName, parentCmdOwnerName);
    }
    
    /**
     * Clear the registered REST command set.
     */
    public void clear() {
        synchronized (m_cmdMap) {
            m_cmdMap.clear();
            m_parentMap.clear();
        }
    }   // clear
    
    /**
     * Freeze or unfreeze the command set. This should be called after the command set has
     * been frozen so that findMatch() does not need to acquire a lock, thus single-
     * threading command searching. If this is called with "true", subsequent calls to
     * {@link #addCommand(RESTCommand)} will throw an exception.
     *  
     * @param bFreeze   True to freeze the command set. 
     */
    public void freezeCommandSet(boolean bFreeze) {
        m_bCmdSetIsFrozen = bFreeze;
    }   // freezeCommandSet

    // Experimental
    public Xyzzy findCommand(String ownerService, HttpMethod method, String uri, String query,
                             Map<String, String> variableMap) {
        String cmdOwner = Utils.isEmpty(ownerService) ? "_system" : ownerService;
        Xyzzy cmd = null;
        while (cmd == null && !Utils.isEmpty(cmdOwner)) {
            cmd = searchCommands(cmdOwner, method, uri, query, variableMap);
            cmdOwner = m_parentMap.get(cmdOwner);
        }
        return cmd;
    }

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
    public RESTCommand findMatch(String                ownerService,
                                 String                method,
                                 String                uri,
                                 String                query, 
                                 Map<String, String>   variableMap) {
        String ownerKey = Utils.isEmpty(ownerService) ? SYSTEM_OWNER : ownerService;
        RESTCommand cmd = null;
        while (cmd == null && !Utils.isEmpty(ownerKey)) {
            cmd = searchOwnerCommands(ownerKey, method, uri, query, variableMap);
            ownerKey = m_parentMap.get(ownerKey);
        }
        return cmd;
    }
    
    /**
     * Get all registered REST commands as a list of strings for debugging purposes.
     * 
     * @return  List of commands for debugging.
     */
    public Collection<String> getCommands() {
        List<String> commands = new ArrayList<String>();
        for (String cmdOwner : m_cmdMap.keySet()) {
            Map<String, SortedSet<RESTCommand>> ownerCmdMap = m_cmdMap.get(cmdOwner);
            for (String method: ownerCmdMap.keySet()) {
                for (RESTCommand cmd : ownerCmdMap.get(method)) {
                    commands.add(cmdOwner + ": " + cmd.toString());
                }
            }
        }
        return commands;
    }   // getCommands

    //----- Private methods
    
    private void addCommand(String cmdOwner, Xyzzy cmd) {
        Map<String, Xyzzy> nameMap = getCmdNameMap(cmdOwner);
        String cmdName = cmd.getName();
        Xyzzy oldCmd = nameMap.put(cmdName, cmd);
        if (oldCmd != null) {
            throw new RuntimeException("Duplicate REST command with same owner/name: " +
                                       "owner=" + cmdOwner + ", name=" + cmdName +
                                       ", [1]=" + cmd + ", [2]=" + oldCmd); 
        }
        
        Map<HttpMethod, SortedSet<Xyzzy>> evalMap = getCmdEvalMap(cmdOwner);
        for (HttpMethod method : cmd.getMethods()) {
            SortedSet<Xyzzy> methodSet = evalMap.get(method);
            if (methodSet == null) {
                methodSet = new TreeSet<>();
                evalMap.put(method, methodSet);
            }
            if (!methodSet.add(cmd)) {
                throw new RuntimeException("Duplicate REST command: owner=" + cmdOwner +
                                           ", name=" + cmdName + ", commmand=" + cmd); 
                
            }
        }
    }
    
    private Map<String, Xyzzy> getCmdNameMap(String cmdOwner) {
        Map<String, Xyzzy> cmdNameMap = m_cmdsByOwnerMap.get(cmdOwner);
        if (cmdNameMap == null) {
            cmdNameMap = new HashMap<>();
            m_cmdsByOwnerMap.put(cmdOwner, cmdNameMap);
        }
        return cmdNameMap;
    }

    private Map<HttpMethod, SortedSet<Xyzzy>> getCmdEvalMap(String cmdOwner) {
        Map<HttpMethod, SortedSet<Xyzzy>> evalMap = m_cmdEvalMap.get(cmdOwner);
        if (evalMap == null) {
            evalMap = new HashMap<>();
            m_cmdEvalMap.put(cmdOwner, evalMap);
        }
        return evalMap;
    }
    
    // Search the given command owner for a matching command.
    private Xyzzy searchCommands(String cmdOwner, HttpMethod method, String uri,
                                 String query, Map<String, String> variableMap) {
        Map<HttpMethod, SortedSet<Xyzzy>> evalMap = getCmdEvalMap(cmdOwner);
        if (evalMap == null) {
            return null;
        }
        
        // Find the sorted command set for the given HTTP method.
        SortedSet<Xyzzy> cmdSet = evalMap.get(method);
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
        for (Xyzzy cmd : cmdSet) {
            if (cmd.matches(pathNodeList, query, variableMap)) {
                return cmd;
            }
        }
        return null;
    }   // searchOwnerCommands
    
    // Search the given command owner for a matching command.
    private RESTCommand searchOwnerCommands(String ownerKey, String method, String uri,
                                            String query, Map<String, String> variableMap) {
        Map<String, SortedSet<RESTCommand>> ownerCmdMap = m_cmdMap.get(ownerKey);
        if (ownerCmdMap == null) {
            return null;
        }
        
        // Find the sorted command set for the given HTTP method.
        SortedSet<RESTCommand> cmdSet = ownerCmdMap.get(method.toUpperCase());
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
        return null;
    }   // searchOwnerCommands

}   // class RESTCommandSet

