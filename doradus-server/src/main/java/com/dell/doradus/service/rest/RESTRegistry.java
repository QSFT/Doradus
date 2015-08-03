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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import com.dell.doradus.common.HttpMethod;
import com.dell.doradus.common.Utils;
import com.dell.doradus.common.rest.RESTCommand;
import com.dell.doradus.common.rest.RESTCatalog;
import com.dell.doradus.service.StorageService;
import com.dell.doradus.service.rest.annotation.Description;

/**
 * Holds a collection of REST Commands registered by Doradus components and organizes them
 * in the correct order for evaluation. This class extends {@link RESTCatalog}, adding an
 * "evaluation map" and an "owner hierarchy". The evaluation map groups commands by owner
 * and method and sorts them by evaluation order. For example, suppose the following
 * commands are registered to the same owner:
 * <pre>
 *      GET /{application}/{shard}
 *      GET /{application}/_shards
 * <pre>
 * The second command must be matched before the first command so that "_shards" isn't
 * evaluated as a {shard} name.
 * <p>
 * The owner hierarchy allows an owner to "extend" the commands of another owner. Commands
 * are first registered by calling {@link #registerCallbacks(String, Iterable)}. An owner can
 * register its commands as extending those of another owner by calling
 * {@link #setParent(String, String)}. A REST request is matched to a REST command by the
 * following process: 
 * <ul>
 * <li>{@link #findCommand()} is called passing the starting owner name plus other
 *      parameters about the REST request.
 * <li>If a matching REST command is found belonging to the given owner, it is returned.
 * <li>If no match is found but the specified owner has a parent, the search continues in
 *     the parent's command set.
 * <li>If the hierarchy is searched and no match is found, {@link #findCommand()} returns
 *     null, indicating that no matching REST command was found.
 * </ul>
 */
public class RESTRegistry extends RESTCatalog {
    // Commands by owner and HttpMethod, sorted by evaluation order:
    private final Map<String, Map<HttpMethod, SortedSet<RegisteredCommand>>> m_cmdEvalMap = new HashMap<>();
    
    // Map of command owners to parent command owners.
    private final Map<String, String> m_parentMap = new HashMap<String, String>();
    
    // When this is true, new commands are not allowed. This allows us to search the
    // command set without a lock after it's built.
    private boolean m_bCmdSetIsFrozen;
    
    /**
     * Create a new RESTRegistry set with no commands.
     */
    public RESTRegistry() { }
    
    /**
     * Add a set of REST commands by introspecting a set of callback classes. The given
     * callback classes must use the {@link Description} annotation to provide command
     * metadata, which is used to create a RegisteredCommand object, which is added to the
     * command registry. If the given storage service is null, the commands are considered
     * global. Otherwise, they belong to the given storage service and used in the context
     * of applications that it manages.
     * 
     * @param storageService    {@link StorageService} that owns the commands. Null if the
     *                          commands are global.
     * @param callbackClasses   Iterable {@link RESTCallback} classes that define the
     *                          commands to be added.
     */
    public void registerCallbacks(StorageService storageService,
                                  Iterable<Class<? extends RESTCallback>> callbackClasses) {
        if (m_bCmdSetIsFrozen) {
            throw new RuntimeException("New commands cannot be added: command set is frozen");
        }
        
        String cmdOwner = storageService == null ? SYSTEM_OWNER : storageService.getClass().getSimpleName();
        synchronized (m_cmdEvalMap) {
            for (Class<? extends RESTCallback> cmdClass : callbackClasses) {
                RegisteredCommand cmd = new RegisteredCommand(cmdClass);
                registerCommand(cmdOwner, cmd);
            }
        }
    }
    
    /**
     * Describe the current set of visible, registered commands in a {@link RESTCatalog}
     * object.
     *  
     * @return  A copy of visible, registered commands as a {@link RESTCatalog}.
     */
    public RESTCatalog describeCommands() {
        Map<String, SortedMap<String, RESTCommand>> ownerMap = new HashMap<>(m_cmdsByOwnerMap);
        for (String cmdOwner : ownerMap.keySet()) {
            SortedMap<String, RESTCommand> cmdMap = ownerMap.get(cmdOwner);
            Iterator<RESTCommand> iter = cmdMap.values().iterator();
            while (iter.hasNext()) {
                if (!iter.next().isVisible()) {
                    iter.remove();
                }
            }
        }
        return new RESTCatalog(ownerMap);
    }

    /**
     * Set the parent of the given command owner to the given parent owner. This enables
     * command owners to be arranged hierarchically so that one owner can inherit and
     * optionally override its parent's commands. {@link #findCommand()} searches the
     * hierarchy automatically.
     *   
     * @param cmdOwnerName          Name of a command owner.
     * @param parentCmdOwnerName    Name of parent command owner.
     */
    public void setParent(String cmdOwnerName, String parentCmdOwnerName) {
        m_parentMap.put(cmdOwnerName, parentCmdOwnerName);
    }
    
    /**
     * Freeze or unfreeze the command set. This should be called after the command set has
     * been frozen so that findMatch() does not need to acquire a lock, thus single-
     * threading command searching. If this is called with "true", subsequent attempts to
     * add a new command to the set will throw an exception.
     *  
     * @param bFreeze   True to freeze the command set. 
     */
    public void freezeCommandSet(boolean bFreeze) {
        m_bCmdSetIsFrozen = bFreeze;
    }   // freezeCommandSet

    /**
     * Attempt to find a command that matches the properties of a REST request. If a match
     * is found, the corresponding {@link RegisteredCommand} is returned and the given
     * variable map is populated with extracted parameters. For example, if the command's
     * URI template is:
     * <pre>
     *      /{application}/_query?{params}
     * </pre>
     * And the actual URI and query components provided are:
     * <pre>
     *      /MyApp/_query?q=Smith&s=100
     * </pre>
     * The variable map will be populated with to reflect the following variable settings:
     * <pre>
     *      application = MyApp
     *      params = q=Smith&s=100
     * </pre>
     * If any parameter values are URI-encoded, they remain encoded when returned in the
     * updated variable map.
     * 
     * @param owner         The command owner that defines the command set to search. If
     *                      null or empty, only system commands are searched. Otherwise,
     *                      the given owner's commands are searched followed by its parent
     *                      commands, if any.
     * @param method        The {@link HttpMethod} of the REST request. To match, a
     *                      command must be registerd with this as one of its methods.
     * @param uri           The URI provided in the REST request. The URI must remain
     *                      encoded so that path nodes can be properly recognized.
     * @param query         The query parameter of the REST request, if any. May be null
     *                      or empty if the request has no query component.
     * @param variableMap   Updated with variables extracted from URI and query parameters.
     *                      Not touched if the command does not match.
     * @return              The {@link RegisteredCommand} that matched the given request
     *                      or null if a match was not found.
     */
    public RegisteredCommand findCommand(String owner, HttpMethod method, String uri, String query,
                                         Map<String, String> variableMap) {
        String cmdOwner = Utils.isEmpty(owner) ? SYSTEM_OWNER : owner;
        RegisteredCommand cmd = null;
        while (cmd == null && !Utils.isEmpty(cmdOwner)) {
            cmd = searchCommands(cmdOwner, method, uri, query, variableMap);
            cmdOwner = m_parentMap.get(cmdOwner);
        }
        return cmd;
    }

    /**
     * Get all REST commands as a list of strings for debugging purposes.
     * 
     * @return  List of commands for debugging.
     */
    public Collection<String> getCommands() {
        List<String> commands = new ArrayList<String>();
        for (String cmdOwner : m_cmdsByOwnerMap.keySet()) {
            SortedMap<String, RESTCommand> ownerCmdMap = m_cmdsByOwnerMap.get(cmdOwner);
            for (String name : ownerCmdMap.keySet()) {
                RESTCommand cmd = ownerCmdMap.get(name);
                commands.add(String.format("%s: %s = %s", cmdOwner, name, cmd.toString()));
            }
        }
        return commands;
    }   // getCommands

    //----- Private methods
    
    // Register the given command and throw if is a duplicate name or command.
    private void registerCommand(String cmdOwner, RegisteredCommand cmd) {
        // Add to owner map in RESTCatalog.
        Map<String, RESTCommand> nameMap = getCmdNameMap(cmdOwner);
        String cmdName = cmd.getName();
        RESTCommand oldCmd = nameMap.put(cmdName, cmd);
        if (oldCmd != null) {
            throw new RuntimeException("Duplicate REST command with same owner/name: " +
                                       "owner=" + cmdOwner + ", name=" + cmdName +
                                       ", [1]=" + cmd + ", [2]=" + oldCmd); 
        }
        
        // Add to local evaluation map.
        Map<HttpMethod, SortedSet<RegisteredCommand>> evalMap = getCmdEvalMap(cmdOwner);
        for (HttpMethod method : cmd.getMethods()) {
            SortedSet<RegisteredCommand> methodSet = evalMap.get(method);
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
    
    // Get the method -> command map for the given owner.
    private Map<String, RESTCommand> getCmdNameMap(String cmdOwner) {
        SortedMap<String, RESTCommand> cmdNameMap = m_cmdsByOwnerMap.get(cmdOwner);
        if (cmdNameMap == null) {
            cmdNameMap = new TreeMap<>();
            m_cmdsByOwnerMap.put(cmdOwner, cmdNameMap);
        }
        return cmdNameMap;
    }

    // Get the method -> sorted command map for the given owner.
    private Map<HttpMethod, SortedSet<RegisteredCommand>> getCmdEvalMap(String cmdOwner) {
        Map<HttpMethod, SortedSet<RegisteredCommand>> evalMap = m_cmdEvalMap.get(cmdOwner);
        if (evalMap == null) {
            evalMap = new HashMap<>();
            m_cmdEvalMap.put(cmdOwner, evalMap);
        }
        return evalMap;
    }
    
    // Search the given command owner for a matching command.
    private RegisteredCommand searchCommands(String cmdOwner, HttpMethod method, String uri,
                                             String query, Map<String, String> variableMap) {
        Map<HttpMethod, SortedSet<RegisteredCommand>> evalMap = getCmdEvalMap(cmdOwner);
        if (evalMap == null) {
            return null;
        }
        
        // Find the sorted command set for the given HTTP method.
        SortedSet<RegisteredCommand> cmdSet = evalMap.get(method);
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
        for (RegisteredCommand cmd : cmdSet) {
            if (cmd.matches(pathNodeList, query, variableMap)) {
                return cmd;
            }
        }
        return null;
    }   // searchOwnerCommands
    
}   // class RESTCommandSet

