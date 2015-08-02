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

package com.dell.doradus.service.olap.mono;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.dell.doradus.common.AggregateResult;
import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.BatchResult;
import com.dell.doradus.common.CommonDefs;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;
import com.dell.doradus.olap.OlapBatch;
import com.dell.doradus.search.SearchResult;
import com.dell.doradus.search.SearchResultList;
import com.dell.doradus.service.StorageService;
import com.dell.doradus.service.olap.OLAPService;
import com.dell.doradus.service.rest.RESTCallback;
import com.dell.doradus.service.rest.RESTService;
import com.dell.doradus.service.taskmanager.Task;

/**
 * A specialized {@link StorageService} that provides a "single shard OLAP" service. This
 * class front-ends the {@link OLAPService}, supporting OLAP applications that only need a
 * single shard. Update and query requests are similar to native OLAP applications except
 * that they do not reference shards. Instead, all commands are automatically directed to
 * the solo shard. Hence, the normal "range" and "shards" parameters are neither required
 * nor allowed.
 * <p>
 * As data is added to an OLAP mono application, the solo shard is automatically merged in
 * a background task. By default, a check is made every minute for new data. Hence, when
 * data is added, it will start appearing in queries within a minute or two depending on 
 * shared merge time.
 * <p>
 * The OLAPMonoService is viable for applications that only require "a few million"
 * objects (possibly more or less depending on size) and hence do not need or want to
 * manage shards or shard names.
 */
public class OLAPMonoService extends StorageService {
    /**
     * Name of the single shard used by OLAPMono applications.
     */
    public static final String MONO_SHARD_NAME = "_mono";
    
    private static final OLAPMonoService INSTANCE = new OLAPMonoService();
    private OLAPMonoService() {};

    // OLAPMonoService-specific commands
    private static final List<Class<? extends RESTCallback>> CMD_CLASSES = Arrays.asList(
        QueryURICmd.class,
        QueryDocCmd.class,
        AggregateURICmd.class,
        AggregateDocCmd.class,
        UpdateBatchCmd.class,
        ShardStatsCmd.class,
        ShardStatisticsCmd.class,
        MergeShardCmd.class
    );
    
    //----- Service methods
    
    /**
     * Get the singleton instance of the StorageService. The object may or may not have
     * been initialized yet.
     * 
     * @return  The singleton instance of the OLAPService.
     */
    public static OLAPMonoService instance() {
        return INSTANCE;
    }   // instance

    @Override
    public void initService() {
        RESTService.instance().registerCommands(CMD_CLASSES, this, OLAPService.instance());
    }   // initService
    
    @Override
    public void startService() {
        Utils.require(OLAPService.instance().getState().isInitialized(),
                      "OLAPMonoService requires the OLAPService");
        OLAPService.instance().waitForFullService();
    }   // startService
    
    @Override
    public void stopService() {
    }   // stopService

    //----- StorageService: Schema update methods
    
    @Override
    public void deleteApplication(ApplicationDefinition appDef) {
        checkServiceState();
        OLAPService.instance().deleteApplication(appDef);
    }   // deleteApplication
    
    @Override
    public void initializeApplication(ApplicationDefinition oldAppDef,
                                      ApplicationDefinition appDef) {
        checkServiceState();
        OLAPService.instance().initializeApplication(oldAppDef, appDef);
    }   // initializeApplication
    
    @Override
    public void validateSchema(ApplicationDefinition appDef) {
        checkServiceState();
        validateApplication(appDef);
    }   // validateSchema
    
    @Override
    public Collection<Task> getAppTasks(ApplicationDefinition appDef) {
        // For now, always check to see if our shard needs merging.
        checkServiceState();
        List<Task> appTasks = new ArrayList<>();
        appTasks.add(new OLAPShardMerger(appDef));
        return appTasks;
    }   // getAppTasks
    
    //----- StorageService: Object query methods
    
    public SearchResultList objectQueryURI(TableDefinition tableDef, String uriQuery) {
        String monoURIQuery = addMonoShard(uriQuery);
        SearchResultList resultList = OLAPService.instance().objectQueryURI(tableDef, monoURIQuery);
        for (SearchResult result : resultList.results) {
            result.scalars.remove("_shard");
        }
        return resultList;
    }   // objectQueryURI
    
    public SearchResultList objectQueryDoc(TableDefinition tableDef, UNode rootNode) {
        UNode monoRootNode = addMonoShard(rootNode);
        SearchResultList resultList = OLAPService.instance().objectQueryDoc(tableDef, monoRootNode);
        for (SearchResult result : resultList.results) {
            result.scalars.remove("_shard");
        }
        return resultList;
    }   // objectQueryDoc
    
    public AggregateResult aggregateQueryURI(TableDefinition tableDef, String uriQuery) {
        String monoURIQuery = addMonoShard(uriQuery);
        return OLAPService.instance().aggregateQueryURI(tableDef, monoURIQuery);
    }
    
    public AggregateResult aggregateQueryDoc(TableDefinition tableDef, UNode rootNode) {
        UNode monoRootNode = addMonoShard(rootNode);
        return OLAPService.instance().aggregateQueryDoc(tableDef, monoRootNode);
    }
    
    //----- StorageService: Object update methods

    // Store name should alwauys be MONO_SHARD_NAME for OLAPMono.
    
    public BatchResult addBatch(ApplicationDefinition appDef, String shardName, OlapBatch batch) {
        Utils.require(shardName.equals(MONO_SHARD_NAME), "Shard name must be: " + MONO_SHARD_NAME);
        return OLAPService.instance().addBatch(appDef, shardName, batch);
    }   // addBatch
    
    public BatchResult addBatch(ApplicationDefinition appDef, String shardName,
                                OlapBatch batch, Map<String, String> options) {
        Utils.require(shardName.equals(MONO_SHARD_NAME), "Shard name must be: " + MONO_SHARD_NAME);
        return OLAPService.instance().addBatch(appDef, shardName, batch, options);
    }   // addBatch

    //----- OLAPMonoService additional public methods
    
    /**
     * Add the given batch of object updates for the given application. The updates may
     * be new, updated, or deleted objects. The updates are applied to the application's
     * mono shard.
     *  
     * @param appDef    Application to which update batch is applied.
     * @param batch     {@link OlapBatch} of object adds, updates, and/or deletes.
     * @return          {@link BatchResult} reflecting status of update.
     */
    public BatchResult addBatch(ApplicationDefinition appDef, OlapBatch batch) {
        return OLAPService.instance().addBatch(appDef, MONO_SHARD_NAME, batch);
    }   // addBatch
    
    /**
     * Add the given batch of object updates for the given application. The updates may
     * be new, updated, or deleted objects. The updates are applied to the application's
     * mono shard.
     *  
     * @param appDef    Application to which update batch is applied.
     * @param batch     {@link OlapBatch} of object adds, updates, and/or deletes.
     * @param options   Optional batcgh options such as overwrite=false.
     * @return          {@link BatchResult} reflecting status of update.
     */
    public BatchResult addBatch(ApplicationDefinition appDef, OlapBatch batch, Map<String, String> options) {
        return OLAPService.instance().addBatch(appDef, MONO_SHARD_NAME, batch, options);
    }   // addBatch
    
    public static String addMonoShard(String uriQuery) {
        Map<String, String> uriParams = Utils.parseURIQuery(uriQuery);
        Utils.require(!uriParams.containsKey("range"), "'range' parameter not allowed");
        Utils.require(!uriParams.containsKey("shards"), "'shards' parameter not allowed");
        uriParams.put("shards", MONO_SHARD_NAME);
        return Utils.joinURIQuery(uriParams);
    }   // addMonoShard

    public static UNode addMonoShard(UNode rootNode) {
        Utils.require(rootNode.getMember("shards") == null, "'shards' parameter not allowed");
        Utils.require(rootNode.getMember("shards-range") == null, "'shards-range' parameter not allowed");
        Utils.require(rootNode.getMember("x-shards") == null, "'x-shards' parameter not allowed");
        Utils.require(rootNode.getMember("x-shards-range") == null, "'x-shards-range' parameter not allowed");
        rootNode.addValueNode("shards", MONO_SHARD_NAME);
        return rootNode;
    }

    //----- Private methods
    
    // Validate the given application for OLAPMono semantics.
    private void validateApplication(ApplicationDefinition appDef) {
        for (String optName : appDef.getOptionNames()) {
            String optValue = appDef.getOption(optName);
            switch (optName) {
            case CommonDefs.OPT_STORAGE_SERVICE:
                assert optValue.equals(this.getClass().getSimpleName());
                break;
                
            case CommonDefs.OPT_TENANT:
                // Ignore
                break;
                
            default:
                throw new IllegalArgumentException("Unknown option for OLAPMonoService application: " + optName);
            }
        }

        for (TableDefinition tableDef : appDef.getTableDefinitions().values()) {
            validateTable(tableDef);
        }
    }
    
    // Validate the given table for OLAPMono constraints.
    private void validateTable(TableDefinition tableDef) {
        // No options are currently allowed:
        for (String optName : tableDef.getOptionNames()) {
            Utils.require(false, "Unknown option for OLAPMonoService table: " + optName);
        }
        
        for (FieldDefinition fieldDef : tableDef.getFieldDefinitions()) {
            validateField(fieldDef);
        }
    }   // validateTable

    // Validate the given field for OLAP constraints.
    private void validateField(FieldDefinition fieldDef) {
        if (fieldDef.isScalarField()) {
            Utils.require(Utils.isEmpty(fieldDef.getAnalyzerName()),
                          "'analyzer' not allowed for OLAPMono application fields:" + fieldDef);
        } else if (fieldDef.isXLinkField()) {
            Utils.require(false, "XLinks are not allowed for OLAPMono applications: " + fieldDef);
        } else if (fieldDef.isLinkField()) {
            Utils.require(!fieldDef.isSharded(),
                          "Link fields cannot be sharded: {}", fieldDef);
        }
    }   // validateField
    
}   // class OLAPMonoService
