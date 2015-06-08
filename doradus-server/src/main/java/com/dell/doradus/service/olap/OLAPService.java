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

package com.dell.doradus.service.olap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.dell.doradus.common.AggregateResult;
import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.BatchResult;
import com.dell.doradus.common.BatchResult.Status;
import com.dell.doradus.common.CommonDefs;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;
import com.dell.doradus.olap.MergeOptions;
import com.dell.doradus.olap.Olap;
import com.dell.doradus.olap.OlapAggregate;
import com.dell.doradus.olap.OlapBatch;
import com.dell.doradus.olap.OlapQuery;
import com.dell.doradus.olap.OlapStatistics;
import com.dell.doradus.olap.Olapp;
import com.dell.doradus.olap.aggregate.AggregateResultConverter;
import com.dell.doradus.olap.aggregate.AggregationResult;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.olap.store.SegmentStats;
import com.dell.doradus.search.SearchResultList;
import com.dell.doradus.service.StorageService;
import com.dell.doradus.service.db.Tenant;
import com.dell.doradus.service.rest.RESTCommand;
import com.dell.doradus.service.rest.RESTService;
import com.dell.doradus.service.schema.SchemaService;
import com.dell.doradus.service.taskmanager.Task;
import com.dell.doradus.service.taskmanager.TaskFrequency;

/**
 * The OLAP storage service for Doradus.
 */
public class OLAPService extends StorageService {
    private static final OLAPService INSTANCE = new OLAPService();
    
    private Olap m_olap = null;

    // OLAPService-specific commands
    private static final List<RESTCommand> REST_RULES = Arrays.asList(new RESTCommand[]{
        // Object retrieval:
        new RESTCommand("GET    /{application}/{table}/_query?{params}      com.dell.doradus.service.olap.QueryURICmd"),
        new RESTCommand("GET    /{application}/{table}/_query               com.dell.doradus.service.olap.QueryDocCmd"),
        new RESTCommand("PUT    /{application}/{table}/_query               com.dell.doradus.service.olap.QueryDocCmd"),
        new RESTCommand("GET    /{application}/{table}/_aggregate?{params}  com.dell.doradus.service.olap.AggregateURICmd"),
        new RESTCommand("GET    /{application}/{table}/_aggregate           com.dell.doradus.service.olap.AggregateDocCmd"),
        new RESTCommand("PUT    /{application}/{table}/_aggregate           com.dell.doradus.service.olap.AggregateDocCmd"),
        
        // Object updates:
        new RESTCommand("POST   /{application}/{shard}                      com.dell.doradus.service.olap.AddObjectsCmd"),
        new RESTCommand("POST   /{application}/{shard}?{params}             com.dell.doradus.service.olap.AddObjectsCmd"),
        new RESTCommand("PUT    /{application}/{shard}                      com.dell.doradus.service.olap.AddObjectsCmd"),
        new RESTCommand("PUT    /{application}/{shard}?{params}             com.dell.doradus.service.olap.AddObjectsCmd"),
        
        // Shard management:
        new RESTCommand("POST   /{application}/_shards/{shard}              com.dell.doradus.service.olap.MergeSegmentCmd"),
        new RESTCommand("POST   /{application}/_shards/{shard}?{params}     com.dell.doradus.service.olap.MergeSegmentCmd"),
        new RESTCommand("DELETE /{application}/_shards/{shard}              com.dell.doradus.service.olap.DeleteSegmentCmd"),
        new RESTCommand("GET    /{application}/_shards                      com.dell.doradus.service.olap.ListShardsCmd"),
        new RESTCommand("GET    /{application}/_shards/{shard}              com.dell.doradus.service.olap.ShardStatsCmd"),
        // Troubleshooting & repair
        new RESTCommand("GET    /{application}/_statistics/{shard}?{params} com.dell.doradus.service.olap.ShardStatisticsCmd"),
        new RESTCommand("GET    /{application}/{table}/_duplicates?{params} com.dell.doradus.service.olap.DuplicatesCmd"),
        new RESTCommand("GET    /{application}/_verify/{shard}              com.dell.doradus.service.olap.ShardVerifyCmd"),
        new RESTCommand("DELETE /{application}/_shards/{shard}/{segment}    com.dell.doradus.service.olap.DeleteSegmentCmd2"),
 
               
        new RESTCommand("GET    /_olapp?{params}                             com.dell.doradus.service.olap.OlappCmd"),
    });
    
    //----- Service methods
    
    /**
     * Get the singleton instance of the StorageService. The object may or may not have
     * been initialized yet.
     * 
     * @return  The singleton instance of the OLAPService.
     */
    public static OLAPService instance() {
        return INSTANCE;
    }   // instance

    @Override
    public void initService() {
        RESTService.instance().registerApplicationCommands(REST_RULES, this);
    }   // initService
    
    @Override
    public void startService() {
        SchemaService.instance().waitForFullService();
        m_olap = new Olap();
    }   // startService
    
    @Override
    public void stopService() {
        m_olap = null;
    }   // stopService

    //----- StorageService: Schema update methods
    
    @Override
    public void deleteApplication(ApplicationDefinition appDef) {
        checkServiceState();
        m_olap.deleteApplication(appDef);
    }   // deleteApplication
    
    @Override
    public void initializeApplication(ApplicationDefinition oldAppDef,
                                      ApplicationDefinition appDef) {
        checkServiceState();
        m_olap.createApplication(Tenant.getTenant(appDef), appDef.getAppName());
    }   // initializeApplication
    
    @Override
    public void validateSchema(ApplicationDefinition appDef) {
        checkServiceState();
        validateApplication(appDef);
    }   // validateSchema
    
    @Override
    public Collection<Task> getAppTasks(ApplicationDefinition appDef) {
        checkServiceState();
        List<Task> appTasks = new ArrayList<>();
        String agingFreq = appDef.getOption(CommonDefs.OPT_AGING_CHECK_FREQ);
        if (agingFreq != null) {
            Task task = new Task(appDef.getAppName(), null, "data-aging", agingFreq, OLAPDataAger.class);
            appTasks.add(task);
        }
        return appTasks;
    }   // getAppTasks
    
    //----- Object query methods
    
    /**
     * Perform an object query on the given table using query parameters encoded as a URI
     * query parameter. Example:
     * <pre>
     *      q=Size%3E3+AND+Name:smith&shards=Fox,Charlie
     * </pre>
     * 
     * @param tableDef  {@link TableDefinition} of table to query.
     * @param uriQuery  URI-encoded query parameters.
     * @return          {@link SearchResultList} containing search results.
     */
    public SearchResultList objectQueryURI(TableDefinition tableDef, String uriQuery) {
        checkServiceState();
        OlapQuery olapQuery = new OlapQuery(uriQuery);
        return m_olap.search(tableDef.getAppDef(), tableDef.getTableName(), olapQuery);
    }   // objectQueryURI
    
    /**
     * Perform an object query on the given table using query parameters parsed into a
     * UNode tree.
     * 
     * @param tableDef  {@link TableDefinition} of table to query.
     * @param rootNode  Root {@link UNode} of an object query parameter document.
     * @return          {@link SearchResultList} containing search results.
     */
    public SearchResultList objectQueryDoc(TableDefinition tableDef, UNode rootNode) {
        checkServiceState();
        OlapQuery olapQuery = new OlapQuery(rootNode);
        return m_olap.search(tableDef.getAppDef(), tableDef.getTableName(), olapQuery);
    }   // objectQueryDoc
    
    /**
     * Perform an aggregate query on the given table using query parameters encoded as a
     * URI query parameter. Example:
     * <pre>
     *      q=Size%3E3+AND+Name:smith&shards=Fox,Charlie&m=COUNT(*)
     * </pre>
     * 
     * @param tableDef  {@link TableDefinition} of table to query.
     * @param uriQuery  URI-encoded query parameters.
     * @return          {@link AggregateResult} containing search results.
     */
    public AggregateResult aggregateQueryURI(TableDefinition tableDef, String uriQuery) {
        checkServiceState();
        OlapAggregate request = new OlapAggregate(uriQuery);
        AggregationResult result = m_olap.aggregate(tableDef.getAppDef(), tableDef.getTableName(), request);
        return AggregateResultConverter.create(result, request);
    }
    
    /**
     * Perform an aggregate query on the given table using query parameters parsed into a
     * UNode tree.
     * 
     * @param tableDef  {@link TableDefinition} of table to query.
     * @param rootNode  Root {@link UNode} of an aggregate query parameter document.
     * @return          {@link AggregateResult} containing search results.
     */
    public AggregateResult aggregateQueryDoc(TableDefinition tableDef, UNode rootNode) {
        checkServiceState();
        OlapAggregate request = new OlapAggregate(rootNode);
        AggregationResult result = m_olap.aggregate(tableDef.getAppDef(), tableDef.getTableName(), request);
        return AggregateResultConverter.create(result, request);
    }
    
    //----- StorageService: Object update methods

    /**
     * Add a batch of updates for the given application to the given shard. Objects can
     * new, updated, or deleted.
     * 
     * @param appDef    {@link ApplicationDefinition} of application to update.
     * @param shardName Shard to add batch to.
     * @param batch     {@link OlapBatch} containing object updates.
     * @return          {@link BatchResult} indicating results of update.
     */
    public BatchResult addBatch(ApplicationDefinition appDef, String shardName, OlapBatch batch) {
        return addBatch(appDef, shardName, batch, null);
    }   // addBatch
    
    /**
     * Same as {@link #addBatch(ApplicationDefinition, String, OlapBatch)} but allows
     * batch options to be passed. Currently, the only option supported is "overwrite",
     * which must be true or false. The default is true, which means field values for
     * existing objects are replaced. When overwrite is set to false, existing field
     * values are not overwritten with the values in this batch.
     * 
     * @param appDef    {@link ApplicationDefinition} of application to update.
     * @param shardName Shard to add batch to.
     * @param batch     {@link OlapBatch} containing object updates.
     * @param options   Map of option key/value pairs. Currently, only "overwrite" is
     *                  supported and must be true/false. 
     * @return          {@link BatchResult} indicating results of update.
     */
    public BatchResult addBatch(ApplicationDefinition appDef, String shardName,
                                OlapBatch batch, Map<String, String> options) {
        checkServiceState();
        String guid = m_olap.addSegment(appDef, shardName, batch, getOverwriteOption(options));
        BatchResult result = new BatchResult();
        result.setStatus(Status.OK);
        result.setComment("GUID=" + guid);
        return result;
    }   // addBatch

    //----- OLAPService-specific public methods

    /**
     * Perform the given OLAP browser (_olapp) command.
     * 
     * @param tenant        Tenant context for command.
     * @param parameters    OLAP browser parameters. Should be empty to start the browser
     *                      at the home page.
     * @return              An HTML-formatted page containing the results of the given
     *                      OLAP browser command.
     */
    public String browseOlapp(Tenant tenant, Map<String, String> parameters) {
        checkServiceState();
        return Olapp.process(tenant, m_olap, parameters);
    }   // browseOlapp
    
    /**
     * Delete the shard for the given application, including all of its data. This method
     * is a no-op if the given shard does not exist or has no data.
     * 
     * @param appDef        {@link ApplicationDefinition} of application.
     * @param shard         Shard name.
     */
    public void deleteShard(ApplicationDefinition appDef, String shard) {
        checkServiceState();
        m_olap.deleteShard(appDef, shard);
    }   // deleteShard

    /**
     * Get a list of {@link ApplicationDefinition}s that are assigned to the OLAP service
     * for the given tenant. An empty list is returned if there are no OLAP applications.
     * 
     * @param   tenant  {@link Tenant} that owns the OLAP applications.
     * @return  A list of {@link ApplicationDefinition}s that are assigned to the OLAP
     *          service.
     */
    public List<ApplicationDefinition> getAllOLAPApplications(Tenant tenant) {
        List<ApplicationDefinition> appDefs = new ArrayList<>();
        for (ApplicationDefinition appDef : SchemaService.instance().getAllApplications(tenant)) {
            if (OLAPService.class.getSimpleName().equals(appDef.getStorageService())) {
                appDefs.add(appDef);
            }
        }
        return appDefs;
    }   // getAllOLAPApplications
    
    /**
     * Get the internal {@link Olap} object; for internal/direct access only. Waits for the
     * database to be opened and OLAP to be fully initialized.
     * 
     * @return The internal {@link Olap} object.
     */
    public Olap getOlap() {
        checkServiceState();
        return m_olap;
    }   // getOlap
    
    /**
     * Get statistics for the given shard, owned by the given application. Statistics are
     * returned as a {@link SegmentStats} object. If there is no information for the given
     * shard, an IllegalArgumentException is thrown.
     *  
     * @param appDef        OLAP application definition.
     * @param shard         Shard name.
     * @return              Shard statistics as a {@link SegmentStats} object.
     */
    public SegmentStats getStats(ApplicationDefinition appDef, String shard) {
        checkServiceState();
        return m_olap.getStats(appDef, shard);
    }   // getStats 
    
    /**
     * Get detailed shard statistics for the given shard. This command is mostly used for
     * development and diagnostics.
     *   
     * @param appDef    {@link ApplicationDefinition} of application to query.
     * @param shard     Name of shard to query.
     * @param paramMap  Map of statistic option key/value pairs.
     * @return          Root of statistics information as a {@link UNode} tree. 
     */
    public UNode getStatistics(ApplicationDefinition appDef, String shard, Map<String, String> paramMap) {
        checkServiceState();
        CubeSearcher searcher = m_olap.getSearcher(appDef, shard);
        String file = paramMap.get("file");
        if(file != null) {
            return OlapStatistics.getFileData(searcher, file);
        }
        String sort = paramMap.get("sort");
        boolean memStats = !"false".equals(paramMap.get("mem"));
        return OlapStatistics.getStatistics(searcher, sort, memStats);
    }   // getStats
    
    /**
     * List the names of all shards for the given OLAP application.
     * 
     * @param appDef        OLAP application definition.
     * @return              List of shard names. Empty if there are no shards.
     */
    public List<String> listShards(ApplicationDefinition appDef) {
        checkServiceState();
        return m_olap.listShards(appDef);
    }   // listShards
    
    /**
     * Merge loaded batches for the given shard name and application, optionally assigning
     * an expire-date. An exception is thrown if a merge is already underway for the
     * shard. If the given expireDate is not null, the shard is updated with the given
     * expire-date. If the given expireDate is null, the shard will be updated to not have
     * an expire-date.
     * 
     * @param appDef        OLAP application definition.
     * @param shard         Shard name.
     * @param options    	Merge options
     */
    public void mergeShard(ApplicationDefinition appDef, String shard, MergeOptions options) {
        checkServiceState();
        m_olap.merge(appDef, shard, options);
    }
    
    /**
     * Get the expire-date for the given shard name and OLAP application. Null is returned
     * if the shard does not exist or has no expire-date.
     * 
     * @param appDef        OLAP application definition.
     * @param shard         Shard name.
     * @return              Shard's expire-date or null if it doesn't exist or has no
     *                      expire-date.
     */
    public Date getExpirationDate(ApplicationDefinition appDef, String shard) {
        checkServiceState();
        return m_olap.getExpirationDate(appDef, shard);
    }	// getExpirationDate
    
    /**
     * Search for and return duplicate object IDs for the given OLAP application and table
     * across the given shard range. The given shard range must be in the form of:
     * <pre>
     *      <i>from-shard</i>[,<i>to-shard</i>]
     * </pre>
     * All shards whose name is greater than or equal to <i>from-shard</i> are searched.
     * If <i>to-shard</i> is given, shard searched will also be only those whose name is
     * less than or equal to it.
     * 
     * @param application   OLAP application name.
     * @param table         Table name belonging to application.
     * @param range         From shard name, optionally followed by to shard name.
     * @return              List of object IDs in more than one shard within the given range,
     *                      returned as a {@link SearchResultList}.
     */
    public SearchResultList getDuplicateIDs(ApplicationDefinition appDef, String table, String range) {
        checkServiceState();
    	return m_olap.getDuplicateIDs(appDef, table, range);
    }	// getDuplicateIDs
    
    //----- Private methods
    
    // Singleton instantiation only.
    private OLAPService() { }

    // Get case-insensitive "overwrite" option. Default to "true".
    private boolean getOverwriteOption(Map<String, String> options) {
        boolean bOverwrite = true;
        if (options != null) {
            for (String name : options.keySet()) {
                if ("overwrite".equals(name.toLowerCase())) {
                    bOverwrite = Boolean.parseBoolean(options.get(name));
                } else {
                    Utils.require(false, "Unknown OLAP batch option: " + name);
                }
            }
        }
        return bOverwrite;
    }   // getOverwriteOption

    // Validate the given application for OLAP constraints.
    private void validateApplication(ApplicationDefinition appDef) {
        boolean bSawAgingFreq = false;
        for (String optName : appDef.getOptionNames()) {
            String optValue = appDef.getOption(optName);
            switch (optName) {
            case CommonDefs.OPT_STORAGE_SERVICE:
                assert optValue.equals(this.getClass().getSimpleName());
                break;
                
            case CommonDefs.OPT_TENANT:
                // Ignore
                break;
                
            case CommonDefs.OPT_AGING_CHECK_FREQ:
                new TaskFrequency(optValue);
                bSawAgingFreq = true;
                break;
                
            default:
                throw new IllegalArgumentException("Unknown option for OLAPService application: " + optName);
            }
        }

        for (TableDefinition tableDef : appDef.getTableDefinitions().values()) {
            validateTable(tableDef);
        }
        
        if (!bSawAgingFreq) {
            appDef.setOption(CommonDefs.OPT_AGING_CHECK_FREQ, "1 DAY");
        }
    }   // validateApplication

    // Validate the given table for OLAP constraints.
    private void validateTable(TableDefinition tableDef) {
        // No options are currently allowed:
        for (String optName : tableDef.getOptionNames()) {
            Utils.require(false, "Unknown option for OLAPService table: " + optName);
        }
        
        for (FieldDefinition fieldDef : tableDef.getFieldDefinitions()) {
            validateField(fieldDef);
        }
    }   // validateTable

    // Validate the given field for OLAP constraints.
    private void validateField(FieldDefinition fieldDef) {
        if (fieldDef.isScalarField()) {
            Utils.require(Utils.isEmpty(fieldDef.getAnalyzerName()),
                          "'analyzer' not allowed for OLAP application fields:" + fieldDef);
        } else if (fieldDef.isLinkType()) {
            Utils.require(!fieldDef.isSharded(),
                          "Link and xlink fields cannot be sharded: {}", fieldDef);
        }
    }   // validateField
    
}   // class OLAPService
