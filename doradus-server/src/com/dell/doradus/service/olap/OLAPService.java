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

import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.dell.doradus.common.AggregateResult;
import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.BatchResult;
import com.dell.doradus.common.CommonDefs;
import com.dell.doradus.common.DBObject;
import com.dell.doradus.common.DBObjectBatch;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.ScheduleDefinition;
import com.dell.doradus.common.StatisticDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;
import com.dell.doradus.common.BatchResult.Status;
import com.dell.doradus.common.ScheduleDefinition.SchedType;
import com.dell.doradus.olap.Olap;
import com.dell.doradus.olap.OlapAggregate;
import com.dell.doradus.olap.OlapQuery;
import com.dell.doradus.olap.OlapStatistics;
import com.dell.doradus.olap.Olapp;
import com.dell.doradus.olap.aggregate.AggregateResultConverter;
import com.dell.doradus.olap.aggregate.AggregationResult;
import com.dell.doradus.olap.store.SegmentStats;
import com.dell.doradus.search.SearchResultList;
import com.dell.doradus.service.StorageService;
import com.dell.doradus.service.rest.RESTCommand;
import com.dell.doradus.service.rest.RESTService;
import com.dell.doradus.service.schema.SchemaService;

/**
 * The OLAP storage service for Doradus.
 */
public class OLAPService extends StorageService {
    private static final OLAPService INSTANCE = new OLAPService();
    
    private Olap m_olap = null;

    // OLAPService-specific commands
    private static final List<RESTCommand> REST_RULES = Arrays.asList(new RESTCommand[]{
        new RESTCommand("POST   /{application}/_shards/{shard}              com.dell.doradus.service.olap.MergeSegmentCmd"),
        new RESTCommand("POST   /{application}/_shards/{shard}?{params}     com.dell.doradus.service.olap.MergeSegmentCmd"),
        new RESTCommand("DELETE /{application}/_shards/{shard}              com.dell.doradus.service.olap.DeleteSegmentCmd"),
        new RESTCommand("GET    /{application}/_shards                      com.dell.doradus.service.olap.ListShardsCmd"),
        new RESTCommand("GET    /{application}/_shards/{shard}              com.dell.doradus.service.olap.ShardStatsCmd"),
        new RESTCommand("GET    /{application}/_statistics/{shard}?{params} com.dell.doradus.service.olap.ShardStatisticsCmd"),
        new RESTCommand("GET    /{application}/{table}/_duplicates?{params} com.dell.doradus.service.olap.DuplicatesCmd"),
               
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
        RESTService.instance().registerRESTCommands(REST_RULES);
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
        m_olap.deleteApplication(appDef.getAppName());
    }   // deleteApplication
    
    @Override
    public void initializeApplication(ApplicationDefinition oldAppDef, ApplicationDefinition appDef) {
        // Nothing extra to do
    }   // initializeApplication
    
    @Override
    public void validateSchema(ApplicationDefinition appDef) {
        validateApplication(appDef);
        validateSchedules(appDef);
    }   // validateSchema
    
    //----- StorageService: Object query methods
    
    @Override
    public DBObject getObject(TableDefinition tableDef, String objID) {
        throw new IllegalArgumentException("OLAP applications do not support this command");
    }
    
    @Override
    public SearchResultList objectQueryURI(TableDefinition tableDef, String uriQuery) {
        checkServiceState();
        OlapQuery olapQuery = new OlapQuery(uriQuery);
        return m_olap.search(tableDef.getAppDef().getAppName(), tableDef.getTableName(), olapQuery);
    }   // objectQueryURI
    
    @Override
    public SearchResultList objectQueryDoc(TableDefinition tableDef, UNode rootNode) {
        checkServiceState();
        OlapQuery olapQuery = new OlapQuery(rootNode);
        return m_olap.search(tableDef.getAppDef().getAppName(), tableDef.getTableName(), olapQuery);
    }   // objectQueryDoc
    
    @Override
    public AggregateResult aggregateQueryURI(TableDefinition tableDef, String uriQuery) {
        checkServiceState();
        String application = tableDef.getAppDef().getAppName();
        OlapAggregate request = new OlapAggregate(uriQuery);
        AggregationResult result = m_olap.aggregate(application, tableDef.getTableName(), request);
        return AggregateResultConverter.create(result, request);
    }
    
    @Override
    public AggregateResult aggregateQueryDoc(TableDefinition tableDef, UNode rootNode) {
        checkServiceState();
        String application = tableDef.getAppDef().getAppName();
        OlapAggregate request = new OlapAggregate(rootNode);
        AggregationResult result = m_olap.aggregate(application, tableDef.getTableName(), request);
        return AggregateResultConverter.create(result, request);
    }
    
    //----- StorageService: Object update methods

    // Store name is a shard name for OLAP.
    
    @Override
    public BatchResult addBatch(ApplicationDefinition appDef, String shardName, DBObjectBatch batch) {
        return addBatch(appDef, shardName, batch, null);
    }   // addBatch
    
    @Override
    public BatchResult addBatch(ApplicationDefinition appDef, String shardName,
                                DBObjectBatch batch, Map<String, String> options) {
        checkServiceState();
        String guid = m_olap.addSegment(appDef, shardName, batch, getOverwriteOption(options));
        BatchResult result = new BatchResult();
        result.setStatus(Status.OK);
        result.setComment("GUID=" + guid);
        return result;
    }   // addBatch

    @Override
    public BatchResult updateBatch(ApplicationDefinition appDef, String shardName, DBObjectBatch batch) {
        return addBatch(appDef, shardName, batch, null);
    }   // updateBatch
    
    @Override
    public BatchResult updateBatch(ApplicationDefinition appDef, String shardName,
                                   DBObjectBatch batch, Map<String, String> options) {
        return addBatch(appDef, shardName, batch, options);
    }   // updateBatch

    @Override
    public BatchResult deleteBatch(ApplicationDefinition appDef, String storeName, DBObjectBatch batch) {
        // Just add "_deleted" flag to all objects.
        for (DBObject dbObj : batch.getObjects()) {
            dbObj.setDeleted(true);
        }
        return addBatch(appDef, storeName, batch, null);
    }   // deleteBatch

    //----- OLAPService-specific public methods

    /**
     * Perform the given OLAP browser (_olapp) command.
     * 
     * @param parameters    OLAP browser parameters. Should be empty to start the browser
     *                      at the home page.
     * @return              An HTML-formatted page containing the results of the given
     *                      OLAP browser command.
     */
    public String browseOlapp(Map<String, String> parameters) {
        checkServiceState();
        return Olapp.process(m_olap, parameters);
    }   // browseOlapp
    
    /**
     * Delete the shard for the given application, including all of its data. This method
     * is a no-op if the given shard does not exist or has no data.
     * 
     * @param application   OLAP application name.
     * @param shard         Shard name.
     */
    public void deleteShard(String application, String shard) {
        checkServiceState();
        m_olap.deleteShard(application, shard);
    }   // deleteShard

    /**
     * Get a list of {@link ApplicationDefinition}s that are assigned to the OLAP service.
     * An empty list is returned if there are no OLAP applications.
     * 
     * @return  A list of {@link ApplicationDefinition}s that are assigned to the OLAP
     *          service.
     */
    public List<ApplicationDefinition> getAllOLAPApplications() {
        List<ApplicationDefinition> appDefs = SchemaService.instance().getAllApplications();
        Iterator<ApplicationDefinition> iter = appDefs.iterator();
        while (iter.hasNext()) {
            ApplicationDefinition appDef = iter.next();
            if (!OLAPService.class.getSimpleName().equals(appDef.getStorageService())) {
                iter.remove();
            }
        }
        return appDefs;
    }   // getAllOLAPApplications
    
    /**
     * Get the {@link ApplicationDefinition} for the given OLAP application. An exception
     * is thrown if the application does not exist or is not an OLAP application.
     * 
     * @param applicationName   Name of an OLAP application.
     * @return                  {@link ApplicationDefinition} for the given OLAP application
     */
    public ApplicationDefinition getOLAPApplication(String applicationName) {
        ApplicationDefinition appDef = SchemaService.instance().getApplication(applicationName);
        Utils.require(appDef != null, "Application '%s' does not exist", applicationName);
        Utils.require(OLAPService.class.getSimpleName().equals(appDef.getStorageService()),
                      "Application '%s' is not an OLAP application", applicationName);
        return appDef;
    }   // getOLAPApplication

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
     * @param application   OLAP application name.
     * @param shard         Shard name.
     * @return              Shard statistics as a {@link SegmentStats} object.
     */
    public SegmentStats getStats(String application, String shard) {
        checkServiceState();
        return m_olap.getStats(application, shard);
    }   // getStats 

    public UNode getStatistics(String application, String shard) {
        checkServiceState();
        return OlapStatistics.getStatistics(m_olap.getSearcher(application, shard));
    }   // getStats
    
    public UNode getStatisticsFileData(String application, String shard, String file) {
        checkServiceState();
        return OlapStatistics.getFileData(m_olap.getSearcher(application, shard), file);
    }   // getStats 
    
    /**
     * List the names of all shards for the given OLAP application.
     * 
     * @param application   OLAP application name.
     * @return              List of shard names. Empty if there are no shards.
     */
    public List<String> listShards(String application) {
        checkServiceState();
        return m_olap.listShards(application);
    }   // listShards
    
    /**
     * Merge loaded batches for the given shard name and application, optionally assigning
     * an expire-date. An exception is thrown if a merge is already underway for the
     * shard. If the given expireDate is not null, the shard is updated with the given
     * expire-date. If the given expireDate is null, the shard will be updated to not have
     * an expire-date.
     * 
     * @param application   OLAP application name.
     * @param shard         Shard name.
     * @param expireDate    Optional expire-date for shard. Null to remove expire-date.
     */
    public void mergeShard(String application, String shard, Date expireDate) {
        checkServiceState();
        m_olap.merge(application, shard, expireDate);
    }   // mergeShard
    
    /**
     * Get the expire-date for the given shard name and OLAP application. Null is returned
     * if the shard does not exist or has no expire-date.
     * 
     * @param application   OLAP application name.
     * @param shard         Shard name.
     * @return              Shard's expire-date or null if it doesn't exist or has no
     *                      expire-date.
     */
    public Date getExpirationDate(String application, String shard) {
        checkServiceState();
        return m_olap.getExpirationDate(application, shard);
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
    public SearchResultList getDuplicateIDs(String application, String table, String range) {
        checkServiceState();
    	return m_olap.getDuplicateIDs(application, table, range);
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
        for (String optName : appDef.getOptionNames()) {
            String optValue = appDef.getOption(optName);
            switch (optName) {
            case CommonDefs.OPT_STORAGE_SERVICE:
                assert optValue.equals(this.getClass().getSimpleName());
                break;
                
            default:
                throw new IllegalArgumentException("Unknown option for OLAPService application: " + optName);
            }
        }

        for (TableDefinition tableDef : appDef.getTableDefinitions().values()) {
            validateTable(tableDef);
        }
    }   // validateApplication

    // Validate the given table for OLAP constraints.
    private void validateTable(TableDefinition tableDef) {
        // No options are currently allowed:
        for (String optName : tableDef.getOptionNames()) {
            Utils.require(false, "Unknown option for OLAPService table: " + optName);
        }
        
        // Statistics are not supported.
        for (StatisticDefinition statDef : tableDef.getStatDefinitions()) {
            Utils.require(false, "OLAPService applications do not support statistic definitions: " + statDef.getStatName());
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
//            Utils.require(!fieldDef.isXLinkField() || !fieldDef.getName().equals(fieldDef.getLinkInverse()),
//                          "xlinks cannot be their own inverse (self-reflexive): " + fieldDef);
        }
    }   // validateField
    
    // Validate OLAP background tasks schedules. At least one "data-aging" task
    // should exist, no table must be defined for that task
    private void validateSchedules(ApplicationDefinition appDef) {
    	for (ScheduleDefinition schedDef : appDef.getSchedules().values()) {
    		schedDef.validate(getClass().getSimpleName());
    	}

    	boolean hasDataAgingTask = false;
		for (ScheduleDefinition schedDef : appDef.getSchedules().values()) {
			SchedType taskType = schedDef.getType();
			if (taskType == SchedType.DATA_AGING) {
				hasDataAgingTask = true;
				break;
			}
		}
		if (!hasDataAgingTask) {
			appDef.addSchedule(SchedType.DATA_AGING, 
					ScheduleDefinition.DEFAULT_AGING_SCHEDULE, null, null);
		}
    }

}   // class OLAPService
