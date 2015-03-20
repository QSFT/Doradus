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

package com.dell.doradus.service.spider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.utils.Pair;

import com.dell.doradus.common.AggregateResult;
import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.BatchResult;
import com.dell.doradus.common.CommonDefs;
import com.dell.doradus.common.DBObject;
import com.dell.doradus.common.DBObjectBatch;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.FieldType;
import com.dell.doradus.common.RetentionAge;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.TableDefinition.ShardingGranularity;
import com.dell.doradus.common.UNode;
import com.dell.doradus.common.Utils;
import com.dell.doradus.fieldanalyzer.FieldAnalyzer;
import com.dell.doradus.search.SearchResultList;
import com.dell.doradus.search.aggregate.Aggregate;
import com.dell.doradus.service.StorageService;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.DRow;
import com.dell.doradus.service.db.Tenant;
import com.dell.doradus.service.schema.SchemaService;
import com.dell.doradus.service.taskmanager.Task;
import com.dell.doradus.service.taskmanager.TaskFrequency;

/**
 * The main class for the SpiderService storage service. The Spider service stores objects
 * in a tabular database (currently Cassandra), providing fully inverted indexing, data
 * aging, and other features.
 */
public class SpiderService extends StorageService {
    // Maximum length of a ColumnFamily name:
    private static final int MAX_CF_NAME_LENGTH = 48;
    
    // Singleton object:
    private static final SpiderService INSTANCE = new SpiderService();
    private final ShardCache m_shardCache = new ShardCache();

    /**
     * Get the singleton instance of this service. The service may or may not have been
     * initialized yet.
     * 
     * @return  The singleton instance of this service.
     */
    public static SpiderService instance() {
        return INSTANCE;
    }   // instance
    
    //----- Service methods
    
    @Override
    public void initService() {
    }   // initService

    @Override
    public void startService() {
        SchemaService.instance().waitForFullService();
    }   // startService

    @Override
    public void stopService() {
        m_shardCache.clearAll();
    }   // stopService

    //----- StorageService schema update methods
    
    // Delete all CFs used by the given application.
    @Override
    public void deleteApplication(ApplicationDefinition appDef) {
        checkServiceState();
        deleteApplicationCFs(appDef);
        m_shardCache.clear(appDef);
    }   // deleteApplication
    
    // Create all CFs needed for the given application.
    @Override
    public void initializeApplication(ApplicationDefinition oldAppDef,
                                      ApplicationDefinition appDef) {
        checkServiceState();
        Tenant tenant = Tenant.getTenant(appDef);
        verifyApplicationCFs(tenant.getKeyspace(), oldAppDef, appDef);
    }   // initializeApplication
    
    // Verify that the given application's options are valid for the Spider service.
    @Override
    public void validateSchema(ApplicationDefinition appDef) {
        checkServiceState();
        validateApplication(appDef);
    }   // validateSchema
    
    @Override
    public Collection<Task> getAppTasks(ApplicationDefinition appDef) {
        List<Task> appTasks = new ArrayList<>();
        for (TableDefinition tableDef : appDef.getTableDefinitions().values()) {
            String agingFreq = tableDef.getOption(CommonDefs.OPT_AGING_CHECK_FREQ);
            if (agingFreq != null) {
                Task task =
                    new Task(appDef.getAppName(), tableDef.getTableName(), "data-aging", agingFreq, SpiderDataAger.class);
                appTasks.add(task);
            }
        }
        return appTasks;
    }   // getAppTasks
    
    //----- StorageService object query methods
    
    /**
     * Get all scalar and link fields for the object in the given table with the given ID.
     * 
     * @param tableDef  {@link TableDefinition} in which object resides.
     * @param objID     Object ID.
     * @return          {@link DBObject} containing all object scalar and link fields, or
     *                  null if there is no such object.
     */
    @Override
    public DBObject getObject(TableDefinition tableDef, String objID) {
        checkServiceState();
        String storeName = objectsStoreName(tableDef);
        Iterator<DColumn> colIter = DBService.instance().getAllColumns(Tenant.getTenant(tableDef), storeName, objID);
        if (colIter == null) {
            return null;
        }

        DBObject dbObj = createObject(tableDef, objID, colIter);
        addShardedLinkValues(tableDef, dbObj);
        return dbObj;
    }   // getObject
    
    @Override
    public SearchResultList objectQueryURI(TableDefinition tableDef, String uriQuery) {
        checkServiceState();
        return new ObjectQuery(tableDef, uriQuery).query();
    }   // objectQueryURI
    
    @Override
    public SearchResultList objectQueryDoc(TableDefinition tableDef, UNode rootNode) {
        checkServiceState();
        return new ObjectQuery(tableDef, rootNode).query();
    }   // objectQueryDoc    

    @Override
    public AggregateResult aggregateQueryURI(TableDefinition tableDef, String uriQuery) {
        checkServiceState();
        Aggregate aggregate = new Aggregate(tableDef);
        aggregate.parseParameters(uriQuery);
        try {
        	aggregate.execute();
        } catch (IOException e) {
        	throw new RuntimeException("Aggregation failed with " + e);
        }
        return aggregate.getResult();
    }   // aggregateQuery
    
    @Override
    public AggregateResult aggregateQueryDoc(TableDefinition tableDef, UNode rootNode) {
        checkServiceState();
        Aggregate aggregate = new Aggregate(tableDef);
        aggregate.parseParameters(rootNode);
        try {
        	aggregate.execute();
        } catch (IOException e) {
        	throw new RuntimeException("Aggregation failed with " + e);
        }
        return aggregate.getResult();
    }   // aggregateQuery
    
    //----- StorageService object update methods
    
    // Add and/or update a batch of objects. For a Spider application, the store name must
    // be a table name.
    @Override
    public BatchResult addBatch(ApplicationDefinition appDef, String tableName, DBObjectBatch batch) {
        return addBatch(appDef, tableName, batch, null);
    }   // addBatch
    
    // Add and/or update a batch of objects. For a Spider application, the store name must
    // be a table name.
    @Override
    public BatchResult addBatch(ApplicationDefinition appDef, String tableName,
                                DBObjectBatch batch, Map<String, String> options) {
        checkServiceState();
        TableDefinition tableDef = appDef.getTableDef(tableName);
        Utils.require(tableDef != null || appDef.allowsAutoTables(),
                      "Unknown table for application '%s': %s", appDef.getAppName(), tableName);
        Utils.require(options == null || options.size() == 0, "No parameters expected");
        
        if (tableDef == null && appDef.allowsAutoTables()) {
            tableDef = addAutoTable(appDef, tableName);
        }
        
        BatchObjectUpdater batchUpdater = new BatchObjectUpdater(tableDef);
        return batchUpdater.addBatch(batch);
    }   // addBatch

    // Delete a batch of objects. For a Spider application, the store name must be a table
    // name, and (for now) all objects in the batch must belong to that table.
    @Override
    public BatchResult deleteBatch(ApplicationDefinition appDef, String tableName, DBObjectBatch batch) {
        checkServiceState();
        TableDefinition tableDef = appDef.getTableDef(tableName);
        Utils.require(tableDef != null, "Unknown table for application '%s': %s", appDef.getAppName(), tableName);
        
        Set<String> objIDSet = new HashSet<>();
        for (DBObject dbObj : batch.getObjects()) {
            Utils.require(!Utils.isEmpty(dbObj.getObjectID()), "All objects must have _ID defined");
            objIDSet.add(dbObj.getObjectID());
        }
        BatchObjectUpdater batchUpdater = new BatchObjectUpdater(tableDef);
        return batchUpdater.deleteBatch(objIDSet);
    }   // deleteBatch

    // Same as addBatch()
    @Override
    public BatchResult updateBatch(ApplicationDefinition appDef, String storeName, DBObjectBatch batch) {
        return addBatch(appDef, storeName, batch, null);
    }   // updateBatch
    
    // Same as addBatch()
    @Override
    public BatchResult updateBatch(ApplicationDefinition appDef, String storeName,
                                   DBObjectBatch batch, Map<String, String> paramMap) {
        return addBatch(appDef, storeName, batch, paramMap);
    }   // updateBatch
    
    //----- SpiderService-specific public static methods
    
    /**
     * Return the column name used to store the given value for the given link. The column
     * name uses the format:
     * <pre>
     *   ~{link name}/{object ID}
     * </pre>
     * This method should be used for unsharded links or link values to that refer to an
     * object whose shard number is 0.
     * 
     * @param linkDef   {@link FieldDefinition} of a link.
     * @param objID     ID of an object referenced by the link.
     * @return          Column name that is used to store a value for the given link and
     *                  object ID.
     * @see             #shardedLinkTermRowKey(FieldDefinition, String, int)
     */
    public static String linkColumnName(FieldDefinition linkDef, String objID) {
        assert linkDef.isLinkField();
        StringBuilder buffer = new StringBuilder();
        buffer.append("~");
        buffer.append(linkDef.getName());
        buffer.append("/");
        buffer.append(objID);
        return buffer.toString();
    }   // linkColumnName

    /**
     * Return the store name (ColumnFamily) in which objects are stored for the given table.
     * This name is either {table name} or {application name}_{table name} truncated to
     * {@link #MAX_CF_NAME_LENGTH} if needed.
     * 
     * @param tableDef  {@link TableDefinition} of a table.
     * @return          Store name (ColumnFamily) in which objects are stored for the given
     *                  table.
     */
    public static String objectsStoreName(TableDefinition tableDef) {
        String storeName = null;
        storeName = tableDef.getAppDef().getAppName() + "_" + tableDef.getTableName();
        return Utils.truncateTo(storeName, MAX_CF_NAME_LENGTH);
    }   // objectsStoreName
    
    /**
     * Convert the given scalar value to binary form. This method returns the appropriate
     * format for storage based on the scalar's type. This method is the twin of
     * {@link #scalarValueToString(TableDefinition, String, byte[])}
     * 
     * @param tableDef      {@link TableDefinition} of table in which field resides.
     * @param fieldName     Scalar field name.
     * @param fieldValue    Field value in string form (may be null).
     * @return              Binary storage format.
     */
    public static byte[] scalarValueToBinary(TableDefinition tableDef, String fieldName, String fieldValue) {
        if (fieldValue == null || fieldValue.length() == 0) {
            return null;    // means "don't store".
        }
        FieldDefinition fieldDef = tableDef.getFieldDef(fieldName);
        if (fieldDef != null && fieldDef.isBinaryField()) {
            return fieldDef.getEncoding().decode(fieldValue);
        } else {
            return Utils.toBytes(fieldValue);
        }
    }   // scalarValueToBinary

    /**
     * Convert the given binary scalar field value to string form based on its definition.
     * This method is the twin of
     * {@link #scalarValueToBinary(TableDefinition, String, String)}.
     * 
     * @param tableDef      TableDefinition that defines field.
     * @param fieldName     Name of a scalar field.
     * @param colValue      Binary column value.
     * @return              Scalar value as a string.
     */
    public static String scalarValueToString(TableDefinition tableDef, String fieldName, byte[] colValue) {
        // Only binary fields are treated specially.
        FieldDefinition fieldDef = tableDef.getFieldDef(fieldName);
        if (fieldDef != null && fieldDef.isBinaryField()) {
            return fieldDef.getEncoding().encode(colValue);
        } else {
            return Utils.toString(colValue);
        }
    }   // scalarValueToString

    /**
     * Return the row key for the Terms record that represents a link shard record for the
     * given link, owned by the given object ID, referencing target objects in the given
     * shard. The key format is:
     * <pre>
     *      {shard number}/{link name}/{object ID}
     * </pre>
     * 
     * @param linkDef       {@link FieldDefinition} of a sharded link.
     * @param objID         ID of object that owns link field.
     * @param shardNumber   Shard number in the link's extent table.
     * @return
     */
    public static String shardedLinkTermRowKey(FieldDefinition linkDef, String objID, int shardNumber) {
        assert linkDef.isLinkField() && linkDef.isSharded() && shardNumber > 0;
        StringBuilder shardPrefix = new StringBuilder();
        shardPrefix.append(shardNumber);
        shardPrefix.append("/");
        shardPrefix.append(linkColumnName(linkDef, objID));
        return shardPrefix.toString();
    }   // shardedLinkTermRowKey
    
    /**
     * Create the Terms row key for the given table, object, field name, and term.
     * 
     * @param tableDef  {@link TableDefinition} of table that owns object.
     * @param dbObj     DBObject that owns field.
     * @param fieldName Field name.
     * @param term      Term to be indexed.
     * @return
     */
    public static String termIndexRowKey(TableDefinition tableDef, DBObject dbObj, String fieldName, String term) {
        StringBuilder termRecKey = new StringBuilder();
        int shardNumber = tableDef.getShardNumber(dbObj);
        if (shardNumber > 0) {
            termRecKey.append(shardNumber);
            termRecKey.append("/");
        }
        termRecKey.append(FieldAnalyzer.makeTermKey(fieldName, term));
        return termRecKey.toString();
    }   // termIndexRowKey

    /**
     * Return the store name (ColumnFamily) in which terms are stored for the given table.
     * This name is the object store name appended with "_terms". If the object store name
     * is too long, it is truncated so that the terms store name is less than
     * {@link #MAX_CF_NAME_LENGTH}.
     * 
     * @param tableDef  {@link TableDefinition} of a table.
     * @return          Store name (ColumnFamily) in which terms are stored for the given
     *                  table.
     */
    public static String termsStoreName(TableDefinition tableDef) {
        String objStoreName = Utils.truncateTo(objectsStoreName(tableDef), MAX_CF_NAME_LENGTH - "_Terms".length());
        return objStoreName + "_Terms";
    }   // termsStoreName
    
    //----- SpiderService public methods
    
    /**
     * Retrieve the requested scalar fields for the given object IDs in the given table.
     * The map returned is object IDs -> field names -> field values. The map will be
     * empty (but not null) if (1) objIDs is empty, (2) fieldNames is empty, or (3) none
     * of the requested object IDs were found. An entry in the outer map may exist with an
     * empty field map if none of the requested fields were found for the corresponding
     * object.
     * 
     * @param tableDef      {@link TableDefinition} of table to query.
     * @param objIDs        Collection of object IDs to fetch.
     * @param fieldNames    Collection of field names to fetch.
     * @return              Map of object IDs -> field names -> field values for all
     *                      objects found. Objects not found will have no entry in the
     *                      outer map. The map will be empty if no objects were found.
     */
    public Map<String, Map<String, String>> getObjectScalars(TableDefinition    tableDef,
                                                             Collection<String> objIDs,
                                                             Collection<String> fieldNames) {
        checkServiceState();
        Map<String, Map<String, String>> objScalarMap = new HashMap<>();
        if (objIDs.size() > 0 && fieldNames.size() > 0) {
            String storeName = objectsStoreName(tableDef);
            Iterator<DRow> rowIter =
                DBService.instance().getRowsColumns(Tenant.getTenant(tableDef), storeName, objIDs, fieldNames);
            while (rowIter.hasNext()) {
                DRow row = rowIter.next();
                Map<String, String> scalarMap = new HashMap<>();
                objScalarMap.put(row.getKey(), scalarMap);
                Iterator<DColumn> colIter = row.getColumns();
                while (colIter.hasNext()) {
                    DColumn col = colIter.next();
                    String fieldValue = scalarValueToString(tableDef, col.getName(), col.getRawValue());
                    scalarMap.put(col.getName(), fieldValue);
                }
            }
        }
        return objScalarMap;
    }   // getObjectScalars

    /**
     * Retrieve a single scalar field for the given object IDs in the given table.
     * The map returned is object IDs -> field value. The map will be empty (but not null)
     * if objIDs is empty or none of the requested object IDs have a value for the
     * requested field name.
     * 
     * @param tableDef      {@link TableDefinition} of table to query.
     * @param objIDs        Collection of object IDs to fetch.
     * @param fieldName     Scalar field name to get values for.
     * @return              Map of object IDs -> field values for values found.
     */
    public Map<String, String> getObjectScalar(TableDefinition    tableDef,
                                               Collection<String> objIDs,
                                               String             fieldName) {
        checkServiceState();
        Map<String, String> objScalarMap = new HashMap<>();
        if (objIDs.size() > 0) {
            String storeName = objectsStoreName(tableDef);
            Iterator<DRow> rowIter =
                DBService.instance().getRowsColumns(Tenant.getTenant(tableDef), storeName, objIDs, Arrays.asList(fieldName));
            while (rowIter.hasNext()) {
                DRow row = rowIter.next();
                Iterator<DColumn> colIter = row.getColumns();
                while (colIter.hasNext()) {
                    DColumn col = colIter.next();
                    if (col.getName().equals(fieldName)) {
                        String fieldValue = scalarValueToString(tableDef, col.getName(), col.getRawValue());
                        objScalarMap.put(row.getKey(), fieldValue);
                    }
                }
            }
        }
        return objScalarMap;
    }   // getObjectScalar
    
    /**
     * Get the starting date of the shard with the given number in the given sharded
     * table. If the given table has not yet started the given shard, null is returned.
     * 
     * @param tableDef      {@link TableDefinition} of a sharded table.
     * @param shardNumber   Shard number (must be > 0).
     * @return              Start date of the given shard or null if no objects have been
     *                      stored in the given shard yet.
     */
    public void verifyShard(TableDefinition tableDef, int shardNumber) {
        assert tableDef.isSharded();
        assert shardNumber > 0;
        checkServiceState();
        m_shardCache.verifyShard(tableDef, shardNumber);
    }   // verifyShard

    /**
     * Get all known shards for the given table. Each shard is defined in a column in the
     * "_shards" row of the table's Terms store. If the given table is not sharded, an
     * empty map is returned.
     * 
     * @param tableDef  Sharded table to get current shards for.
     * @return          Map of shard numbers to shard start dates. May be empty but will
     *                  not be null.
     */
    public Map<Integer, Date> getShards(TableDefinition tableDef) {
        checkServiceState();
        if (tableDef.isSharded()) {
            return m_shardCache.getShardMap(tableDef);
        } else {
            return new HashMap<>();
        }
    }   // getShards
    
    //----- Private methods
    
    // Singleton creation only
    private SpiderService() {}

    // Add an implicit table to the given application and return its new TableDefinition.
    private TableDefinition addAutoTable(ApplicationDefinition appDef, String tableName) {
        m_logger.debug("Adding implicit table '{}' to application '{}'", tableName, appDef.getAppName());
        Tenant tenant = Tenant.getTenant(appDef);
        TableDefinition tableDef = new TableDefinition(appDef);
        tableDef.setTableName(tableName);
        appDef.addTable(tableDef);
        SchemaService.instance().defineApplication(appDef);
        appDef = SchemaService.instance().getApplication(tenant, appDef.getAppName());
        return appDef.getTableDef(tableName);
    }   // addAutoTable
    
    // Add sharded link values, if any, to the given DBObject.
    private void addShardedLinkValues(TableDefinition tableDef, DBObject dbObj) {
        for (FieldDefinition fieldDef : tableDef.getFieldDefinitions()) {
            if (fieldDef.isLinkField() && fieldDef.isSharded()) {
                TableDefinition extentTableDef = tableDef.getLinkExtentTableDef(fieldDef);
                Set<Integer> shardNums = getShards(extentTableDef).keySet();
                Set<String> values = getShardedLinkValues(dbObj.getObjectID(), fieldDef, shardNums);
                dbObj.addFieldValues(fieldDef.getName(), values);
            }
        }
    }   // addShardedlinkValues
    
    // Create a DBObject from the given scalar/link column values.
    private DBObject createObject(TableDefinition tableDef, String objID, Iterator<DColumn> colIter) {
        DBObject dbObj = new DBObject();
        dbObj.setObjectID(objID);
        while (colIter.hasNext()) {
            DColumn col = colIter.next();
            Pair<String, String> linkCol = extractLinkValue(tableDef, col.getName());
            if (linkCol == null) {
                String fieldName = col.getName();
                String fieldValue = scalarValueToString(tableDef, col.getName(), col.getRawValue());
                FieldDefinition fieldDef = tableDef.getFieldDef(fieldName);
                if (fieldDef != null && fieldDef.isCollection()) {
                    // MV scalar field
                    Set<String> values = Utils.split(fieldValue, CommonDefs.MV_SCALAR_SEP_CHAR);
                    dbObj.addFieldValues(fieldName, values);
                } else {
                    dbObj.addFieldValue(col.getName(), fieldValue);
                }
            // Skip links no longer present in schema
            } else if (tableDef.isLinkField(linkCol.left)) {
                dbObj.addFieldValue(linkCol.left, linkCol.right);
            }
        }
        return dbObj;
    }   // createObject
    
    // Delete all ColumnFamilies used by the given application. Only delete the ones that
    // actually exist in case a previous delete-app failed.
    private void deleteApplicationCFs(ApplicationDefinition appDef) {
        // Table-level CFs:
        Tenant tenant = Tenant.getTenant(appDef);
        for (TableDefinition tableDef : appDef.getTableDefinitions().values()) {
            DBService.instance().deleteStoreIfPresent(tenant, objectsStoreName(tableDef));
            DBService.instance().deleteStoreIfPresent(tenant, termsStoreName(tableDef));
        }
    }   // deleteApplicationCFs
    
    // Get all target object IDs for the given sharded link.
    private Set<String> getShardedLinkValues(String objID, FieldDefinition linkDef, Set<Integer> shardNums) {
        Set<String> values = new HashSet<String>();
        if (shardNums.size() == 0) {
            return values;
        }
        
        // Construct row keys for the link's possible Terms records.
        Set<String> termRowKeys = new HashSet<String>();
        for (Integer shardNumber : shardNums) {
            termRowKeys.add(shardedLinkTermRowKey(linkDef, objID, shardNumber));
        }
        TableDefinition tableDef = linkDef.getTableDef();
        String termStore = termsStoreName(linkDef.getTableDef());
        Iterator<DRow> rowIter = DBService.instance().getRowsAllColumns(Tenant.getTenant(tableDef), termStore, termRowKeys);
        
        // We only need the column names from each row.
        while (rowIter.hasNext()) {
            DRow row = rowIter.next();
            Iterator<DColumn> colIter = row.getColumns();
            while (colIter.hasNext()) {
                values.add(colIter.next().getName());
            }
        }
        return values;
    }   // getShardedLinkValues

    // If the given column name represents a link value, return the link's field name and
    // target object ID as a Pair<String, String>
    private Pair<String, String> extractLinkValue(TableDefinition tableDef, String colName) {
        if (colName.length() < 3 || colName.charAt(0) != '~') {
            return null;
        }
        
        // A '/' should separate the field name and object ID value. Example: ~foo/xyz
        int slashInx = 1;
        while (slashInx < colName.length() && colName.charAt(slashInx) != '/') {
            slashInx++;
        }
        if (slashInx >= colName.length()) {
            return null;
        }
        String fieldName = colName.substring(1, slashInx);
        String objID = colName.substring(slashInx + 1);
        return Pair.create(fieldName, objID);
    }   // extractLinkValue
    
    // Verify that the given shard-starting date is in the format YYYY-MM-DD. If the format
    // is bad, just return false.
    private boolean isValidShardDate(String shardDate) {
        try {
            // If the format is invalid, a ParseException is thrown.
            Utils.dateFromString(shardDate);
            return true;
        } catch (IllegalArgumentException ex) {
            return false;
        }
    }   // isValidShardDate
    
    // Validate the given application against SpiderService-specific constraints.
    private void validateApplication(ApplicationDefinition appDef) {
        boolean bAutoTablesSet = false;
        for (String optName : appDef.getOptionNames()) {
            String optValue = appDef.getOption(optName);
            switch (optName) {
            case CommonDefs.AUTO_TABLES:
                validateBooleanOption(optName, optValue);
                bAutoTablesSet = true;
                break;
                
            case CommonDefs.OPT_STORAGE_SERVICE:
                assert optValue.equals(this.getClass().getSimpleName());
                break;
                
            case CommonDefs.OPT_TENANT:
                // Ignore
                break;
                
            case CommonDefs.OPT_AGING_CHECK_FREQ:
                new TaskFrequency(optValue);    // default for all tables
                break;
                
            default:
                throw new IllegalArgumentException("Unknown option for SpiderService application: " + optName);
            }
        }
        
        if (!bAutoTablesSet) {
            // For backwards compatibility, default AutoTables to true.
            appDef.setOption(CommonDefs.AUTO_TABLES, "true");
        }

        for (TableDefinition tableDef : appDef.getTableDefinitions().values()) {
            validateTable(tableDef);
        }
    }   // validateApplication

    // Validate that the given string is a valid Booleab value.
    private void validateBooleanOption(String optName, String optValue) {
        if (!optValue.equalsIgnoreCase("true") && !optValue.equalsIgnoreCase("false")) {
            throw new IllegalArgumentException("Boolean value expected for '" + optName + "' option: " + optValue);
        }
    }   // validateBooleanOption
    
    // Validate the given field against SpiderService-specific constraints.
    private void validateField(FieldDefinition fieldDef) {
        Utils.require(!fieldDef.isXLinkField(), "Xlink fields are not allowed in Spider applications");
        
        // Validate scalar field analyzer.
        if (fieldDef.isScalarField()) {
            String analyzerName = fieldDef.getAnalyzerName();
            if (Utils.isEmpty(analyzerName)) {
                analyzerName = FieldType.getDefaultAnalyzer(fieldDef.getType());
                fieldDef.setAnalyzer(analyzerName);
            }
            FieldAnalyzer.verifyAnalyzer(fieldDef);
        }
    }   // validateField

    // Validate the given table against SpiderService-specific constraints.
    private void validateTable(TableDefinition tableDef) {
        for (String optName : tableDef.getOptionNames()) {
            String optValue = tableDef.getOption(optName);
            switch (optName) {
            case CommonDefs.OPT_AGING_FIELD:
                validateTableOptionAgingField(tableDef, optValue);
                break;
            case CommonDefs.OPT_RETENTION_AGE:
                validateTableOptionRetentionAge(tableDef, optValue);
                break;
            case CommonDefs.OPT_SHARDING_FIELD:
                validateTableOptionShardingField(tableDef, optValue);
                break;
            case CommonDefs.OPT_SHARDING_GRANULARITY:
                validateTableOptionShardingGranularity(tableDef, optValue);
                break;
            case CommonDefs.OPT_SHARDING_START:
                validateTableOptionShardingStart(tableDef, optValue);
                break;
            case CommonDefs.OPT_AGING_CHECK_FREQ:
                validateTableOptionAgingCheckFrequency(tableDef, optValue);
                break;
            default:
                Utils.require(false, "Unknown option for SpiderService table: " + optName);
            }
        }
        
        for (FieldDefinition fieldDef : tableDef.getFieldDefinitions()) {
            validateField(fieldDef);
        }
        
        if (tableDef.getOption(CommonDefs.OPT_AGING_FIELD) != null &&
            tableDef.getOption(CommonDefs.OPT_AGING_CHECK_FREQ) == null) {
            String agingCheckFreq = tableDef.getAppDef().getOption(CommonDefs.OPT_AGING_CHECK_FREQ);
            if (Utils.isEmpty(agingCheckFreq)) {
                agingCheckFreq = "1 DAY";
            }
            tableDef.setOption(CommonDefs.OPT_AGING_CHECK_FREQ, agingCheckFreq);
        }
    }   // validateTable
    
    // Validate the table option "aging-check-frequency"
    private void validateTableOptionAgingCheckFrequency(TableDefinition tableDef,
                                                        String optValue) {
        new TaskFrequency(optValue);
        Utils.require(tableDef.getOption(CommonDefs.OPT_AGING_FIELD) != null,
                        "Option 'aging-check-frequency' requires option 'aging-field'");
    }   // validateTableOptionAgingCheckFrequency

    // Validate the table option "aging-field".
    private void validateTableOptionAgingField(TableDefinition tableDef, String optValue) {
        FieldDefinition agingFieldDef = tableDef.getFieldDef(optValue);
        Utils.require(agingFieldDef != null,
                      "Aging field has not been defined: " + optValue);
        assert agingFieldDef != null;   // Make FindBugs happy
        Utils.require(agingFieldDef.getType() == FieldType.TIMESTAMP,
                      "Aging field must be a timestamp field: " + optValue);
        Utils.require(tableDef.getOption(CommonDefs.OPT_RETENTION_AGE) != null,
                      "Option 'aging-field' requires option 'retention-age'");
    }   // validateTableOptionAgingField
    
    // Validate the table option "retention-age".
    private void validateTableOptionRetentionAge(TableDefinition tableDef, String optValue) {
        RetentionAge retAge = new RetentionAge(optValue); // throws if invalid format
        optValue = retAge.toString();
        tableDef.setOption(CommonDefs.OPT_RETENTION_AGE, optValue); // rewrite value
        Utils.require(tableDef.getOption(CommonDefs.OPT_AGING_FIELD) != null,
                      "Option 'retention-age' requires option 'aging-field'");
    }   // validateTableOptionRetentionAge
    
    // Validate the table option "sharding-field".
    private void validateTableOptionShardingField(TableDefinition tableDef, String optValue) {
        // Verify that the sharding-field exists and is a timestamp field.
        FieldDefinition shardingFieldDef = tableDef.getFieldDef(optValue);
        Utils.require(shardingFieldDef != null,
                      "Sharding field has not been defined: " + optValue);
        assert shardingFieldDef != null;    // Make FindBugs happy
        Utils.require(shardingFieldDef.getType() == FieldType.TIMESTAMP,
                      "Sharding field must be a timestamp field: " + optValue);
        Utils.require(!shardingFieldDef.isCollection(),
                        "Sharding field cannot be a collection: " + optValue);
        
        // Default sharding-granularity to MONTH.
        if (tableDef.getOption(CommonDefs.OPT_SHARDING_GRANULARITY) == null) {
            tableDef.setOption(CommonDefs.OPT_SHARDING_GRANULARITY, "MONTH");
        }
        
        // Default sharding-start to "tomorrow".
        if (tableDef.getOption(CommonDefs.OPT_SHARDING_START) == null) {
            GregorianCalendar startDate = new GregorianCalendar(Utils.UTC_TIMEZONE);
            startDate.add(Calendar.DAY_OF_MONTH, 1);  // adds 1 day
            String startOpt = String.format("%04d-%02d-%02d",
                                            startDate.get(Calendar.YEAR),
                                            startDate.get(Calendar.MONTH)+1,    // 0-relative!
                                            startDate.get(Calendar.DAY_OF_MONTH));
            tableDef.setOption(CommonDefs.OPT_SHARDING_START, startOpt);
        }
    }   // validateTableOptionShardingField
    
    // Validate the table option "sharding-granularity".
    private void validateTableOptionShardingGranularity(TableDefinition tableDef, String optValue) {
        ShardingGranularity shardingGranularity = ShardingGranularity.fromString(optValue);
        Utils.require(shardingGranularity != null,
                      "Unrecognized 'sharding-granularity' value: " + optValue);
        
        // 'sharding-granularity' requires 'sharding-field'
        Utils.require(tableDef.getOption(CommonDefs.OPT_SHARDING_FIELD) != null,
                      "Option 'sharding-granularity' requires option 'sharding-field'");
    }   // validateTableOptionShardingGranularity
    
    // Validate the table option "sharding-start".
    private void validateTableOptionShardingStart(TableDefinition tableDef, String optValue) {
        Utils.require(isValidShardDate(optValue),
                      "'sharding-start' must be YYYY-MM-DD: " + optValue);
        GregorianCalendar shardingStartDate = new GregorianCalendar(Utils.UTC_TIMEZONE);
        shardingStartDate.setTime(Utils.dateFromString(optValue));

        // 'sharding-start' requires 'sharding-field'
        Utils.require(tableDef.getOption(CommonDefs.OPT_SHARDING_FIELD) != null,
                      "Option 'sharding-start' requires option 'sharding-field'");
    }   // validateTableOptionShardingStart

    // Verify that all ColumnFamilies needed for the given application exist.
    private void verifyApplicationCFs(String keyspace,
                                      ApplicationDefinition oldAppDef,
                                      ApplicationDefinition appDef) {
        // Add new table-level CFs:
        DBService dbService = DBService.instance();
        Tenant tenant = Tenant.getTenant(appDef);
        for (TableDefinition tableDef : appDef.getTableDefinitions().values()) {
            dbService.createStoreIfAbsent(tenant, objectsStoreName(tableDef), true);
            dbService.createStoreIfAbsent(tenant, termsStoreName(tableDef), true);
        }
        
        // Delete obsolete table-level CFs:
        if (oldAppDef != null) {
            for (TableDefinition oldTableDef : oldAppDef.getTableDefinitions().values()) {
                if (appDef.getTableDef(oldTableDef.getTableName()) == null) {
                    dbService.deleteStoreIfPresent(tenant, objectsStoreName(oldTableDef));
                    dbService.deleteStoreIfPresent(tenant, termsStoreName(oldTableDef));
                }
            }
        }
    }   // verifyApplicationCFs
    
}   // class SpiderService
