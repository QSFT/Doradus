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

package com.dell.doradus.client.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import com.dell.doradus.client.ApplicationSession;
import com.dell.doradus.client.Client;
import com.dell.doradus.client.OLAPSession;
import com.dell.doradus.client.QueryResult;
import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.DBObject;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.FieldType;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;

/**
 * CSVDumper dumps all objects in a given Doradus application to CSV files. It is the
 * counterpart to {@link CSVLoader}. CSVDumper opens a Doradus database and dumps all
 * objects for each table to its own CSV file. It uses a configurable number of workers to
 * dump tables in parallel. Each file is called "{table name}.csv" and is written into the
 * current directory. Existing files, if present, are replaced. Each CSV record holds all
 * scalar and link field values for one object. The values of scalar collections and link
 * fields are concatenated into a single value, separated by "~". For example, the scalar
 * collections Colors could become a column with the value "red~blue~green". All column
 * names match the corresponding field name except for the _ID field, whose name can be
 * configured. The default is "Key".
 * <p>
 * Default options are defined in {@link CSVConfig} and can be overwritten
 * programmatically via {@link CSVConfig#set(String, String)} or via parameters to
 * {@link #main(String[])}. For example, the parameter "-optimize true" causes only one
 * link in each bi-directional relationship to be dumped, thereby reducing file size.
 * <p>
 * Because this utility only dumps fields named in the schema, it will skip any unnamed
 * fields. The files created by CSVDumper can be loaded back into another database using
 * CSVLoader as long as the same schema is used and all fields are named.
 * <p>
 * This utility is suitable for small databases (millions of objects), but dumping larger
 * databases to CSV files is probably impractical.
 */
public class CSVDumper {
    // Default values.
    private static final int LINK_FANOUT_SAMPLE_SIZE = 100;

    // Members:
    private CSVConfig                   m_config = CSVConfig.instance();
    private Client                      m_client;
    private ApplicationSession          m_session;
    private Map<String, Set<String>>    m_dbSuppressedFieldMap;
    private AtomicLong                  m_totalTables = new AtomicLong();
    private AtomicLong                  m_totalObjects = new AtomicLong();
    private AtomicLong                  m_totalBytes = new AtomicLong();
    private ApplicationDefinition       m_appDef;
    private Iterator<TableDefinition>   m_tableIterator;
    private List<DumpWorker>            m_workerList = new ArrayList<DumpWorker>();
    
    // Logging interface:
    private Logger m_logger = LoggerFactory.getLogger(getClass().getSimpleName());
    
    //----- Public methods
    
    /**
     * Run with "-?" to get details on program arguments. Also, see {@link CSVConfig} for
     * a description of parameters.
     * 
     * @param args  Program arguments.
     */
    public static void main(String[] args) {
        try {
        	CSVDumper app = new CSVDumper();
        	app.parseArgs(args);
        	app.run();
            System.exit(0);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }        
    }   // main

    /**
     * Performs the CSV dump as defined by the {@link CSVConfig} singleton object.
     * This method can be called instead of {@link #main(String[])} for programmatic
     * access. Parameters should be set first by direct access to the CSVConfig
     * object or by calling {@link CSVConfig#set(String, String)}.
     */
    public void run() {
        loadSchema();
        m_tableIterator = m_appDef.getTableDefinitions().values().iterator();
        
        m_logger.info("Starting {} workers", m_config.workers);
        long startTime = (new Date()).getTime();
        for (int workerNo = 1; workerNo <= m_config.workers; workerNo++) {
            DumpWorker dumpWorker = new DumpWorker(workerNo);
            dumpWorker.start();
            m_workerList.add(dumpWorker);
        }
        
        for (DumpWorker dumpWorker : m_workerList) {
            try {
                dumpWorker.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }
        
        long stopTime = System.currentTimeMillis();
        m_logger.info("Total tables dumped:    {}", m_totalTables.get());
        m_logger.info("Total objects dumped:   {}", m_totalObjects.get());
        m_logger.info("Total bytes written:    {}", m_totalBytes.get());
        long totalMillis = stopTime - startTime;
        m_logger.info("Total time for dump:    {}", Utils.formatElapsedTime(totalMillis));
        m_logger.info("Average objects/sec:    {}", ((m_totalObjects.get() * 1000) / totalMillis));
        m_logger.info("Average bytes/sec:      {}", ((m_totalBytes.get() * 1000) / totalMillis));
    }   // run
    
    //----- Package private methods used by DumpWorker
    
    // Return the next table to be dumped.
    synchronized TableDefinition nextTableDef() {
        if (m_tableIterator.hasNext()) {
            m_totalTables.incrementAndGet();
            return m_tableIterator.next();
        }
        return null;
    }   // nextTableDef 
    
    // Increment the number of objects dumped
    void incrementObjectCount(long count) {
        m_totalObjects.addAndGet(count);
    }   // incrementObjectCount
    
    // Increment the number of bytes dumped
    void incrementObjectByteCount(long count) {
        m_totalBytes.addAndGet(count);
    }   // incrementByteCount
    
    /**
     * Return true if the given {@link FieldDefinition} should be suppressed when its
     * table is dumped to a CSV file. Currently, this will only occur for link fields
     * when -optimize is specified.
     *  
     * @param fieldDef  {@link FieldDefinition} of a field.
     * @return          True if the field should be suppressed.
     */
    boolean shouldSuppressField(FieldDefinition fieldDef) {
        if (!m_config.optimize) {
            return false;
        }
        
        Set<String> suppressedFields = m_dbSuppressedFieldMap.get(fieldDef.getTableName());
        return suppressedFields != null && suppressedFields.contains(fieldDef.getName()); 
    }   // shouldSuppressLinkField
    
    //----- Private methods

    private static void usage() {
        display("Usage: CSVDumper <params>");
        display("where <params> are:");
        display("   -app <name>         Doradus application name. Default is: {}", CSVConfig.DEFAULT_APPNAME);
        display("   -batchsize <#>      Batch size. Default is: {}", CSVConfig.DEFAULT_BATCH_SIZE);
        display("   -compress [T|F]     Compress messages. Default is: {}", CSVConfig.DEFAULT_COMPRESS);
        display("   -host <host>        Doradus server host name. Default is: {}", CSVConfig.DEFAULT_HOST);
        display("   -id <name>          Column name of ID field. Default is: {}", CSVConfig.DEFAULT_ID_FIELD);
        display("   -port <port>        Doradus server port. Default is: {}", CSVConfig.DEFAULT_PORT);
        display("   -root <folder>      Root folder of CSV files. Default is: {}", CSVConfig.DEFAULT_ROOT);
        display("   -shard <name>       (OLAP only): Name of shard to load. Default is: {}", CSVConfig.DEFAULT_SHARD);
        display("   -workers <#>        # of worker threads. Default is: {}", CSVConfig.DEFAULT_WORKERS);
        display("Reads all records in all tables for the given OLAP or Spider application and dumps");
        display("them to CSV files found in 'root' folder. TLS options are also available.");
        System.exit(0);
    }   // usage
    
    // Write the given message to stdout only. Uses {}-style parameters
    private static void display(String format, Object... args) {
        System.out.println(MessageFormatter.arrayFormat(format, args).getMessage());
    }   // display
    
    // Parse args into CSVConfig object.
    private void parseArgs(String[] args) {
        int index = 0;
        while (index < args.length) {
            String name = args[index];
            if (name.equals("-?") || name.equalsIgnoreCase("-help")) {
                usage();
            }
            if (name.charAt(0) != '-') {
                m_logger.error("Unrecognized parameter: {}", name);
                usage();
            }
            if (++index >= args.length) {
                m_logger.error("Another parameter expected after '{}'", name);
                usage();
            }
            String value = args[index];
            try {
                m_config.set(name.substring(1), value);
            } catch (Exception e) {
                m_logger.error(e.toString());
                usage();
            }
            index++;
        }
        if (!m_config.root.endsWith(File.separator)) {
            m_config.root += File.separator;
        }
    }   // parseArgs
    
    // Connect to the Doradus server and download the requested application's schema.
    private void loadSchema() {
        m_logger.info("Loading schema for application: {}", m_config.app);
        m_client = new Client(m_config.host, m_config.port, m_config.getTLSParams());
        m_session = m_client.openApplication(m_config.app); // throws if unknown app
        m_appDef = m_session.getAppDef();
        if (m_config.optimize) {
            computeLinkFanouts();
        }
    }   // loadSchema

    // Simple class for holding mutable float objects.
    static class MutableFloat {
        float m_value;
        MutableFloat(float value) {
            m_value = value;
        }
    }   // static class MutableFloat
    
    // Compute approximate link fanouts for each table by querying up to 100 objects each.
    private void computeLinkFanouts() {
        // Table name -> link field name -> average number of links per owning object.
        Map<String, Map<String, MutableFloat>> dbLinkFanoutMap =
            new HashMap<String, Map<String, MutableFloat>>();
        
        m_logger.info("Computing link fanouts");
        for (TableDefinition tableDef : m_appDef.getTableDefinitions().values()) {
            Map<String, MutableFloat> tableLinkFanoutMap = new HashMap<String, MutableFloat>();
            dbLinkFanoutMap.put(tableDef.getTableName(), tableLinkFanoutMap);
            computeLinkFanouts(tableDef, tableLinkFanoutMap);
        }
        
        m_dbSuppressedFieldMap = new HashMap<String, Set<String>>();
        Set<FieldDefinition> decidedLinkSet = new HashSet<FieldDefinition>();
        for (TableDefinition tableDef : m_appDef.getTableDefinitions().values()) {
            Set<String> tableSuppFieldSet = m_dbSuppressedFieldMap.get(tableDef.getTableName());
            if (tableSuppFieldSet == null) {
                tableSuppFieldSet = new HashSet<String>();
                m_dbSuppressedFieldMap.put(tableDef.getTableName(), tableSuppFieldSet);
            }
            
            Map<String, MutableFloat> tableLinkFanoutMap =
                dbLinkFanoutMap.get(tableDef.getTableName());
            for (FieldDefinition fieldDef : tableDef.getFieldDefinitions()) {
                if (!fieldDef.isLinkField() || decidedLinkSet.contains(fieldDef)) {
                    continue;
                }

                float linkFanout = 0;
                if (tableLinkFanoutMap != null) {
                    MutableFloat avgLinks = tableLinkFanoutMap.get(fieldDef.getName());
                    if (avgLinks != null) {
                        linkFanout = avgLinks.m_value;
                    }
                }
                
                TableDefinition invTableDef = tableDef.getLinkExtentTableDef(fieldDef);
                FieldDefinition invFieldDef = invTableDef.getFieldDef(fieldDef.getLinkInverse());
                float invLinkFanout = 0;
                Map<String, MutableFloat> invTableLinkFanoutMap =
                   dbLinkFanoutMap.get(invTableDef.getTableName());
                if (invTableLinkFanoutMap != null) {
                    MutableFloat avgLinks = invTableLinkFanoutMap.get(invFieldDef.getName());
                    if (avgLinks != null) {
                        invLinkFanout = avgLinks.m_value;
                    }
                }
                
                // If this link's fanout is higher than or equal to that of its inverse,
                // we'll suppress this link, otherwise the inverse. When the fanouts are
                // the same or very close, it's arbitrary which one we pick, really.
                if (linkFanout >= invLinkFanout) {
                    // Suppress this link.
                    tableSuppFieldSet.add(fieldDef.getName());
                    m_logger.info("Will suppress {}.{} and dump {}.{}", 
                                   new Object[] {tableDef.getTableName(), fieldDef.getName(),
                                                 invTableDef.getTableName(), invFieldDef.getName()});
                } else {
                    // Suppress the inverse.
                    Set<String> invTableSuppFieldSet = m_dbSuppressedFieldMap.get(invTableDef.getTableName());
                    if (invTableSuppFieldSet == null) {
                        invTableSuppFieldSet = new HashSet<String>();
                        m_dbSuppressedFieldMap.put(invTableDef.getTableName(), invTableSuppFieldSet);
                    }
                    invTableSuppFieldSet.add(invFieldDef.getName());
                    m_logger.info("Will suppress {}.{} and dump {}.{}",
                                   new Object[]{invTableDef.getTableName(), invFieldDef.getName(),
                                                tableDef.getTableName(), fieldDef.getName()});
                }
                
                decidedLinkSet.add(fieldDef);
                decidedLinkSet.add(invFieldDef);
            }
        }
    }   // computeLinkFanouts

    // Compute link fanouts for the given table
    private void computeLinkFanouts(TableDefinition           tableDef,
                                    Map<String, MutableFloat> tableLinkFanoutMap) {
        m_logger.info("Computing link field fanouts for table: {}", tableDef.getTableName());
        StringBuilder buffer = new StringBuilder();
        for (FieldDefinition fieldDef : tableDef.getFieldDefinitions()) {
            if (fieldDef.isLinkField()) {
                if (buffer.length() > 0) {
                    buffer.append(",");
                }
                buffer.append(fieldDef.getName());
            }
        }
        if (buffer.length() == 0) {
            return;
        }
        
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("q", "*");
        queryParams.put("f", buffer.toString());
        queryParams.put("s", Integer.toString(LINK_FANOUT_SAMPLE_SIZE));
        if (m_session instanceof OLAPSession) {
            queryParams.put("shards", m_config.shard);
        }
        QueryResult qResult = m_session.objectQuery(tableDef.getTableName(), queryParams);
        Collection<DBObject> objectSet = qResult.getResultObjects();
        if (objectSet.size() == 0) {
            return;
        }
        
        Map<String, AtomicInteger> linkValueCounts = new HashMap<String, AtomicInteger>();
        int totalObjs = 0;
        for (DBObject dbObj : objectSet) {
            totalObjs++;
            for (String fieldName : dbObj.getLinkFieldNames(tableDef)) {
                Set<String> linkValues = dbObj.getFieldValues(fieldName);
                AtomicInteger totalLinkValues = linkValueCounts.get(fieldName);
                if (totalLinkValues == null) {
                    linkValueCounts.put(fieldName, new AtomicInteger(linkValues.size()));
                } else {
                    totalLinkValues.addAndGet(linkValues.size());
                }
            }
        }

        for (String fieldName : linkValueCounts.keySet()) {
            AtomicInteger totalLinkValues = linkValueCounts.get(fieldName);
            float linkFanout = totalLinkValues.get() / (float)totalObjs; // may round to 0
            m_logger.info("Average fanout for link {}: {}", fieldName, linkFanout);
            tableLinkFanoutMap.put(fieldName, new MutableFloat(linkFanout));
        }
    }   // computeLinkFanouts(tableDef)

    /**
     * Each DumpWorker grabs a table from CSVDumper and dumps the whole table.
     */
    final class DumpWorker extends Thread {
        // Number of objects we should request at one time:
        private static final int BATCH_SIZE = 1000;
        
        // Members: 
        private final int                   m_workerNo;
        private final ApplicationSession    m_session;
        private final CSVConfig             m_config;
        private       int m_totalObjects;
        private       int m_totalBytes;
        
        // Logging interface:
        private Logger m_logger = LoggerFactory.getLogger(getClass().getSimpleName());

        // Create a DumpWorker object that belongs to the given application. A client object
        // is created, which causes the database to be openned.
        DumpWorker(int workerNo) {
            m_workerNo = workerNo;
            m_config = CSVConfig.instance();
            m_logger.info("Worker {}: Opening session to application: {}",
                           new Object[]{m_workerNo, m_config.app});
            Client client = new Client(m_config.host, m_config.port, m_config.getTLSParams());
            m_session = client.openApplication(m_config.app);   // throws if unknown
            client.close();
        }   // constructor
        
        // run contains the main get-and-dump loop.
        @Override
        public void run() {
            String tableName = "<none>";
            try {
                for (TableDefinition tableDef = nextTableDef(); tableDef != null; tableDef = nextTableDef()) {
                    tableName = tableDef.getTableName();
                    m_totalObjects = 0;
                    m_totalBytes = 0;
                    dumpTable(tableDef);
                    incrementObjectCount(m_totalObjects);
                    incrementObjectByteCount(m_totalBytes);
                }
            } catch (Exception ex) {
                m_logger.error("Worker {}: Error dumping table '{}'", m_workerNo, tableName);
                m_logger.error("Worker {}", m_workerNo, ex);
            } finally {
                m_session.close();
                m_logger.info("Worker {}: Thread shutting down", m_workerNo);
            }
        }   // run

        // Dump the given table to a CSV file.
        private void dumpTable(TableDefinition tableDef) throws IOException {
            m_logger.info("Worker {}: Dumping table: {}", m_workerNo, tableDef.getTableName());
            File csvFile = new File(m_config.root + tableDef.getTableName() + ".csv");
            if (csvFile.exists()) {
                if (!csvFile.delete()) {
                    throw new IOException("Could not delete existing CSV file: " + csvFile.getAbsolutePath());
                }
            }
            
            BufferedWriter writer = null;
            try {
                writer = new BufferedWriter(new FileWriter(csvFile));
                dumpTable(tableDef, writer);
                writer.close();
                m_totalBytes += csvFile.length();
            } finally {
                Utils.close(writer);
            }
        }   // dumpTable

        // Get all objects for the given table and write to the given writer in CSV format.
        private void dumpTable(TableDefinition tableDef, BufferedWriter writer) throws IOException {
            StringBuilder fieldParam = new StringBuilder("*");
            StringBuilder csvHeader = new StringBuilder(m_config.id);
            
            // Build a list of fields in the order they'll be dumped.
            List<FieldDefinition> fieldDefList = new ArrayList<FieldDefinition>();
            for (FieldDefinition fieldDef : tableDef.getFieldDefinitions()) {
                if (!(fieldDef.isScalarField() || fieldDef.isLinkField()) ||
                    shouldSuppressField(fieldDef)) {
                    continue;
                }
                
                fieldDefList.add(fieldDef);
                csvHeader.append(',');
                csvHeader.append(fieldDef.getName());
                if (fieldDef.isLinkField()) {
                    fieldParam.append(',');
                    fieldParam.append(fieldDef.getName());
                }
            }
            
            writer.write(csvHeader.toString());
            writer.newLine();
            
            Map<String, String> queryParams = new HashMap<>();
            queryParams.put("q", "*");
            queryParams.put("f", fieldParam.toString());
            queryParams.put("s", Integer.toString(BATCH_SIZE));
            if (m_session instanceof OLAPSession) {
                queryParams.put("shards", m_config.shard);
            }
            String contToken = null;
            int objsDumped = 0;
            while (true) {
                if (m_session instanceof OLAPSession) {
                    if (objsDumped > 0) {
                        queryParams.put("k", Integer.toString(objsDumped));
                    }
                } else if (contToken != null) {
                    queryParams.put("g", contToken);
                }
                QueryResult qResult = m_session.objectQuery(tableDef.getTableName(), queryParams);
                Collection<DBObject> objectSet = qResult.getResultObjects();
                for (DBObject dbObj : objectSet) {
                    dumpObject(fieldDefList, dbObj, writer);
                    objsDumped++;
                }
                contToken = qResult.getContinuationToken();
                if (objectSet.size() == 0 || Utils.isEmpty(contToken)) {
                    break;
                }
            }
        }   // dumpTable

        // Write a CSV record to the given writer containing all fields of the given object.
        // The fields must appear in the oder of the given field list, with empty commas for
        // fields that have no values.
        private void dumpObject(List<FieldDefinition> fieldDefList, DBObject dbObj, BufferedWriter writer)
                throws IOException {
            StringBuilder buffer = new StringBuilder();
            buffer.append("\"");
            buffer.append(dbObj.getObjectID());
            buffer.append("\"");
            
            for (FieldDefinition fieldDef : fieldDefList) {
                buffer.append(",");
                if (fieldDef.isLinkField()) {
                    Set<String> linkValues = dbObj.getFieldValues(fieldDef.getName());
                    if (linkValues != null && linkValues.size() > 0) {
                        buffer.append("\"");
                        buffer.append(Utils.concatenate(linkValues, "~"));
                        buffer.append("\"");
                    }
                } else if (fieldDef.isCollection()) {
                    Collection<String> collValues = dbObj.getFieldValues(fieldDef.getName());
                    if (collValues != null && collValues.size() > 0) {
                        // Quote the value if it might contain commas.
                        if (fieldDef.getType() == FieldType.TEXT) {
                            buffer.append("\"");
                        }
                        buffer.append(Utils.concatenate(collValues, "~"));
                        if (fieldDef.getType() == FieldType.TEXT) {
                            buffer.append("\"");
                        }
                    }
                } else {
                    assert fieldDef.isScalarField();
                    String fieldValue = dbObj.getFieldValue(fieldDef.getName());
                    if (fieldValue != null && fieldValue.length() > 0) {
                        if (fieldDef.getType() == FieldType.TEXT) {
                            buffer.append("\"");
                        }
                        buffer.append(fieldValue);
                        if (fieldDef.getType() == FieldType.TEXT) {
                            buffer.append("\"");
                        }
                    }
                }
            }
            
            m_totalObjects++;
            writer.write(buffer.toString());
            writer.newLine();
        }   // dumpObject
        
    }   // class DumpWorker
    
}   // class CSVDumper
