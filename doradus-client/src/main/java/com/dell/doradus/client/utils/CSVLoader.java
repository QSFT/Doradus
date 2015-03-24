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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import com.dell.doradus.client.ApplicationSession;
import com.dell.doradus.client.Client;
import com.dell.doradus.client.OLAPSession;
import com.dell.doradus.client.SpiderSession;
import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.BatchResult;
import com.dell.doradus.common.CommonDefs;
import com.dell.doradus.common.ContentType;
import com.dell.doradus.common.DBObject;
import com.dell.doradus.common.DBObjectBatch;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.FieldType;
import com.dell.doradus.common.ObjectResult;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;

/**
 * Loads data from CSV files into a Doradus database. Each run loads tables for a single
 * application. The CSV files must be named "{table name}{cruft}.csv" where {table name}
 * is a defined table. {cruft} can be distinguishing characters such as "_1" or
 * "_{timestamp}". If table names overlap, CSVLoader figures it out. For example, if the
 * application has table names "Message" and "MessageParticipant". It knows that the
 * file "MessageParticipant_1.csv" belongs to the latter table and won't accidentally
 * load it into the Message table.  
 * <p>
 * The column names in each CSV file must match the corresponding field names. Values that
 * may contain commas should be quoted to prevent confusion with value-separating commas.
 * If a link or scalar collection has multiple values, they should be concatenated into a
 * single string using "~" as the separator.
 * <p>
 * The _ID field can be any named field in each file. By default, it is "Key". That is, if
 * a "Key" column is found, it is used as the _ID value, otherwise a unique ID is added
 * automatically. Each CSV file must start with a header row that defines the column names
 * present.
 * <p>
 * All parameters are defined in the file {@link CSVConfig} have defaults. Parameters
 * can be overridden via arguments to {@link #main(String[])}. For example, "-workers 5"
 * sets the number of parallel load threads to 5. Parameters can also be set via via
 * {@link CSVConfig#set(String, String)}. 
 * <p>
 * CSVLoader works for applications with all-defined fields; undefined fields are not
 * handled by this app.
 * <p>
 * The counterpart of CSVLoader is the {@link CSVDumper} utility, which can load the CSV
 * files created by this utility.
 */
public class CSVLoader {
    private static final int MAX_WORK_BACKLOG = 1000;

    // Members:
    private List<LoadWorker>        m_workerList = new ArrayList<LoadWorker>();
    private StringBuilder           m_token = new StringBuilder();    // reused -- not thread safe!
    private CSVConfig               m_config = CSVConfig.instance();
    private Client                  m_client;
    private ApplicationSession      m_session;
    private boolean                 m_bOLAPApp;
    private Thread                  m_appThread;
    
    // Logging interface:
    private Logger m_logger = LoggerFactory.getLogger("CSVLoader");
    
    // This holds the same of tables in longest-to-shorted table name. We need this funny
    // ordering to properly derive table names from CSV file names. For example, if a file
    // is called MessageParticipants.csv, we want to to match the table name
    // "MessageParticipants" and not "Message".
    private List<String> m_tableNameList;
    
    // Statistics updated locally:
    private int  m_totalFiles;
    private long m_totalLines;
    private long m_totalBytes;

    // A CSV file being loaded.
    private static class CSVFile {
        final String m_fileName;
        final TableDefinition m_tableDef;
        final List<String> m_fieldList;
        
        CSVFile(String fileName, TableDefinition tableDef, List<String> fieldList) {
            m_fileName = fileName;
            m_tableDef = tableDef;
            m_fieldList = fieldList;
        }   // constructor
    }   // static class CSVFile
    
    // This class stores the field values of a record and the class into which the record
    // should be stored.
    private static class Record {
        final CSVFile m_csvFile;
        final Map<String, String> m_fieldMap;
    
        // This "null" value tells the worker threads to stop.
        static final Record SENTINEL = new Record(null, null);

        // We use the passed field map; we don't copy it.
        Record(CSVFile csvFile, Map<String, String> fieldMap) {
            m_csvFile = csvFile;
            m_fieldMap = fieldMap;
        }   // constructor
    }   // Record
    
    // Query of records for workers. A sentinel tells each worker to shut down.
    private final BlockingQueue<Record> m_workerQueue =
        new LinkedBlockingQueue<Record>(MAX_WORK_BACKLOG);
    
    //----- Public methods
    
    /**
     * Parses the given parameters and calls {@link #run()} to perform the CSV load. Run
     * with "-?" to get a list of popular parameters. Also, see {@link CSVConfig} for a
     * list of all parameters used by CSVLoader.
     * 
     * @param args  Program arguments.
     */
    public static void main(String[] args) {
        try {
        	CSVLoader app = new CSVLoader();
        	app.parseArgs(args);
            app.run();
            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }  
    }   // main
 
    /**
     * Performs the CSV load as defined by the {@link CSVConfig} singleton object.
     * This method can be called instead of {@link #main(String[])} for programmatic
     * access. Parameters should be set first by direct access to the CSVConfig
     * object or by calling {@link CSVConfig#set(String, String)}.
     */
    public void run() {
        m_appThread = Thread.currentThread();
        validateRootFolder();
        openDatabase();
        deleteApplication();
        createApplication();
        startWorkers();
        
        long startTime = System.currentTimeMillis();
        loadFolder(new File(m_config.root));
        stopWorkers();
            
        // If loading an OLAP shard, merge the results.
        if (m_bOLAPApp) {
            m_logger.info("Merging shard: {}", m_config.shard);
            ((OLAPSession)m_session).mergeShard(m_config.shard, null);
        }
        
        // Display final statistics.
        long stopTime = System.currentTimeMillis();
        m_logger.info("Total files loaded:    {}", m_totalFiles);
        m_logger.info("Total lines scanned:   {}", m_totalLines);
        m_logger.info("Total bytes read:      {}", m_totalBytes);
        long totalMillis = stopTime - startTime;
        m_logger.info("Total time for load:   {}", Utils.formatElapsedTime(totalMillis));
        m_logger.info("Average lines/sec:     {}", ((m_totalLines * 1000) / totalMillis));
        m_logger.info("Average bytes/sec:     {}", ((m_totalBytes * 1000) / totalMillis));
        
        // Done with the client connection.
        m_client.close();
    }   // run

    //----- Private methods

    private static void usage() {
        display("Usage: CSVLoader <params>");
        display("where <params> are:");
        display("   -app <name>         Doradus application name. Default is: {}", CSVConfig.DEFAULT_APPNAME);
        display("   -batchsize <#>      Batch size. Default is: {}", CSVConfig.DEFAULT_BATCH_SIZE);
        display("   -compress [T|F]     Compress messages. Default is: {}", CSVConfig.DEFAULT_COMPRESS);
        display("   -host <host>        Doradus server host name. Default is: {}", CSVConfig.DEFAULT_HOST);
        display("   -id <name>          Column name of ID field. Default is: {}", CSVConfig.DEFAULT_ID_FIELD);
        display("   -increment_ts [T|F] True to increment timestamp fields 1 day per batch. Default is: {}", CSVConfig.DEFAULT_INCREMENT_TS);
        display("   -merge_all [T|F]    (OLAP only): true to merge after every batch. Default is: {}", CSVConfig.DEFAULT_MERGE_ALL);
        display("   -port <port>        Doradus server port. Default is: {}", CSVConfig.DEFAULT_PORT);
        display("   -root <folder>      Root folder of CSV files. Default is: {}", CSVConfig.DEFAULT_ROOT);
        display("   -schema <file>      Name of application schema file. Default is: {}", CSVConfig.DEFAULT_SCHEMA);
        display("   -skip_undef [T|F]   True to skip fields not declared in the schema. Default is: {}", CSVConfig.DEFAULT_SKIP_UNDEF);
        display("   -shard <name>       (OLAP only): Name of shard to load. Default is: {}", CSVConfig.DEFAULT_SHARD);
        display("   -workers <#>        # of worker threads. Default is: {}", CSVConfig.DEFAULT_WORKERS);
        display("To use TLS:");
        display("   -tls [T|F]               True to enable TLS/SSL. Default is: {}", CSVConfig.DEFAULT_TLS);
        display("   -keystore <file>         File name of keystore. Default is: {}", CSVConfig.DEFAULT_KEYSTORE);
        display("   -keystorepassword <pw>   Password of keystore file. Default is: {}", CSVConfig.DEFAULT_KEYSTORE_PW);
        display("   -truststore <file>       File name of truststore. Default is: {}", CSVConfig.DEFAULT_TRUSTSTORE);
        display("   -truststorepassword <pw> Password of truststore file. Default is: {}", CSVConfig.DEFAULT_TRUSTSTORE_PW);
        display("Deletes and recreates OLAP or Spider application defined by 'schema' file, then loads all CSV");
        display("files found in 'root' folder.");
        System.exit(0);
    }   // usage
    
    // Create the application from the configured schema file name.
    private void createApplication() {
        String schema = getSchema();
        ContentType contentType = null;
        if (m_config.schema.toLowerCase().endsWith(".json")) {
            contentType = ContentType.APPLICATION_JSON;
        } else if (m_config.schema.toLowerCase().endsWith(".xml")) {
            contentType = ContentType.TEXT_XML;
        } else {
            logErrorThrow("Unknown file type for schema: {}", m_config.schema);
        }
        
        try {
            m_logger.info("Creating application '{}' with schema: {}", m_config.app, m_config.schema);
            m_client.createApplication(schema, contentType);
        } catch (Exception e) {
            logErrorThrow("Error creating schema: {}", e);
        }
        
        try {
            m_session = m_client.openApplication(m_config.app);
        } catch (RuntimeException e) {
            logErrorThrow("Application '{}' not found after creation. Name doesn't match schema?", m_config.app); 
        }
        String ss = m_session.getAppDef().getStorageService();
        if (!Utils.isEmpty(ss) && ss.equals("OLAPService")) {
            m_bOLAPApp = true;
        }
        loadTables();
    }   // createApplication
    
    // Delete existing application if it exists.
    private void deleteApplication() {
        ApplicationDefinition appDef = m_client.getAppDef(m_config.app);
        if (appDef != null) {
            m_logger.info("Deleting existing application: {}", appDef.getAppName());
            m_client.deleteApplication(appDef.getAppName(), appDef.getKey());
        }
    }   // deleteApplication
    
    // See if we can figure out the table name from this file name. We match the first
    // table name that matches the starting characters of the CSV file name.
    private TableDefinition determineTableFromFileName(String fileName) {
        ApplicationDefinition appDef = m_session.getAppDef();
        for (String tableName : m_tableNameList) {
            if (fileName.regionMatches(true, 0, tableName, 0, tableName.length())) {
                return appDef.getTableDef(tableName);
            }
        }
        return null;
    }   // determineTableFromFileName

    // Write the given message to stdout only. Uses {}-style parameters
    private static void display(String format, Object... args) {
        System.out.println(MessageFormatter.arrayFormat(format, args).getMessage());
    }   // display
    
    // Extract the field names from the given CSV header line and return as a list.
    private List<String> getFieldListFromHeader(BufferedReader reader) {
        // Read the first line and watch out for BOM at the front of the header.
        String header = null;
        try {
            header = reader.readLine();
        } catch (IOException e) {
            // Leave header null
        }
        if (Utils.isEmpty(header)) {
            logErrorThrow("No header found in file");
        }
        if (header.charAt(0) == '\uFEFF') {
            header = header.substring(1);
        }
        
        // Split around commas, though this will include spaces.
        String[] tokens = header.split(",");
        List<String> result = new ArrayList<String>();
        for (String token : tokens) {
            // If this field matches the _ID field, add it with that name.
            String fieldName = token.trim();
            if (fieldName.equals(m_config.id)) {
                result.add(CommonDefs.ID_FIELD);
            } else {
                result.add(token.trim());
            }
        }
        return result;
    }   // getFieldListFromHeader

    // Load schema contents from m_config.schema file.
    private String getSchema() {
        if (Utils.isEmpty(m_config.schema)) {
            m_config.schema = m_config.app + ".xml";
        }
        File schemaFile = new File(m_config.schema);
        if (!schemaFile.exists()) {
            logErrorThrow("Schema file not found: {}", m_config.schema);
        }
        StringBuilder schemaBuffer = new StringBuilder();
        char[] charBuffer = new char[65536];
        try (FileReader reader = new FileReader(schemaFile)) {
            for (int bytesRead = reader.read(charBuffer); bytesRead > 0; bytesRead = reader.read(charBuffer)) {
                schemaBuffer.append(charBuffer, 0, bytesRead);
            }
        } catch (Exception e) {
            logErrorThrow("Cannot read schema file '{}': {}", m_config.schema, e);
        }
        return schemaBuffer.toString();
    }   // getSchema
    
    // Get all table names and sort from longest-to-shortest name. We need this funny
    // ordering to match CSV file names correctly.
    private void loadTables() {
        ApplicationDefinition appDef = m_session.getAppDef();
        m_tableNameList = new ArrayList<String>(appDef.getTableDefinitions().keySet());
        Collections.sort(m_tableNameList, new Comparator<String>() {
            @Override
            public int compare(String s1, String s2) {
                // We want longer table names to be first. So, if s1 == "a" and s2 == "aa",
                // "a".length() - "aa".length() == -1, which means "aa" appears first. We
                // don't care about the order of same-length names.
                return s2.length() - s1.length();
            }   // compare
        });
    }   // loadTables

    // Load the records in the given file into the given table.
    private void loadCSVFile(TableDefinition tableDef, File file) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), Utils.UTF8_CHARSET))
            ) {
            loadCSVFromReader(tableDef, file.getAbsolutePath(), file.length(), reader);
        }
    }   // loadCSVFile

    // Load all files in the given folder whose name matches a known table.
    private void loadFolder(File folder) {
        m_logger.info("Scanning for files in folder: {}", folder.getAbsolutePath());
        File[] files = folder.listFiles();
        if (files == null || files.length == 0) {
            m_logger.error("No files found in folder: {}", folder.getAbsolutePath());
            return;
        }
        
        for (File file : files) {
            String fileName = file.getName().toLowerCase();
            if (!fileName.endsWith(".csv") && !fileName.endsWith(".zip")) {
                continue;
            }
            TableDefinition tableDef = determineTableFromFileName(file.getName());
            if (tableDef == null) {
                m_logger.error("Ignoring file; unknown corresponding table: {}", file.getName());
                continue;
            }
            try {
                if (fileName.endsWith(".csv")) {
                    loadCSVFile(tableDef, file);
                } else  {
                    loadZIPFile(tableDef, file);
                }
            } catch (IOException ex) {
                m_logger.error("I/O error scanning file '{}': {}", file.getName(), ex);
            }
        }
    }   // loadFolder

    private void loadZIPFile(TableDefinition tableDef, File file) throws IOException {
        try (ZipFile zipFile = new ZipFile(file)) {
            Enumeration<? extends ZipEntry> zipEntries = zipFile.entries();
            while (zipEntries.hasMoreElements()) {
                ZipEntry zipEntry = zipEntries.nextElement();
                if (!zipEntry.getName().toLowerCase().endsWith(".csv")) {
                    m_logger.warn("Skipping zip file entry: " + zipEntry.getName());
                    continue;
                }
                try (InputStream zipEntryStream = zipFile.getInputStream(zipEntry)) {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(zipEntryStream, Utils.UTF8_CHARSET));
                    loadCSVFromReader(tableDef, zipEntry.getName(), zipEntry.getSize(), reader);
                }
            }
        }
    }   // loadZIPFile
    
    // Open Doradus database, setting m_client.
    private void openDatabase() {
        if (m_config.tls) {
            m_client = new Client(m_config.host, m_config.port, m_config.getTLSParams());
        } else {
            m_client = new Client(m_config.host, m_config.port);
        }
    }   // openDatabase

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
    }   // parseArgs
    
    // Load CSV records from the given reader, whose format is identified with the given
    // CSVFile. The caller must pass an opened stream and close it.
    private void loadCSVFromReader(TableDefinition  tableDef,
                                   String           csvName,
                                   long             byteLength,
                                   BufferedReader   reader) {
        m_logger.info("Loading CSV file: {}", csvName);
        
        // Determine the order in which fields in this file appear. Wrap this in a CSVFile.
        List<String> fieldList = getFieldListFromHeader(reader);
        CSVFile csvFile = new CSVFile(csvName, tableDef, fieldList);
        
        long startTime = System.currentTimeMillis();
        long recsLoaded = 0;
        int lineNo = 0;
            while (true) {
                Map<String, String> fieldMap = new HashMap<String, String>();
                if (!tokenizeCSVLine(csvFile, reader, fieldMap)) {
                    break;
                }
                lineNo++;
                m_totalLines++;
                
                try {
                    m_workerQueue.put(new Record(csvFile, fieldMap));
                } catch (InterruptedException e) {
                    logErrorThrow("Error posting to queue", e.toString());
                }
                if ((++recsLoaded % 10000) == 0) {
                    m_logger.info("...loaded {} records.", recsLoaded);
                }
            }
            
        // Always close the file and display stats even if an error occurred.
        m_totalFiles++;
        m_totalBytes += byteLength;
        long stopTime = System.currentTimeMillis();
        long totalMillis = stopTime - startTime;
        if (totalMillis == 0) {
            totalMillis = 1;    // You never know
        }
        m_logger.info("File '{}': time={} millis; lines={}",
                      new Object[]{csvFile.m_fileName, totalMillis, lineNo});
    }   // loadCSVFromReader

    // Launch the requested number of workers. Quit if any can't be started.
    private void startWorkers() {
        m_logger.info("Starting {} workers", m_config.workers);
        for (int workerNo = 1; workerNo <= m_config.workers; workerNo++) {
            LoadWorker loadWorker = new LoadWorker(workerNo);
            loadWorker.start();
            m_workerList.add(loadWorker);
        }
    }   // startWorkers

    // Add a sentinel to the queue for each worker and wait all to finish.
    private void stopWorkers() {
        Record sentinel = Record.SENTINEL;
        for (int inx = 0; inx < m_workerList.size(); inx++) {
            try {
                m_workerQueue.put(sentinel);
            } catch (InterruptedException e) {
                // ignore
            }
        }
        
        for (LoadWorker loadWorker : m_workerList) {
            try {
                loadWorker.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }   // stopWorkers

    // Tokenize the next comma-separated record from the given reader. Map fields by name
    // into the given field map.
    private boolean tokenizeCSVLine(CSVFile             csvFile, 
                                    BufferedReader      reader,
                                    Map<String, String> fieldMap) {
        fieldMap.clear();
        m_token.setLength(0);
        
        // First build a list of tokens found in the order they are found.
        List<String> tokenList = new ArrayList<String>();
        boolean bInQuote = false;
        int aChar = 0;
        try {
            while (true) {
                aChar = reader.read();
                if (aChar < 0) {
                    break;
                }
                
                if (!bInQuote && aChar == ',') {
                    tokenList.add(m_token.toString());
                    m_token.setLength(0);
                } else if (!bInQuote && aChar == '\r') {
                    tokenList.add(m_token.toString());
                    m_token.setLength(0);
                    reader.mark(1);
                    aChar = reader.read();
                    if (aChar == -1) {
                        break; 
                    }
                    if (aChar != '\n') {
                        reader.reset(); // put back non-LF
                    }
                    break;
                } else if (!bInQuote && aChar == '\n') {
                    tokenList.add(m_token.toString());
                    m_token.setLength(0);
                    break;
                } else if (aChar == '"') {
                    bInQuote = !bInQuote;
                } else {
                    m_token.append((char)aChar);
                }
            }
        } catch (IOException e) {
            logErrorThrow("I/O error reading file", e.toString());
        }

        // If we hit EOF without a final EOL, we could have a token in the buffer.
        if (m_token.length() > 0) {
            tokenList.add(m_token.toString());
            m_token.setLength(0);
        }
        
        if (tokenList.size() > 0) {
            for (int index = 0; index < tokenList.size(); index++) {
                String token = tokenList.get(index).trim();
                if (token.length() > 0) {
                    String fieldName = csvFile.m_fieldList.get(index);
                    fieldMap.put(fieldName, token);
                }
            }
            return true;
        }
        
        // No tokens found; return false if EOF.
        return aChar != -1;
    }   // tokenizeCSVLine

    // Format the given message using {}-style parameters, log as an error, and throw as
    // a RuntimeException.
    private void logErrorThrow(String format, Object... args) {
        String msg = MessageFormatter.arrayFormat(format, args).getMessage();
        m_logger.error(msg);
        throw new RuntimeException(msg);
    }   // logErrorThrow
    
    // Root directory must exist and be a folder
    private void validateRootFolder() {
        File rootDir = new File(m_config.root);
        if (!rootDir.exists()) {
            logErrorThrow("Root directory does not exist: {}", m_config.root);
        } else if (!rootDir.isDirectory()) {
            logErrorThrow("Root directory must be a folder: {}", m_config.root);
        } else if (!m_config.root.endsWith(File.separator)) {
            m_config.root = m_config.root + File.separator;
        }
    }   // validateRootFolder

    /**
     * LoadWorker loads records on behalf of CSVLoader. It runs in its own thread, receiving
     * records via a queue, loading them into the appropriate table in batches. The worker
     * shuts down when it receives a special sentinel record.
     */
    final class LoadWorker extends Thread {
        private static final long MILLIS_PER_DAY = 1000 * 3600 * 24;
        
        // Date formats we try to recognize (add to these as needed):
        private final SimpleDateFormat[] DATE_FORMATS = new SimpleDateFormat[] {
            new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a"),   // e.g., 7/17/2010 9:32:01 PM
            new SimpleDateFormat("MM/dd/yyyy HH:mm:ss"),     // e.g., 7/17/2010 21:32:01
        };
        
        // Members:
        private final int               m_workerNo;
        private final DBObjectBatch     m_batch;
        private String                  m_batchTableName = "";
        private int                     m_batchNo;
        private ApplicationSession      m_session;
        
        private LoadWorker(int workerNo) {
            m_workerNo = workerNo;
            
            m_logger.debug("Worker {}: Opening connection to {}:{}",
                           new Object[]{m_workerNo, m_config.host, m_config.port});
            Client client = new Client(m_config.host, m_config.port, m_config.getTLSParams());
            client.setCompression(m_config.compress);
            try {
                m_session = client.openApplication(m_config.app);
            } catch (Exception e) {
                logErrorThrow("Cannot access application {}: {}", m_config.app, e);
            }
            m_batch = new DBObjectBatch();
            client.close();
        }   // constructor

        // Run is entered when our supervisor starts us.
        @Override
        public void run() {
            while (true) {
                try {
                    Record record = m_workerQueue.take();
                    if (record == Record.SENTINEL) {
                        flushBatch();
                        break;
                    }
                    loadRecord(record);
                } catch (InterruptedException e) {
                    // ignore
                } catch (Exception ex) {
                    m_logger.error("Worker '" + m_workerNo + "'", ex);
                    m_appThread.interrupt();
                    break;
                }
            }

            m_session.close();
            m_logger.debug("Worker {}: Thread shutting down", m_workerNo);
        }   // run

        // Load the record into the database.
        private void loadRecord(Record record) throws IOException {
            if (!m_bOLAPApp && m_batch.getObjectCount() > 0 &&
                !record.m_csvFile.m_tableDef.getTableName().equals(m_batchTableName)) {
                flushBatch();
                m_batchNo = 0;
            }
            
            m_batch.addObject(createObject(record));
            m_batchTableName = record.m_csvFile.m_tableDef.getTableName();
            if (m_batch.getObjectCount() >= m_config.batchsize) {
                flushBatch();
            }
        }   // loadRecord

        private DBObject createObject(Record record) {
            TableDefinition tableDef = record.m_csvFile.m_tableDef;
            DBObject dbObj = new DBObject();
            dbObj.setTableName(tableDef.getTableName());
            
            for (Map.Entry<String, String> mapEntry : record.m_fieldMap.entrySet()) {
                String fieldName = mapEntry.getKey();
                String fieldValue = mapEntry.getValue();
                FieldDefinition fieldDef = tableDef.getFieldDef(fieldName);
                if (fieldDef == null && !fieldName.equals("_ID") && m_config.skip_undef) {
                    continue;
                }
                boolean isCollection = fieldDef == null ? false : fieldDef.isCollection();
                boolean isLink = fieldDef == null ? false : fieldDef.isLinkField();
                FieldType fieldType = fieldDef == null ? FieldType.TEXT : fieldDef.getType();
                
                // Link and MV scalar fields are separate by '~'.
                if (isLink) {
                    dbObj.addFieldValues(fieldName, Arrays.asList(fieldValue.split("~")));
                } else if (isCollection) {
                    String[] values = fieldValue.split("~");
                    if (fieldType == FieldType.TIMESTAMP) {
                        for (int i = 0; i < values.length; i++) {
                            values[i] = convertTimestamp(values[i]);
                        }
                    }
                    dbObj.addFieldValues(fieldName, Arrays.asList(values));
                } else if (fieldType == FieldType.TIMESTAMP) {
                    dbObj.addFieldValue(fieldName, convertTimestamp(fieldValue));
                } else {
                    dbObj.addFieldValue(fieldName, fieldValue);
                }
            }
            
            return dbObj;
        }   // createObject

        // Attempt to recognize the given timestamp value format and convert to the Doradus
        // required form: YYYY-MM-DD hh:mm:ss.fff. If we don't recognize it, we return as-is.
        private String convertTimestamp(String value) {
            for (SimpleDateFormat sdf : DATE_FORMATS) {
                try {
                    Date date = sdf.parse(value);
                    if (m_config.increment_ts) {
                        Date adjustedDate = new Date(date.getTime() + (m_batchNo * MILLIS_PER_DAY));
                        return Utils.formatDateUTC(adjustedDate);
                    } else {
                        return Utils.formatDateUTC(date);
                    }
                } catch (ParseException e) {
                    // Skip this format.
                }
            }
            
            // Here, no formats were recognized, so let Doradus accept or reject.
            return value;
        }   // convertTimestamp

        // Load the current batch of records that we've queued-up.
        private void flushBatch() {
            if (m_batch.getObjectCount() == 0) {
                return;
            }

            try {
                BatchResult result = null;
                if (m_bOLAPApp) {
                    result = ((OLAPSession)m_session).addBatch(m_config.shard, m_batch);
                    if (m_config.merge_all) {
                        ((OLAPSession)m_session).mergeShard(m_config.shard, null);
                    }
                } else {
                    result = ((SpiderSession)m_session).addBatch(m_batchTableName, m_batch);
                }
                if (result.isFailed()) {
                    m_logger.error("Worker {}: Batch update failed: {}",
                                  new Object[]{m_workerNo, result.getErrorMessage()});
                    for (String objectID : result.getFailedObjectIDs()) {
                        ObjectResult objResult = result.getObjectResult(objectID);
                        m_logger.warn("Worker {}: error for object ID '{}': {}",
                                      new Object[]{m_workerNo, objectID, objResult.getErrorMessage()});
                        Map<String, String> errorDetails = objResult.getErrorDetails();
                        for (String fieldName : errorDetails.keySet()) {
                            m_logger.warn("Worker {}: Detail: {}={}",
                                          new Object[]{m_workerNo, fieldName, errorDetails.get(fieldName)});
                        }
                    }
                }
            } catch (Exception e) {
                logErrorThrow("Error adding batch: {}", e);
            }
            
            m_batch.clear();
            m_batchNo++;
        }   // flushBatch
        
    }   // class LoadWorker
    
}   // class CSVLoader
