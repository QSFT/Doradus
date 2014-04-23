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

package com.dell.doradus.tasks;

//import java.text.ParseException;
//import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.SimpleTimeZone;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.CommonDefs;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.FieldType;
import com.dell.doradus.common.RetentionAge;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.search.SearchResult;
import com.dell.doradus.service.olap.OLAPService;
import com.dell.doradus.service.schema.SchemaService;
import com.dell.doradus.service.spider.ObjectUpdater;
import com.dell.doradus.service.spider.SpiderService;
import com.dell.doradus.service.spider.SpiderTransaction;

/**
 * DataAger is a background task implementation that finds and deletes expired data
 * in a Doradus database (Spider service storage model) for a given application.
 * Usually one thread is used to service all tables in the specified application.
 * One can distribute the task between nodes if she defines several tasks with
 * different tables sets.
 */
public class DataAger extends DoradusTask {
    // Logging interface:
    private Logger m_logger = LoggerFactory.getLogger(getClass().getSimpleName());
    
    // All dates operate in the UTC time zone:
    public static final TimeZone UTC_ZONE = new SimpleTimeZone(0, "UTC");

    // Represents a table to be aged and info about it.
    private static class AgingTable {
        // Table to age data for and the aging parameters specified:
        private final TableDefinition   m_tableDef;
        private final RetentionAge      m_retAge;
        private final FieldDefinition   m_agingFieldDef;

        // Last time we checked aging for this table:
        // TODO: check whether we need it now?
//        private GregorianCalendar m_lastCheckDate;

        // Constructor
        AgingTable(TableDefinition tableDef, RetentionAge retAge, FieldDefinition agingFieldDef) {
            // Prerequisites:
            assert tableDef != null;
            assert retAge != null;
            assert agingFieldDef != null && agingFieldDef.getType() == FieldType.TIMESTAMP;

            m_tableDef = tableDef;
            m_retAge = retAge;
            m_agingFieldDef = agingFieldDef;

            // Set the initial last-checked date to 1970-1-1
            // TODO: check whether we need it now?
//            m_lastCheckDate = new GregorianCalendar(UTC_ZONE);
//            m_lastCheckDate.set(1970, 0, 1);    // zero-relative month!
        }   // constructor

        /**
         * Returns table name
         * @return Table name
         */
        String getName() {
        	return m_tableDef.getTableName();
        }

    }   // static class AgingTable
    
    /////////////////// Implementation main method ///////////////////////

    /**
     * The task implementation. Works differently for OLAP and Spider services.
     */
	protected void runTask() {
		if (OLAP_SERVICE_NAME.equals(getServiceName())) {
			checkShards();
		} else if (SPIDER_SERVICE_NAME.equals(getServiceName())) {
			checkTables();
		} else {
			m_logger.error("Unknown service for data-aging task: {}", getServiceName());
		}
	}	// runTask
	
    /////////////////// Private methods ///////////////////////

	/**
	 * Implementation of the task for OLAP storage service.
	 */
	private void checkShards() {
		Date now = new Date();
		String appName = getAppName();
		OLAPService olap = OLAPService.instance();
		
		m_logger.debug("Check shards for application: " + appName);
		List<String> shards = olap.listShards(appName);
		for (String shardName : shards) {
        	if (isInterrupted()) {
        		return;
        	}
			m_logger.debug("Shard: " + shardName);
			Date expirationDate = olap.getExpirationDate(appName, shardName);
			if (expirationDate != null && expirationDate.before(now)) {
				olap.deleteShard(appName, shardName);
				m_logger.info("Expired shard " + appName + "." + shardName + " deleted");
			}
		}
	}	// checkShards

	/**
	 * Implementation of the task for Spider storage service.
	 */
	private void checkTables() {
		List<AgingTable> tableList = loadSchemaTables();
		if (tableList.isEmpty()) {
			// No tables found
			return;
		}
		ApplicationDefinition appDef = SchemaService.instance().getApplication(getAppName());
		Collection<String> tableNames = "*".equals(m_tableName) ? appDef.getTableDefinitions().keySet() : Arrays.asList(m_tableName.split(","));
        
		// Cycle through all the tables looking for any that need checking
        for (AgingTable agingTable : tableList) {
        	if (tableNames.contains(agingTable.getName())) {
                m_logger.debug("Checking data-aging for table '{}'", agingTable.getName());
        		checkTable(agingTable, new GregorianCalendar(UTC_ZONE));
        	}
        	if (isInterrupted()) {
        		m_logger.info("DataAger interrupted");
        		break;
        	}
        }
	}	// checkTables
	
    // Scan the given table for expired objects relative to the given date.
    private void checkTable(AgingTable table, final GregorianCalendar checkDate) {
        // Determine the timestamp on or before which objects should be expired. This
        // method returns a UTC date/time.
        GregorianCalendar expireDate = table.m_retAge.getExpiredDate(checkDate);
        int objsExpired = 0;

       // Define the query '{aging field} <= "{expire date}"', fetching the _ID and
        // aging field, up to a batch full at a time.
        TableDefinition tableDef = table.m_tableDef;
        String text = "q=" + table.m_agingFieldDef.getName() + " <= " +
                      "\"" + Utils.formatDate(expireDate) + "\"";
        
        SpiderTransaction transaction = new SpiderTransaction();
        ObjectUpdater updater = new ObjectUpdater(transaction, tableDef);
        for(SearchResult result : SpiderService.instance().objectQueryURI(tableDef, text).results)
        {
        	String id = result.id();
            // Attempt to delete this object.
        	m_logger.debug("expiring '{}' in table '{}' application '{}'",
        			new Object[] { id, tableDef.getTableName(), getAppName() });
        	
        	boolean bDeleted = updater.deleteObject(id).isUpdated();
            if (bDeleted) {
                objsExpired++;
            }
            if (isInterrupted()) {
            	return;
            }
        }
        // Found and finished all updates. Set this table's last aging check timestamp
        // to the value we just used.
        // TODO: check whether we need it now?
//        table.m_lastCheckDate = expireDate;
//        String colValue = Utils.formatDate(table.m_lastCheckDate);
//        transaction.setApplicationMetadata(getAppName(), lastAgingCheckColName(tableDef), colValue);

        transaction.commit();
        
        m_logger.info("Total objects expired in table '{}' application '{}': {}",
                new Object[] { tableDef.getTableName(), getAppName(), objsExpired });

    }   // checkTable

    // Load or reload the current schema and determine which tables require data aging.
    private List<AgingTable> loadSchemaTables() {
        // TODO: check whether we need it now?
//    	SimpleDateFormat formatter =
//    	          new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");        // e.g., 2010-07-15 21:32:01 (for now)
        // Rebuild the list of tables each time.
    	List<AgingTable> tableList = new ArrayList<AgingTable>();

        // Get the current schema and watch for the application to have been deleted.
		ApplicationDefinition appDef = SchemaService.instance().getApplication(getAppName());
        if (appDef == null) {
            // Our designated application has apparently been deleted, so its time to shutdown.
			m_logger.error("Application {} doesn\'t exist anymore", getAppName());
            return tableList;
        }

        // Find tables that require aging.
        m_logger.debug("Finding tables to age for application '{}'", getAppName());
        for (TableDefinition tableDef : appDef.getTableDefinitions().values()) {
            // Look for retention-age option for this table.
            String retAgeOpt = tableDef.getOption(CommonDefs.OPT_RETENTION_AGE);
            if (retAgeOpt == null) {
                // No retention-age for this table.
                continue;
            }

            // See if the aging value is > 0 (0 means feature disabled)
            RetentionAge retAge = null;
            try {
                // This throws if the option is malformed.
                retAge = new RetentionAge(retAgeOpt);
            } catch (IllegalArgumentException ex) {
                m_logger.warn("Invalid retention-age value for table '{}': {}",
                              tableDef.getTableName(), retAgeOpt);
                continue;
            }
            if (retAge.getValue() == 0) {
                // Retention-age disabled for this table.
                continue;
            }

            // Ensure the aging-field was defined.
            String agingField = tableDef.getOption(CommonDefs.OPT_AGING_FIELD);
            if (agingField == null) {
                // Hmm. Should have been caught by the parsing code. Oh well.
                m_logger.warn("Table {} has rentention-age='{}' but no aging field",
                              tableDef.getTableName(), retAge);
                continue;
            }
            FieldDefinition fieldDef = tableDef.getFieldDef(agingField);
            if (fieldDef == null || fieldDef.getType() != FieldType.TIMESTAMP) {
                // This is a schema parser boo-boo as well.
                m_logger.warn("Aging field '{}' for table '{}' is not a timestamp; ignored",
                              agingField, tableDef.getTableName());
                continue;
            }

            // Here, aging parameter is legit. Add an AgingTable to our list.
            m_logger.debug("Aging data in table '{}' with age: {}",
                           tableDef.getTableName(), retAge);
            AgingTable agingTable = new AgingTable(tableDef, retAge, fieldDef);

            // See if a "last aging check" timestamp was persisted for this table.
            // TODO: check whether we need it now?
//            String lastAgingCheck = Utils.toString(
//            		DBService.instance().getColumn(CassandraDBConn.COLUMN_FAMILY_APPS, getAppName(), lastAgingCheckColName(tableDef)));
//            if (lastAgingCheck != null) {
//                // Found LastAgingCheck in the DB, so start with that.
//                Date lastAgingDate;
//				try {
//					lastAgingDate = formatter.parse(lastAgingCheck);
//				} catch (ParseException e) {
//					continue;
//				}
//                agingTable.m_lastCheckDate.setTime(lastAgingDate);
//                m_logger.debug("Table '{}' in application '{}' was last checked for data aging on: {}",
//                               new Object[]{tableDef.getTableName(), getAppName(), lastAgingCheck});
//            }
            
            tableList.add(agingTable);
        }
        
        return tableList;
    }   // loadSchemaTables

    // TODO: check whether we need it now?
//    private static String lastAgingCheckColName(TableDefinition tableDef) {
//        return tableDef.getTableName() + "/" + Defs.LAST_AGING_CHECK;
//    }   // lastAgingCheckColName

}
