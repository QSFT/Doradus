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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.CommonDefs;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.common.ScheduleDefinition.SchedType;
import com.dell.doradus.core.Defs;
import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.fieldanalyzer.FieldAnalyzer;
import com.dell.doradus.fieldanalyzer.NullAnalyzer;
import com.dell.doradus.fieldanalyzer.TextAnalyzer;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.DRow;
import com.dell.doradus.service.schema.SchemaService;
import com.dell.doradus.service.spider.SpiderService;
import com.dell.doradus.service.taskmanager.TaskManagerService;
import com.dell.doradus.utilities.BigSet;
import com.google.common.io.Files;

/**
 * A background task implementation that tests objects against their belonging to
 * correct shards. Objects may belong to wrong shard if the sharding options
 * have changed for some table(s). It is recommended that the task is launched
 * when no active changes are made in the tables under checking, actually a general
 * recommendation is schedule the task for a very seldom (or never happening) schedule,
 * and to launch the task manually when necessary.
 */
public class ShardingChecker extends DoradusTask {
	/**
	 * Provides a set of methods to make necessary changes in database
	 * during sharding checking process.
	 */
	private static class CheckerTransaction {
		// Database service
		private DBService m_dbService = DBService.instance();
		// incorporated transaction object
		private DBTransaction m_dbTransaction = null;
		
		/**
		 * Checks whether we can use the transaction object.
		 * Creates the object if it was not created before.
		 */
		private void checkExists() {
			if (m_dbTransaction == null) {
				m_dbTransaction = m_dbService.startTransaction();
			}
		}
		
		/**
		 * Commits the transaction and removes it to start a new transaction
		 */
		public void commit() {
			if (m_dbTransaction != null) {
				DBService.instance().commit(m_dbTransaction);
				m_dbTransaction = null;
			}
		}
		
		/**
		 * Checks whether a current transaction has more than a maximum of mutations.
		 * If so, commits the transaction and starts a new one.
		 */
		public void checkCommit() {
			if (m_dbTransaction != null &&
				m_dbTransaction.getUpdateCount() >= ServerConfig.getInstance().batch_mutation_threshold) {
				commit();
			}
		}
		
		/**
		 * Adding a flag "sharded table was successfully tested" to database.
		 *  
		 * @param tabDef	Table that was tested
		 */
		public void addCheckedRow(TableDefinition tabDef) {
			addCheckedRow(tabDef, TABLE_CHECKED_COL);
		}
		
		/**
		 * Adding a flag "sharded link was successfully tested" to database.
		 * 
		 * @param tabDef	Table that contains a link
		 * @param link		Link that was tested
		 */
		public void addCheckedRow(TableDefinition tabDef, String link) {
			checkExists();
			m_dbTransaction.addColumn(SpiderService.termsStoreName(tabDef),
					                  Defs.TABLE_CHECKS_ROW_KEY,
					                  link,
					                  EMPTY_BYTES);
		}
		
		/**
		 * Deletes a flag "sharded link was successfully tested" from database.
		 * Necessary to restart database checking.
		 * 
		 * @param tabDef	Table that is necessary to re-test.
		 */
		public void deleteCheckedRow(TableDefinition tabDef) {
			checkExists();
			m_dbTransaction.deleteRow(SpiderService.termsStoreName(tabDef),
					                  Defs.TABLE_CHECKS_ROW_KEY);
		}
		
		/**
		 * Adds a column to the "all objects" row in terms table.
		 * 
		 * @param tabDef	Table that contains an object
		 * @param shardNo	Shard number
		 * @param objectId	Object ID
		 */
		public void addAllObjectsColumn(TableDefinition tabDef, int shardNo, String objectId) {
			checkExists();
			String shardPrefix = shardNo == 0 ? "" : shardNo + "/";
			m_dbTransaction.addColumn(SpiderService.termsStoreName(tabDef),
					                  shardPrefix + Defs.ALL_OBJECTS_ROW_KEY,
					                  objectId,
					                  EMPTY_BYTES);
		}
		
		/**
		 * Deletes a column from the "all objects" row in terms table.
		 * 
		 * @param tabDef	Table that contains an object
		 * @param shardNo	Shard number
		 * @param objectId	Object ID
		 */
		public void deleteAllObjectsColumn(TableDefinition tabDef, int shardNo, String objectId) {
			checkExists();
			String shardPrefix = shardNo == 0 ? "" : shardNo + "/";
			m_dbTransaction.deleteColumn(SpiderService.termsStoreName(tabDef),
					                     shardPrefix + Defs.ALL_OBJECTS_ROW_KEY,
	                                     objectId);
		}
		
		/**
		 * Adds a term to a terms table.
		 * 
		 * @param tabDef	Table to process
		 * @param shardNo	Shard number
		 * @param objectId	Object ID
		 * @param field		Scalar field name
		 * @param term		Term to add
		 */
		public void addTerm(TableDefinition tabDef,
				            int shardNo,
				            String objectId,
				            String field,
				            String term) {
			checkExists();
			String shardPrefix = shardNo == 0 ? "" : shardNo + "/";
			String store = SpiderService.termsStoreName(tabDef);
			m_dbTransaction.addColumn(store, 
					                  shardPrefix + field + "/" + term,
					                  objectId, EMPTY_BYTES);
			// Register the newly added term
			m_dbTransaction.addColumn(store, 
					                  shardPrefix + Defs.TERMS_REGISTRY_ROW_PREFIX + "/" + field, 
					                  term, EMPTY_BYTES);
		}
		
		/**
		 * Deletes the term from a terms table.
		 * 
		 * @param tabDef	Table to process
		 * @param shardNo	Shard number
		 * @param objectId	Object ID
		 * @param field		Scalar field name
		 * @param term		Term to delete
		 */
		public void deleteTerm(TableDefinition tabDef,
					           int shardNo,
					           String objectId,
					           String field,
					           String term) {
			checkExists();
			String shardPrefix = shardNo == 0 ? "" : shardNo + "/";
			m_dbTransaction.deleteColumn(SpiderService.termsStoreName(tabDef),
					                     shardPrefix + field + "/" + term,
					                     objectId);
			// We don't unregister the term since it may exist in other objects
		}
		
		/**
		 * Adds a new shard number into the _shards row of the terms table.
		 * 
		 * @param tabDef	Table to process.
		 * @param shardNo	Shard number
		 */
		public void addShard(TableDefinition tabDef, int shardNo) {
			checkExists();
			m_dbTransaction.addColumn(SpiderService.termsStoreName(tabDef),
					                  Defs.SHARDS_ROW_KEY,
					                  String.valueOf(shardNo),
					                  Utils.toBytes(String.valueOf(tabDef.computeShardStart(shardNo).getTime())));
		}
		
		/**
		 * Removes a link from an object table.
		 * 
		 * @param tabDef			Table to process
		 * @param objectId			Object ID (from)
		 * @param linkedObjectId	Object ID (to)
		 * @param linkName			Link field name
		 */
		public void deleteDirectLink(TableDefinition tabDef,
				                     String objectId,
				                     String linkedObjectId,
				                     String linkName) {
			checkExists();
			m_dbTransaction.deleteColumn(SpiderService.objectsStoreName(tabDef),
					                     objectId,
					                     "~" + linkName + "/" + linkedObjectId);
		}
		
		/**
		 * Adds a sharded link to a terms table
		 * 
		 * @param tabDef			Table to process
		 * @param shardNo			Shard number
		 * @param objectId			Object ID (from)
		 * @param linkedObjectId	Object ID (to)
		 * @param linkName			Link field name
		 */
		public void addShardedLink(TableDefinition tabDef,
				                   String shardNo,
				                   String objectId,
				                   String linkedObjectId,
				                   String linkName) {
			checkExists();
			m_dbTransaction.addColumn(SpiderService.termsStoreName(tabDef),
					                  shardNo + "/~" + linkName + "/" + objectId,
					                  linkedObjectId,
					                  EMPTY_BYTES);
		}
	}

    // Logging interface:
    private Logger m_logger = LoggerFactory.getLogger(getClass().getSimpleName());
    
    // Obsolete: the table should be marked as "checked" for the case when
    // the database is switched from "non-sharded" to "sharded" mode.
	private static final String TABLE_CHECKED_COL = "_table";
	
	// "No value" column value
    private static final byte[] EMPTY_BYTES = new byte[0];
	
    // Obsolete: whether to check tables that were once checked.
	private boolean m_bAlwaysCheck = true;
	
    // Setting a balance between memory / time optimizations
	private final int m_maxListCapacity = 1000;
	
	// Size of the set before a shard will be stored on the disk.
	private final static int SET_CARDINALITY = 1 << 20;	// ~1 mln records per file shard
	
	private File m_tempDir;
	private String m_pathToKeys;
	private CheckerTransaction m_checkerTransaction = new CheckerTransaction();

    long m_startTime;
    
	/**
	 * Main function for start as a background task. It takes the program parameters from
	 * context that the scheduler delivers to it.
	 */
	@Override
	public void runTask() {
		// Get application and table(s) parameters from the job DataMap.
		String appName = getAppName();
		String tabNames = getTableName();
		m_tempDir = Files.createTempDir();
		m_pathToKeys = m_tempDir.getAbsolutePath() + File.separator;
		
		m_startTime = System.currentTimeMillis();
		
        ApplicationDefinition appDef = SchemaService.instance().getApplication(appName);
		if (appDef == null) {
			// Apparently the application schema was deleted
			m_logger.error("Application {} doesn\'t exist anymore", appName);
			return;
		}
        // tabNames == "*" means all tables.
		Collection<String> tables = 
				"*".equals(tabNames) ?
					appDef.getTableDefinitions().keySet() :
					Arrays.asList(tabNames.split(","));
		
		// Perform sharding-check for all the tables sequentially
		for (String tabName : tables) {
			TableDefinition tabDef = appDef.getTableDef(tabName.trim());
			if (tabDef != null) {
				if (m_bAlwaysCheck) {
					clearCheckedRow(tabDef);
				}
				checkTableSharding(tabDef);
				if (isInterrupted()) break;
			}
		}

		if (!Utils.deleteDirectory(m_tempDir)) {
			m_logger.info("Could not delete temporary directory " + m_tempDir.getAbsolutePath());
		};

		long timeSpan = (System.currentTimeMillis() - m_startTime) / 1000;
		int mins = (int)(timeSpan / 60);
		int secs = (int)(timeSpan % 60);
		m_logger.info("stopped; working time is "
				+ (mins > 0 ? mins + " mins " : "")
				+ secs + " secs.");
	}	// runTask
	
	/**
	 * Obsolete: initial testing for the old (non-sharded) databases. Checks all the
	 * applications of the database. 
	 */
	public static void checkSharding() {
		TaskManagerService taskService = TaskManagerService.instance();
		List<ApplicationDefinition> appDefs = SchemaService.instance().getAllApplications();
		for (ApplicationDefinition appDef : appDefs) {
			if ("SpiderService".equals(appDef.getStorageService())) {
				ShardingChecker task = (ShardingChecker)DoradusTask.createTask(
						appDef.getAppName(), "*/" + SchedType.SHARDING_CHECK.getName());
				if (task != null) {
					task.m_bAlwaysCheck = false;	// Check only tables that were not checked before.
					taskService.startTask(task);
				}
			}
		}
	}	// checkSharding
	
	//---------------------- Private functions -------------------------
	
	/**
	 * Makes the application to check the table again even if it was already checked earlier.
	 * 
	 * @param tabDef
	 * @throws IOException
	 */
	private void clearCheckedRow(TableDefinition tabDef) {
		m_checkerTransaction.deleteCheckedRow(tabDef);
		m_checkerTransaction.commit();
	}	// clearCheckedRow
	
	/**
	 * Performs sharding checking for a single table. It includes checking of whether the
	 * objects are placed to their actual shards in the terms table (in case of sharded
	 * table), and checking whether links are properly sharded (in case of sharded links).
	 * 
	 * @param tabDef Table to check
	 */
	private void checkTableSharding(final TableDefinition tabDef) {
		// Check objects in a (sharded) table.
		if (tabDef.isSharded()) {
			checkObjectSharding(tabDef);
		}
		if (isInterrupted()) return;
		// Check sharded links for every sharded link field.
		if (hasShardedLinks(tabDef)) {
			checkLinkSharding(tabDef);
		}
	}	// checkTableSharding
	
	/**
	 * Checks whether the table has at least one sharded link.
	 * 
	 * @param tabDef
	 * @return
	 */
	private boolean hasShardedLinks(TableDefinition tabDef) {
		for (FieldDefinition field : tabDef.getFieldDefinitions()) {
			if (field.isLinkField() && field.isSharded()) {
				return true;
			}
		}
		return false;
	}	// hasShardedLinks
	
	/**
	 * Checks objects that are in a common shard, whether they should be moved to
	 * another shard. The process takes every object sequentially, inspects the
	 * corresponding object and moves the terms in case that the object should be
	 * placed to another (numbered) shard.
	 * 
	 * @param tabDef Table to check
	 * @throws IOException
	 */
	private void checkObjectSharding(final TableDefinition tabDef) {
		assert tabDef.isSharded();
		
		DBService dbService = DBService.instance();
		
		String termsTable = SpiderService.termsStoreName(tabDef);
		
		if (dbService.getColumn(
				termsTable, Defs.TABLE_CHECKS_ROW_KEY, TABLE_CHECKED_COL) != null) {
			// OK, the table is already checked; skip it
			return;
		}
		
		m_logger.debug("Checking " + tabDef);
		
		List<String> keys = new ArrayList<String>(m_maxListCapacity);
		BigSet setKeysToMove = new BigSet(m_pathToKeys + "objkeystomove", SET_CARDINALITY);
		
		Iterator<DColumn> colIterator = dbService.getAllColumns(termsTable, Defs.ALL_OBJECTS_ROW_KEY);
		if (colIterator == null) {
			// No objects, table is empty (or does not exist)
			return;
		}
		while (colIterator.hasNext()) {
			if (keys.size() >= m_maxListCapacity) {
				processKeys(tabDef, keys, setKeysToMove);
			}
			keys.add(colIterator.next().getName());
			if (isInterrupted()) break;
		}
		
		processKeys(tabDef, keys, setKeysToMove);
		moveKeys(tabDef, setKeysToMove);
		
		setKeysToMove.delete();
		
		m_checkerTransaction.addCheckedRow(tabDef);
		m_checkerTransaction.commit();
	}	// checkObjectSharding
	
	/**
	 * Performs object moving (if necessary). The object IDs are saved in a previously
	 * created list.
	 * 
	 * @param tabDef	Table to get objects from
	 * @param keys		List of objectIDs to move to shards.
	 */
	private void processKeys(TableDefinition tabDef, List<String> keys, BigSet setKeysToMove) {
		if (keys.isEmpty()) {
			return;
		}
		
		DBService dbService = DBService.instance();
		String shardingFieldName = tabDef.getShardingField().getName();
		String objectTable = SpiderService.objectsStoreName(tabDef);
		
		// Get objects with their _IDs and Shard field values
		Iterator<DRow> collectedFields = 
				dbService.getRowsColumns(
						objectTable, keys, Arrays.asList(CommonDefs.ID_FIELD, shardingFieldName));

		keys.clear();

		// Check all the objects; if found an object that belongs
		// to not a default shard - move it.
		while (collectedFields.hasNext()) {
			DRow row = collectedFields.next();
			Iterator<DColumn> columnList = row.getColumns();
			String objectId = null;
			String dateBytes = null;
			while (columnList.hasNext()) {
				DColumn column = columnList.next();
				if (CommonDefs.ID_FIELD.equals(column.getName())) {
					objectId = column.getValue();
				} else if (shardingFieldName.equals(column.getName())) {
					dateBytes = column.getValue();
				}
			}
			if (objectId == null || dateBytes == null) {
				// Dead object or sharding field is not defined
				continue;
			}
			int shardNo = 0;
			if (dateBytes.length() > 0) {
				try {
					Date dt = Utils.dateFromString(dateBytes);
					shardNo = tabDef.computeShardNumber(dt);
				} catch (IllegalArgumentException e) {
					continue;
				}
			}
			if (shardNo > 0) {
				setKeysToMove.add(shardNo + "/" + objectId);
			}
		}
	}	// processKeys
	
	private void moveKeys(TableDefinition tabDef, BigSet keys) {
		Iterator<BSTR> iKeys = keys.iterator();
		while (iKeys.hasNext()) {
			String shardKey = iKeys.next().toString();
			int sepIndex = shardKey.indexOf('/');
			int shardNo = Integer.parseInt(shardKey.substring(0, sepIndex));
			String objectId = shardKey.substring(sepIndex + 1);
			// Moving an object from general shard to shard # shardNo
			m_checkerTransaction.addShard(tabDef, shardNo);
			moveTo(tabDef, objectId, shardNo);
			m_checkerTransaction.checkCommit();
		}
		m_checkerTransaction.commit();
	}
	
	/**
	 * Moves terms of the objects to a given shard.
	 * 
	 * @param tabDef
	 * @param objectId
	 * @param shardNo
	 * @throws IOException
	 */
	private void moveTo(TableDefinition tabDef, String objectId, int shardNo) {
		String objectTable = SpiderService.objectsStoreName(tabDef);
		
		// Moving id from "all objects row" to "shard objects row"
		m_checkerTransaction.deleteAllObjectsColumn(tabDef, 0, objectId);
		m_checkerTransaction.addAllObjectsColumn(tabDef, shardNo, objectId);

		// Moving terms
		for (Iterator<DColumn> iColumns = DBService.instance().getAllColumns(objectTable, objectId);
				iColumns.hasNext();) {
			DColumn column = iColumns.next();
			String fieldName = column.getName();
			if (fieldName.charAt(0) != '~' && fieldName.charAt(0) != '_' && fieldName.charAt(0) != '!') {
				String fieldValue = column.getValue();
				Set<String> terms = getTerms(tabDef, fieldName, fieldValue);
				for (String term : terms) {
					m_checkerTransaction.deleteTerm(tabDef, 0, objectId, fieldName, term);
					m_checkerTransaction.addTerm(tabDef, shardNo, objectId, fieldName, term);
				}
			}
		}
		
		m_logger.info("Terms of object " + objectId + " moved to shard # " + shardNo);
	}	// moveTo

	/**
	 * Checks sharded links from a given link field. Extracts all the link fields from 
	 * the object table row and checks whether the linked object is actually sharded.
	 * Moves the link to a corresponding shard if necessary.
	 * @param tableFrom Table to inspect links from.
	 * @param fieldFrom Link field.
	 * @param tableTo Destination (extent) table.
	 * @throws IOException
	 */
	private void checkLinkSharding(TableDefinition tableFrom) {
		DBService dbService = DBService.instance();
		
		Set<String> whichLinksToCheck = new HashSet<String>();
		for (FieldDefinition linkDef : tableFrom.getFieldDefinitions()) {
			if (linkDef.isLinkField() && linkDef.isSharded()) {
				whichLinksToCheck.add(linkDef.getName());
			}
		}
		Iterator<DColumn> colsIter = dbService.getAllColumns(
				SpiderService.termsStoreName(tableFrom), Defs.TABLE_CHECKS_ROW_KEY);
		if (colsIter != null) {
			while (colsIter.hasNext()) {
				DColumn column = colsIter.next();
				whichLinksToCheck.remove(column.getName());
			}
		}
		
		if (whichLinksToCheck.isEmpty()) {
			// No links to check
			return;
		}

		m_logger.info("Checking sharded links from " + tableFrom.getTableName());
		
		Iterator<DRow> rowsIter = dbService.getAllRowsAllColumns(
				SpiderService.objectsStoreName(tableFrom));
		KeysMap keysMap = new KeysMap(tableFrom, m_pathToKeys, m_checkerTransaction);
		while (rowsIter.hasNext()) {
			DRow row = rowsIter.next();
			String idFrom = row.getKey();
			
			Iterator<DColumn> columnsIter = row.getColumns();
			while (columnsIter.hasNext()) {
				DColumn column = columnsIter.next();
				String columnName = column.getName();
				if (columnName.charAt(0) == '~') {
					// Link column found
					LinkInfo linkInfo = parseLinkField(tableFrom, columnName);
					if (linkInfo != null && linkInfo.m_fieldDef != null) {
						FieldDefinition fieldFrom = linkInfo.m_fieldDef;
						// Skip link if it is not sharded
						if (!fieldFrom.isSharded() ||
							!whichLinksToCheck.contains(fieldFrom.getName())) {
								// Link is not sharded or was already checked
								continue;
						}
						keysMap.addKey(idFrom, fieldFrom.getName(), linkInfo.m_linkId);
					}
				}
			}
			
			keysMap.processKeys(idFrom);
			keysMap.moveLinks(idFrom);
			
			if (isInterrupted()) {
				m_checkerTransaction.commit();
				return;
			}
		}
		
		keysMap.deleteKeysSet();
		
		for (String newLinkChecked : whichLinksToCheck) {
			m_checkerTransaction.addCheckedRow(tableFrom, newLinkChecked);
		}
		m_checkerTransaction.commit();
	}	// checkLinkSharding
	
	/**
	 * A class that incapsulates information about sharded links to move to shards.
	 */
	private static class KeysMap {
		private static final int maxListCapacity = 1000;
		
		// Links information cache.
		private Map<String, List<String>> keysMap = new HashMap<>();
		private BigSet keysSet;
		
		// Table definition for the objects to process.
		private final TableDefinition tableFrom;
		private final CheckerTransaction m_checkerTransaction;
		
		public KeysMap(TableDefinition tabFrom, String pathToTempDir, CheckerTransaction checkerTransaction) {
			tableFrom = tabFrom;
			keysSet = new BigSet(pathToTempDir + "linkstomove", SET_CARDINALITY);
			m_checkerTransaction = checkerTransaction;
		}
		
		/**
		 * Adds a link to cache.
		 * 
		 * @param idFrom		Object ID that contains the link
		 * @param linkName		Link field name
		 * @param linkObjectId	Linked object ID
		 */
		public void addKey(String idFrom, String linkName, byte[] linkObjectId) {
			List<String> listKeys = keysMap.get(linkName);
			if (listKeys == null) {
				listKeys = new ArrayList<String>();
				keysMap.put(linkName, listKeys);
			} else if (listKeys.size() >= maxListCapacity) {
				processKeys(idFrom, linkName);
			}
			listKeys.add(Utils.toString(linkObjectId));
		}	// addKey
		
		/**
		 * Perfoms link shift to corresponding shards.
		 * 
		 * @param idFrom	Object ID to move the link from	
		 * @param linkName	Link field name
		 */
		public void processKeys(String idFrom, String linkName) {
			DBService dbService = DBService.instance();
			
			List<String> keys = keysMap.get(linkName);
			if (keys.size() == 0) {
				return;
			}
			
			FieldDefinition fieldFrom = tableFrom.getFieldDef(linkName);
			TableDefinition tableTo = 
					tableFrom.getAppDef().getTableDef(fieldFrom.getLinkExtent());
			
			// Get objects with their Sharding fields values
			List<String> shardingFieldName = Arrays.asList(tableTo.getShardingField().getName());
			Iterator<DRow> rowsIter = dbService.getRowsColumns(
					SpiderService.objectsStoreName(tableTo), keys, shardingFieldName);
			while (rowsIter.hasNext()) {
				DRow row = rowsIter.next();
				String rowKey = row.getKey();
				Iterator<DColumn> colsIter = row.getColumns();
				if (!colsIter.hasNext()) {
					// Object is dead or is not sharded
					continue;
				}

				String columnValue = colsIter.next().getValue();
				if (Utils.isEmpty(columnValue)) {
					// No sharding value; assume shard = 0
					continue;
				}
				
				int shardNo = tableTo.computeShardNumber(Utils.dateFromString(columnValue));
				if (shardNo == 0) {
					// Default shard number; no processing needed.
					continue;
				}

				keysSet.add(shardNo + "/" + rowKey + "/" + fieldFrom.getName());
			}
			keys.clear();
			
		}	// processKeys
		
		/**
		 * Moves all the links that were cached for moving.
		 * 
		 * @param idFrom	Object ID to process its links
		 */
		public void processKeys(String idFrom) {
			for (String linkName : keysMap.keySet()) {
				processKeys(idFrom, linkName);
			}
		}	// processKeys
		
		public void moveLinks(String idFrom) {
			Iterator<BSTR> iLinks = keysSet.iterator();
			while (iLinks.hasNext()) {
				// shardNo / rowKey / linkFrom
				String[] combined = iLinks.next().toString().split("/");
				String shardNo = combined[0];
				String rowKey = combined[1];
				String linkFrom = combined[2];
				// Move link to correspondent shard and commit this mutation instantly.
				m_checkerTransaction.deleteDirectLink(tableFrom, idFrom, rowKey, linkFrom);
				m_checkerTransaction.addShardedLink(tableFrom, shardNo, idFrom, rowKey, linkFrom);
				m_checkerTransaction.checkCommit();
			}
		}
		
		public void deleteKeysSet() {
			keysSet.delete();
		}
	}
	
	/**
	 * Computes the terms on the field value provided a table of all the scalar
	 * fields is given.
	 * 
	 * @param fieldName	Field name
	 * @param fieldValue Field value
	 * @param scalarFields Table of all the scalar fields
	 * @return
	 */
	private static Set<String> getTerms(TableDefinition tabDef, String fieldName, String fieldValue) {
		FieldAnalyzer analyzer = TextAnalyzer.instance();	// default analyzer
		FieldDefinition fieldDef = tabDef.getFieldDef(fieldName);
		if (fieldDef != null && fieldDef.isScalarField()) {
			FieldAnalyzer fieldAnalyzer = FieldAnalyzer.findAnalyzer(fieldDef);
			if (fieldAnalyzer == NullAnalyzer.instance()) {
				// No terms produced
				return null;
			} else if (fieldAnalyzer != null) {
				analyzer = fieldAnalyzer;
			}
		}
        return analyzer.extractTerms(fieldValue);
	}	// getTerms
	
	/**
	 * Objects of this small class contains information about a link.
	 */
	private static class LinkInfo {
		/**
		 * Link definition 
		 */
		final FieldDefinition m_fieldDef;
		
		/**
		 * Link destination object ID
		 */
		final byte[] m_linkId;
		
		LinkInfo(FieldDefinition fieldDef, byte[] linkId) {
			m_fieldDef = fieldDef;
			m_linkId = linkId;
		}
	}	// LinkInfo
	
	/**
	 * Splits the link column name to a link identifier and object ID.
	 * Computes the Link definition field (given a table definition).
	 * 
	 * @param tableDef Table where to get the link field from.
	 * @param columnName Column name to parse.
	 * @return
	 */
	private static LinkInfo parseLinkField(TableDefinition tableDef, String columnName) {
		int slashIndex = columnName.indexOf('/');
		if (slashIndex < 2 || slashIndex == columnName.length() - 1) return null;
		String linkName = columnName.substring(1, slashIndex);
		byte[] idValue = Utils.toBytes(columnName.substring(slashIndex+1, columnName.length()));
		return new LinkInfo(tableDef.getFieldDef(linkName), idValue);
	}	// parseLinkField
}
