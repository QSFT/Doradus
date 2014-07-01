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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.CommonDefs;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.FieldType;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.core.Defs;
import com.dell.doradus.fieldanalyzer.TextAnalyzer;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DBTransaction;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.DRow;
import com.dell.doradus.service.schema.SchemaService;
import com.dell.doradus.service.spider.SpiderHelper;
import com.dell.doradus.service.spider.SpiderService;
import com.dell.doradus.utilities.BigSet;
import com.google.common.io.Files;

/**
 * Performs data checks in the database. The algorithm is as follows.
 * <ol><li>Objects-to-terms and terms-to-objects. These are "per/table" checks,
 * every table is scanned sequentially (the objects table and then the terms table),
 * and the sorted data from the table are written in the "Big sets".
 * Then the 2 sets are compared for equality, and the necessary corrections are made
 * (if needed). After this the sets are removed from the disk, so the storage may be
 * re-used for scanning the next table.
 * Note. If some changes were made during the data checks process, the "correction" may
 * introduce wrong data into the terms table. Actually these "wrong corrections"
 * should be improved next time a data-checks applied.
 * 
 * <li>Objects registering. All the objects IDs are stored in one big set,
 * and the contents of the _ record in another. After the tables are scanned entirely,
 * the sets are compared and the necessary corrections are made (if needed).
 * Again, after every table scanning the created sets are removed from disk memory.
 * Note. "Wrong corrections" are also possible if some objects were created or removed
 * during the data check.
 * 
 * <li>Fields/terms registering. When scanning objects, all the fields and terms
 * are stored in a big set, and the contents of the _fields and _terms records
 * of the terms tables are stored in another set.
 * These sets are also created for each table separately.
 * 
 * <li>Links checks. These kind of checks is "global", i.e. 2 big sets are created
 * for all the links from all the database. Actually when the task is launched for
 * one or several tables only, then only the links for a "transitive closure" of
 * the tables that are actually linked.
 * Note. "Wrong corrections" can remove existing  links if data were changed during
 * the data checks, and that is the most potentially dangerous situation since
 * the removed links wouldnt be restored later. For example, we scanned a table
 * and saved all the links from this table in a big set. But before we scan a linked
 * table some new object was created that had some new links. The program will remove
 * these new links since it finds these links lead to a "non-existent"
 * (actually not existed at the time of scanning) object.
 */
public class DataChecker extends DoradusTask {
	
    // Logging interface:
    private Logger m_logger = LoggerFactory.getLogger(getClass().getSimpleName());
    
	/*
	 * Connection to Database.
	 */
	private DBService m_dbService;
	
	/*
	 * Paths to temporary files where "big sets" are stored.
	 */
	private File m_tempDir = null;
	// File for object terms taken from the objects table
	private String m_objs2TermsPath;
	// File for object terms taken from the terms table
	private String m_terms2ObjsPath;
	// File for all the object IDs taken from the objects table
	private String m_objsRegPath;
	// File for all the object IDs taken from the terms table
	private String m_allObjsRegPath;
	// File for object fields and terms taken from the objects table
	private String m_termsPath;
	// File for object fields and terms taken from the terms table
	private String m_termsRegPath;
	// File for object links
	private String m_dirLinksPath;
	// file for reversed links
	private String m_revLinksPath;
	
	// Writers for the links
	private BigSet m_dirLinksSet;
	private BigSet m_revLinksSet;
	
    // Table processing start time
    private long m_startTime;

	// Size of the set before a shard will be stored on the disk.
	private final static int SET_CARDINALITY = 1 << 20;	// ~1 mln records per file shard
	
	// Debugging flags that can exclude some kinds of checks
	final static boolean SKIP_LINKS = false;
	final static boolean SKIP_TERMS = false;
	final static boolean SKIP_REGOBJS = false;
	final static boolean SKIP_REGTERMS = false;
	
	// Tables to check
	private Set<String> m_tables;
	// Additional tables for links checking
	private Set<String> m_linkedTables;
	private ApplicationDefinition m_appDef;
	
	/**
	 * Link class represents a Doradus link - a byte sequence of a form
	 * ~&lt;LinkName&gt;/&lt;ObjectID&gt; or ~&lt;LinkNo&gt;&lt;ObjectID&gt;.
	 */
	private static class Link {
		final FieldDefinition fieldDef;	// Link field definition
		String objectId;				// Linked object ID.
		
		/**
		 * Constructs a link from a given field definition and object ID.
		 * Simply copies and saves the arguments.
		 * @param fieldDef
		 * @param objectId
		 */
		public Link(FieldDefinition fieldDef, String objectId) {
			this.fieldDef = fieldDef;
			this.objectId = objectId;
		}
		
		/**
		 * Constructs the link from a column name in the context of a given table definition.
		 * @param tabDef		Context table definition
		 * @param columnName	Source column name
		 */
		public Link(TableDefinition tabDef, String columnName) {
			// Link is defined by name
			int index = columnName.indexOf('/');
			fieldDef = tabDef.getFieldDef(columnName.substring(1, index));
			// Object ID is a string anyway
			objectId = columnName.substring(index + 1);
		}
		
		/**
		 * Combines the column name from the field
		 * @return
		 */
		public String getColumnName() {
			return "~" + fieldDef.getName() + "/" + objectId;
		}
	}
	
	/**
	 * Represents a key in a terms table. A key may be a term key or a sharded links key
	 * or one of the system rows like "_", "_fields" or "_terms" rows.
	 */
	private static class TermsRowKey {
		FieldDefinition m_fieldDef = null;	// Link field in case of a sharded links key;
											// Scalar field in case of terms or "_terms" key
		String m_term = null;				// Term in case of term key
		String m_objectId = null;			// Object ID in case of a sharded links key
		String m_shardPrefix = "";			// "<shard>/" prefix in case of sharded row
		boolean m_allObjects = false;		// Is it a "_" or "<shard>/_" row?
		boolean m_allFields = false;		// Is it a "_fields" row?
		boolean m_fieldTerms = false;		// Is it a "_terms/<field>" or "<shard>/_terms/<field>" row?
		boolean m_isValid = false;			// Is it a valid row ("_modified" rows are "invalid")?
		
		/**
		 * Constructs the terms table key information in a context
		 * of a given table definition. System rows are skipped.
		 * @param tabDef
		 * @param rowKey
		 */
		public TermsRowKey(TableDefinition tabDef, String rowKey) {
			// Correct shard number for sharded tables
			if (tabDef.isSharded()) {
				// We assume shard prefix is "0/" even for not sharded rows if the table itself is sharded.
				m_shardPrefix = "0/";
			}
			
			// First we try to analyze a system row key
			if (rowKey.charAt(0) == '_') {
				if (rowKey.length() == 1) {
					// All objects row
					m_isValid = m_allObjects = true;
				} else if (Defs.FIELD_REGISTRY_ROW_KEY.equals(rowKey)) {
					// "_fields" row
					m_isValid = m_allFields = true;
				} else if (rowKey.startsWith(Defs.TERMS_REGISTRY_ROW_PREFIX + "/")) {
					// "_terms/<field>" row
					m_isValid = m_fieldTerms = true;
					m_fieldDef = getFieldDef(tabDef,
							rowKey.substring(Defs.TERMS_REGISTRY_ROW_PREFIX.length() + 1));
				}
				// Otherwise it is a system row that we can skip.
				return;
			}
			
			// We can find a first slash symbol to extract shard number
			int slashIndex = rowKey.indexOf('/');
			if (slashIndex >= 0) {
				// Slash found
				try {
					int shardNo = Integer.parseInt(rowKey.substring(0, slashIndex));
					// Shard number prefix exists; save it
					m_shardPrefix = shardNo + "/";
					rowKey = rowKey.substring(slashIndex + 1);
				} catch (NumberFormatException x) {
					// m_shardNo = 0;
				}
			}
			
			// Now that we have shard number processed we can analyze the rest of row key again
			if (rowKey.charAt(0) == '_') {
				if (rowKey.length() == 1) {
					// Sharded all objects row
					m_isValid = m_allObjects = true;
				} else if (rowKey.startsWith(Defs.TERMS_REGISTRY_ROW_PREFIX + "/")) {
					// Sharded "<shard>/_terms/<field>" row key
					m_isValid = m_fieldTerms = true;
					m_fieldDef = getFieldDef(tabDef,
							rowKey.substring(Defs.TERMS_REGISTRY_ROW_PREFIX.length() + 1));
				}
			} else if (rowKey.charAt(0) == '~') {
				// sharded link row
				assert rowKey.length() > 2;
				
				slashIndex = rowKey.indexOf('/');
				
				assert slashIndex >= 0;

				m_fieldDef = tabDef.getFieldDef(rowKey.substring(1, slashIndex));
				m_objectId = rowKey.substring(slashIndex+1);
				m_isValid = true;
			} else {
				// <field>/<term> row
				slashIndex = rowKey.indexOf('/');
				String fieldName = slashIndex >= 0 ? rowKey.substring(0, slashIndex) : rowKey;
				m_fieldDef = getFieldDef(tabDef, fieldName);
				if (slashIndex >= 0) {
					m_term = rowKey.substring(slashIndex + 1);
				}
				m_isValid = true;
			}
		}
		
		// Access functions
		public FieldDefinition getFieldDef() { return m_fieldDef; }
		
		public String getTerm() { return m_term; }
		
		public String getObjectId() { return m_objectId; }
		
		public boolean isAllObjects() { return m_allObjects; }
		
		public boolean isAllFields() { return m_allFields; }
		
		public boolean isFieldTerms() { return m_fieldTerms; }
		
		public boolean isValid() { return m_isValid; }
		
		//-------------- private functions -----------------
		
		/**
		 * Gets field definition from the table definition, and creates a new simple one
		 * if it doesn't exist. The new field definition is necessary for its type and analyzer.
		 * @param tabDef		Source table
		 * @param fieldName		Field name
		 * @return				Field definition
		 */
		private static FieldDefinition getFieldDef(TableDefinition tabDef, String fieldName) {
			FieldDefinition fieldDef = tabDef.getFieldDef(fieldName);
			if (fieldDef == null) {
				// No field definition - create an additional one
				fieldDef = new FieldDefinition(tabDef);
				fieldDef.setType(FieldType.TEXT);
				fieldDef.setName(fieldName);
				fieldDef.setAnalyzer(TextAnalyzer.class.getName());
			}
			return fieldDef;
		}
	}
	
	/**
	 * Main function for start from Quartz. It takes the program parameters from
	 * context that Quartz delivers to it.
	 * @param ctx Task context (parameters, etc.)
	 */
	@Override
	protected void runTask() {
		// Logging
		m_logger.info("Data integrity checking task started");
		m_startTime = System.currentTimeMillis();

		// Create a database connection
        m_dbService = DBService.instance();
		m_tempDir = Files.createTempDir();
		String tempDirPath = m_tempDir.getAbsolutePath() + File.separator;
		
        //m_tempDir = new File(System.getProperty("user.dir"));
		m_objs2TermsPath = tempDirPath + "objs2terms";
		m_terms2ObjsPath = tempDirPath + "terms2objs";
		m_objsRegPath = tempDirPath + "objsreg";
		m_allObjsRegPath = tempDirPath + "allobjsreg";
		m_termsPath = tempDirPath + "terms";
		m_termsRegPath = tempDirPath + "termsreg";
		m_dirLinksPath = tempDirPath + "dirlinks";
		m_revLinksPath = tempDirPath + "revlinks";
		
		m_dirLinksSet = new BigSet(m_dirLinksPath, SET_CARDINALITY);
		m_revLinksSet = new BigSet(m_revLinksPath, SET_CARDINALITY);

        m_appDef = SchemaService.instance().getApplication(getAppName());
		if (m_appDef == null) {
			// Apparently the application was deleted
			m_logger.error("Application {} doesn\'t exist anymore", getAppName());
			return;
		}
		
		m_tables = "*".equals(getTableName()) ? m_appDef.getTableDefinitions().keySet() : new HashSet<String>();
		m_linkedTables = new HashSet<String>();
		if (!"*".equals(getTableName())) {
			for (String tabName : getTableName().split(",")) {
				m_tables.add(tabName.trim());
			}
		}
		
		// Perform data checking for all the tables sequentially
		for (String tabName : m_tables) {
			TableDefinition tabDef = m_appDef.getTableDef(tabName);
			if (tabDef != null) {
				checkTableObjects(tabDef);
				if (isInterrupted()) {
					return;
				}
			}
		}
		
		if (!SKIP_LINKS) {
			m_logger.info("Links processing");
			long startTime = System.currentTimeMillis();
			Set<String> checkedTables = new HashSet<String>();
			Set<String> diffTables;
			for (;;) {
				diffTables = diffSet(m_linkedTables, checkedTables);
				if (diffTables.isEmpty()) break;
				checkedTables.addAll(diffTables);
				for (String tabName : diffTables) {
					TableDefinition tabDef = m_appDef.getTableDef(tabName);
					checkLinkedTable(tabDef);
					if (isInterrupted()) {
						return;
					}
				}
			}
			elapsedTime("Links processing time", startTime);
		}

		long startTime = System.currentTimeMillis();
		
		startTime = elapsedTime("Closing links sets", startTime);
		
		compareAndImproveLinks(m_dirLinksSet, m_revLinksSet);
		
		elapsedTime("Checking links correctness time", startTime);

		if (!Utils.deleteDirectory(m_tempDir)) {
			m_logger.info("Could not delete a temporary directory {}", m_tempDir.getAbsolutePath());
		}

		long timeSpan = (System.currentTimeMillis() - m_startTime) / 1000;
		int mins = (int)(timeSpan / 60);
		int secs = (int)(timeSpan % 60);
		m_logger.info("stopped; working time is "
				+ (mins > 0 ? mins + " mins " : "")
				+ secs + " secs.");
	}
	
	//-------------------- Private functions ---------------------
	
	private Set<String> diffSet(Set<String> s1, Set<String> s2) {
		Set<String> result = new HashSet<String>(s1);
		result.removeAll(s2);
		return result;
	}
	
	private void checkTableObjects(TableDefinition tabDef) {
		m_logger.info(tabDef + " processing");
		long startTime = System.currentTimeMillis();
		
		String objectTable = SpiderService.objectsStoreName(tabDef);
		String termsTable = SpiderService.termsStoreName(tabDef);
		boolean sharded = tabDef.isSharded();
		
		// Records in the obj2terms and terms2objs sets enumerate all the existing
		// object/field/term triples. The records have the following structure:
		// <shard-prefix><object-id><field><term> where
		// <shard-prefix> is empty for non-sharded tables,
		//                and is of "<shardNo>/" form where
		//                the <shardNo is the object shard number (possibly 0);
		// <object-id> is a bytes sequence, the first 2 bytes are the length of
		//             the rest sequence that represent proper object ID
		//             (converted to base64 strings in case of base64 keys);
		// <field> is a "<field name>" string;
		// <term> is a "/<term>" string.
		BigSet objs2TermsSet = new BigSet(m_objs2TermsPath, SET_CARDINALITY);
		BigSet terms2ObjsSet = new BigSet(m_terms2ObjsPath, SET_CARDINALITY);
		
		// Records in the objsreg and allobjsreg sets enumerate all the objects
		// in the form of <shard-prefix><object-id> where the <shard-prefix> is
		// empty for non-sharded tables and the shard number of an object followed
		// by &acute;/&acute; slash symbol, and the <object-id> is simply an object ID.
		BigSet objsRegSet = new BigSet(m_objsRegPath, SET_CARDINALITY);
		BigSet allObjsRegSet = new BigSet(m_allObjsRegPath, SET_CARDINALITY);
		
		// Records in the terms and termsreg sets enumerate all the existing
		// field/term pairs and fields regardless of what objects they belong to.
		// The structure of records are the same as for objs2terms and term2objs sets
		// excluding the <object-id> part.
		// If the <term> part is empty, then the record represents a field.
		BigSet termsSet = new BigSet(m_termsPath, SET_CARDINALITY);
		BigSet termsRegSet = new BigSet(m_termsRegPath, SET_CARDINALITY);
		
		// Process objects table
		Iterator<DRow> iRows = m_dbService.getAllRowsAllColumns(objectTable);
		while (iRows.hasNext()) {
			if (isInterrupted()) {
				return;
			}

			DRow row = iRows.next();
			
			String objectID = row.getKey();	// Current object ID
			String shardPrefix = "";	// Current object shard number (when the table is sharded)
			
			if (sharded) {
				String shardDate = m_dbService.getColumn(
						objectTable, objectID, 
						tabDef.getShardingField().getName()).getValue();
				int shardNo = tabDef.computeShardNumber(Utils.dateFromString(shardDate));
				// shard prefix always exists for sharded tables
				shardPrefix = Integer.toString(shardNo) + "/";
			}

			if (!SKIP_REGOBJS) {
				objsRegSet.add(shardPrefix + objectID);
			}

			Iterator<DColumn> iColumns = row.getColumns();
			while (iColumns.hasNext()) {
				DColumn column = iColumns.next();
				String fieldName = column.getName();
				String fieldValue = column.getValue();
				
				if (fieldName.charAt(0) == '_') {
					// System column
					continue;
				}
				
				if (fieldName.charAt(0) == '~') {
					// Link field
					if (!SKIP_LINKS) {
						try {
							collectLinkColumn(tabDef, shardPrefix, objectID, fieldName);
						} catch (Exception x) {
							// Couldn't create a link; skip the field
						}
					}
				} else {
					// Scalar field
					if (!SKIP_REGTERMS) {
						// Register field
						termsSet.add(fieldName);
					}
					Set<String> terms = SpiderHelper.getTerms(fieldName, fieldValue, tabDef);
					if (terms == null) {
						continue;
					}
					for (String term : terms) {
						String tail = fieldName + "/" + term;
						if (!SKIP_REGTERMS) {
							// Register terms
							termsSet.add(shardPrefix + tail);
						}
						if (!SKIP_TERMS) {
							// Register object/term pairs
							objs2TermsSet.add(shardPrefix + objectID + "/" + tail);
						}
					}
				}
			}	// iColumns
		}	// iRows
		
		startTime = elapsedTime("Object table processed time", startTime);
		
		// Process terms table
		Iterator<DRow> itRows = m_dbService.getAllRowsAllColumns(termsTable);
		while (itRows.hasNext()) {
			if (isInterrupted()) {
				return;
			}

			DRow row = itRows.next();
			
			TermsRowKey rowKey = new TermsRowKey(tabDef, row.getKey());		// Row key collected info
			if (!rowKey.isValid()) {
				// Skip invalid row
				continue;
			}
			
			// Additional fields for the case of sharded links (<shard>/~<link>/<object>) row:
			int shardNo = 0;					// Shard number of the object
			TableDefinition rTabDef = null;		// reversed link table
			FieldDefinition rLinkDef = null;	// reversed link field definition

			if (rowKey.getObjectId() != null) {
				// Sharded link row - calculate additional information.
				rTabDef = tabDef.getAppDef().getTableDef(rowKey.getFieldDef().getLinkExtent());
				rLinkDef = rTabDef.getFieldDef(rowKey.getFieldDef().getLinkInverse());
				if (rLinkDef.isSharded()) {
					String shardDate = m_dbService.getColumn(objectTable, rowKey.getObjectId(), 
							tabDef.getShardingField().getName()).getValue();
					shardNo = tabDef.computeShardNumber(Utils.dateFromString(shardDate));
				}
			}
			
			Iterator<DColumn> itColumns = row.getColumns();
			while (itColumns.hasNext()) {
				DColumn column = itColumns.next();
				String columnName = column.getName();

				if (rowKey.isAllObjects()) {
					// "_" row
					if (!SKIP_REGOBJS) {
						allObjsRegSet.add(rowKey.m_shardPrefix + columnName);
					}
				} else if (rowKey.isAllFields()) {
					// "_fields" row
					if (!SKIP_REGTERMS) {
						termsRegSet.add(columnName);
					}
				} else if (rowKey.isFieldTerms()) {
					//"_terms/<field>" row
					if (!SKIP_REGTERMS) {
						termsRegSet.add(
								rowKey.m_shardPrefix + 
								rowKey.getFieldDef().getName() + 
								"/" + columnName);
					}
				} else if (rowKey.getObjectId() != null) {
					// Sharded links row
					if (!SKIP_LINKS) {
						String prefix = rowKey.m_shardPrefix + tabDef.getTableName() + 
								"/" + rowKey.getFieldDef().getName() + "/";
						m_dirLinksSet.add(prefix + rowKey.getObjectId() + "/" + columnName);
						String rShardPrefix = rLinkDef.isSharded() ? shardNo + "/" : "0/";
						m_revLinksSet.add(rShardPrefix + rTabDef.getTableName() + "/" + 
								rLinkDef.getName() + "/" + columnName + "/" + rowKey.getObjectId());
					}
				} else {
					// "<field>/<term>" row
					if (!SKIP_TERMS) {
						String postfix = rowKey.getFieldDef().getName() + "/" + rowKey.getTerm();
						terms2ObjsSet.add(rowKey.m_shardPrefix + columnName + "/" + postfix);
					}
				}
			}	// itColumns
		}	// itRows
		
		startTime = elapsedTime("Terms table processed time", startTime);

		// Comparing data and making improvements.
		compareAndImproveTerms(tabDef, objs2TermsSet, terms2ObjsSet);
		compareAndImproveObjects(tabDef, objsRegSet, allObjsRegSet);
		compareAndImproveFieldReg(tabDef, termsSet, termsRegSet);
		
		startTime = elapsedTime("Big sets comparing time", startTime);

	}
	
	/**
	 * A function for debugging: register time spans and issue the messages
	 * @param message	Message
	 * @param startTime	Start time
	 * @return			Current time.
	 */
	private long elapsedTime(String message, long startTime) {
		long currentTime = System.currentTimeMillis();
		long secs = (currentTime - startTime) / 1000;
		int mins = (int)(secs / 60);
		
		m_logger.debug(message + ": " + mins + " mins " + (int)(secs % 60) + " secs");
		return currentTime;
	}
	
	/**
	 * Compares 2 big sets that contain information about terms.
	 * The structure of records is as following:
	 * &lt;prefix&gt;&lt;objectID&gt;/&lt;field&gt;/&lt;term&gt; where
	 * <ul><li>prefix - empty for non-sharded tables; &lt;shard&gt;/ for sharded tables
	 * where &lt;shard&gt; is the number of object shard;
	 * <li>&lt;objectID&gt; - ID of the object;
	 * <li>&lt;field&gt; - field name;
	 * <li>&lt;term&gt; - term
	 * </ul>
	 * @param tabDef
	 */
	private void compareAndImproveTerms(TableDefinition tabDef, BigSet objs2TermsSet, BigSet terms2ObjsSet) {
		String termsTable = SpiderService.termsStoreName(tabDef);
		
		Iterator<BSTR> objsIterator = objs2TermsSet.iterator();
		Iterator<BSTR> termsIterator = terms2ObjsSet.iterator();
		
		while (objsIterator.hasNext() || termsIterator.hasNext()) {
			// Extracting records for comparing
			BSTR objRecord = objsIterator.hasNext() ? objsIterator.next() : null;
			BSTR termRecord = termsIterator.hasNext() ? termsIterator.next() : null;
			while (!BSTR.isEqual(objRecord, termRecord)) {
				// We add a term to the terms table if an object has a term
				// that is not registered in the terms table;
				// We remove a term if we see a registered term that doesn't
				// actually belong to the object.
				boolean bAddColumn = 
						termRecord == null ||
						(objRecord != null && BSTR.compare(objRecord, termRecord) < 0);
				// Parse term record
				String extraTerm = (bAddColumn ? objRecord : termRecord).toString();
				int shardNo = 0;
				
				int startSlashIndex = 0;
				if (tabDef.isSharded()) {
					startSlashIndex = extraTerm.indexOf('/') + 1;
					// Parse shard number (possibly 0)
					shardNo = Integer.parseInt(
							extraTerm.substring(0, startSlashIndex - 1));
				}
				
				// Extract object ID: 2 first bytes contain the length.
				int nextSlashIndex = extraTerm.indexOf('/', startSlashIndex) + 1;
				String objectID = extraTerm.substring(startSlashIndex, nextSlashIndex - 1);
				
				// Construct the <shard>/<field>/<term> terms table row key
				String fieldTerm = extraTerm.substring(nextSlashIndex);
				startSlashIndex = fieldTerm.indexOf('/');
				String fieldName = fieldTerm.substring(0, startSlashIndex);
				String term = fieldTerm.substring(startSlashIndex + 1);
				String rowKey = shardNo == 0 ? fieldTerm : shardNo + "/" + fieldTerm;
				
				// Add or remove the term column
				if (bAddColumn) {
					objRecord = objsIterator.hasNext() ? objsIterator.next() : null;
					if (checkTerm(tabDef, shardNo, objectID, fieldName, term)) {
						addColumn(objectID, termsTable, rowKey);
					}
				} else {
					termRecord = termsIterator.hasNext() ? termsIterator.next() : null;
					if (!checkTerm(tabDef, shardNo, objectID, fieldName, term)) {
						removeColumn(objectID, termsTable, rowKey);
					}
				}
			}
		}
		
		objs2TermsSet.delete();
		terms2ObjsSet.delete();
	}
	
	/**
	 * Compares 2 big sets that contain information about object IDs.
	 * The structure of records is as following:
	 * &lt;prefix&gt;&lt;objectID&gt; where
	 * <ul><li>prefix - empty for non-sharded tables; &lt;shard&gt;/ for sharded tables
	 * where &lt;shard&gt; is the number of object shard;
	 * <li>&lt;objectID&gt; - ID of the object;
	 * </ul>
	 * @param tabDef
	 */
	private void compareAndImproveObjects(TableDefinition tabDef, BigSet objsRegSet, BigSet allObjsRegSet) {
		String termsTable = SpiderService.termsStoreName(tabDef);
		
		Iterator<BSTR> objsIterator = objsRegSet.iterator();
		Iterator<BSTR> termsIterator = allObjsRegSet.iterator();
		
		while (objsIterator.hasNext() || termsIterator.hasNext()) {
			// Extracting records for comparing
			BSTR objRecord = objsIterator.hasNext() ? objsIterator.next() : null;
			BSTR termRecord = termsIterator.hasNext() ? termsIterator.next() : null;
			while (!BSTR.isEqual(objRecord, termRecord)) {
				// We add a column if we find an object that is not registered;
				// We remove a column if we find a registering of a non-existing object.
				boolean bAddColumn = 
						termRecord == null ||
						(objRecord != null && BSTR.compare(objRecord, termRecord) < 0);
				// Parsing the record
				String extraObject = (bAddColumn ? objRecord : termRecord).toString();
				
				// Parse shard number (possibly 0)
				int shardNo = 0;
				int slashIndex = 0;
				if (tabDef.isSharded()) {
					slashIndex = extraObject.indexOf('/') + 1;
					shardNo = Integer.parseInt(extraObject.substring(0, slashIndex - 1));
				}
				
				// Extract object ID.
				String objectID = extraObject.substring(slashIndex);
				
				// Construct a terms table row key
				String rowKey =
						(shardNo == 0 ? "" : shardNo + "/") + Defs.ALL_OBJECTS_ROW_KEY;
				// Add or remove the object registering column.
				if (bAddColumn) {
					objRecord = objsIterator.hasNext() ? objsIterator.next() : null;
					if (checkObjectExists(tabDef, shardNo, objectID)) {
						addColumn(objectID, termsTable, rowKey);
					}
				} else {
					if (!checkObjectExists(tabDef, shardNo, objectID)) {
						removeColumn(objectID, termsTable, rowKey);
					}
					termRecord = termsIterator.hasNext() ? termsIterator.next() : null;
				}
			}
		}
		
		objsRegSet.delete();
		allObjsRegSet.delete();
	}
	
	/**
	 * Compares 2 big sets that contain information about fields or terms in the whole table.
	 * The structure of records is as following:
	 * &lt;prefix&gt;&lt;field&gt;&lt;term&gt; where
	 * <ul><li>prefix - empty for non-sharded tables; &lt;shard&gt;/ for sharded tables
	 * where &lt;shard&gt; is the number of object shard where the term appeared;
	 * <li>&lt;field&gt; - field name;
	 * <li>&lt;term&gt; - /term in case of the term record or empty in case of field record.
	 * </ul>
	 * @param tabDef
	 */
	private void compareAndImproveFieldReg(TableDefinition tabDef, BigSet termsSet, BigSet termsRegSet) {
		String termsTable = SpiderService.termsStoreName(tabDef);
		
		Iterator<BSTR> objsIterator = termsSet.iterator();
		Iterator<BSTR> termsIterator = termsRegSet.iterator();
		
		while (objsIterator.hasNext() || termsIterator.hasNext()) {
			// Extracting records for comparing
			BSTR objRecord = objsIterator.hasNext() ? objsIterator.next() : null;
			BSTR termRecord = termsIterator.hasNext() ? termsIterator.next() : null;
			while (!BSTR.isEqual(objRecord, termRecord)) {
				// We add a column if we found a field or term that are not registered.
				// We remove a column if we found a non-existent registered field or term.
				boolean bAddColumn = 
						termRecord == null ||
						(objRecord != null && BSTR.compare(objRecord, termRecord) < 0);
				
				// Parsing the record
				String record = (bAddColumn ? objRecord : termRecord).toString();
				
				// Parse shard number
				String shardPrefix = "";
				if (tabDef.isSharded()) {
					int slashIndex = record.indexOf('/');
					if (slashIndex >= 0) {
						int shardNo = Integer.parseInt(record.substring(0, slashIndex));
						if (shardNo > 0) {
							shardPrefix = record.substring(0, slashIndex + 1);
						}
						record = record.substring(slashIndex + 1);
					}
				}
				
				// Try to find term
				int slashIndex = record.indexOf('/');
				String columnName;
				String rowKey;
				if (slashIndex < 0) {
					// Field registering
					columnName = record;
					rowKey = Defs.FIELD_REGISTRY_ROW_KEY;
				} else {
					// Term registering
					columnName = record.substring(slashIndex + 1);
					rowKey = shardPrefix + Defs.TERMS_REGISTRY_ROW_PREFIX + "/" + record.substring(0, slashIndex);
				}
				
				// We add a column to the terms table if we found a term that is not registered;
				// We remove a column if we found a non-existing registered term.
				if (bAddColumn) {
					addColumn(columnName, termsTable, rowKey);
					objRecord = objsIterator.hasNext() ? objsIterator.next() : null;
				} else {
					removeColumn(columnName, termsTable, rowKey);
					termRecord = termsIterator.hasNext() ? termsIterator.next() : null;
				}
			}
		}
		
		termsSet.delete();
		termsRegSet.delete();
	}
	
	/**
	 * Compares 2 big sets of links and remove the "hanged" links.
	 * Link record has the following structure.
	 * &lt;shard&gt;/&lt;table&gt;/&lt;linkName&gt;/&lt;objectFrom&gt;/&lt;objectTo&gt; where
	 * <ul><li>&lt;shard&gt; - link shard number (0 if the link is not sharded);
	 * <li>&lt;table&gt; - table name where the link resides;
	 * <li>&lt;linkName&gt; - link field name;
	 * <li>&lt;objectFrom&gt; - object ID that contains the link;
	 * <li>&lt;objectTo&gt; - object ID that the link points to.
	 * </ul> 
	 * @throws IOException
	 */
	private void compareAndImproveLinks(BigSet dirLinksSet, BigSet revLinksSet) {
		Iterator<BSTR> dirIterator = dirLinksSet.iterator();
		Iterator<BSTR> revIterator = revLinksSet.iterator();
		
		while (dirIterator.hasNext() || revIterator.hasNext()) {
			// Extracting records for comparing
			BSTR dirRecord = dirIterator.hasNext() ? dirIterator.next() : null;
			BSTR revRecord = revIterator.hasNext() ? revIterator.next() : null;
			while (!BSTR.isEqual(dirRecord, revRecord)) {
				if (revRecord != null &&
						(dirRecord == null || BSTR.compare(revRecord, dirRecord) < 0)) {
					revRecord = revIterator.hasNext() ? revIterator.next() : null;
					continue;
				}
				
				// Parse the record
				String record = dirRecord.toString();
				
				// Extract shard number;
				int ind1 = record.indexOf('/') + 1;
				int shardNo = Integer.parseInt(record.substring(0, ind1 - 1));
				
				// Extract table name;
				int ind2 = record.indexOf('/', ind1) + 1;
				String tabName = record.substring(ind1, ind2 - 1);
				TableDefinition tabDef = m_appDef.getTableDef(tabName);
				
				// Extract link field name;
				ind1 = ind2;
				ind2 = record.indexOf('/', ind1) + 1;
				String linkName = record.substring(ind1, ind2 - 1);
				FieldDefinition linkDef = tabDef.getFieldDef(linkName);
				
				// Extract object ID (from)
				ind1 = ind2; 
				ind2 = record.indexOf('/', ind1) + 1;
				String linkFrom = record.substring(ind1, ind2 - 1);
				
				// Extract objectID (to)
				String linkTo = record.substring(ind2);
				
				dirRecord = dirIterator.hasNext() ? dirIterator.next() : null;
				if (checkReversedLink(tabDef, linkDef, shardNo, linkFrom, linkTo)) {
					// Actually link is correct
					continue;
				}
				
				// Remove the "hanged" link from the object table or terms table
				// depending on shard number (whether the link is sharded or not).
				if (shardNo == 0) {
					String columnName = new Link(linkDef, linkTo).getColumnName();
					removeColumn(columnName, SpiderService.objectsStoreName(tabDef), linkFrom);
				} else {
					String linkBytes = new Link(linkDef, linkFrom).getColumnName();
					removeColumn(linkTo, SpiderService.termsStoreName(tabDef), shardNo + "/" + linkBytes);
				}
			}
		}
		
		dirLinksSet.delete();
		revLinksSet.delete();
	}
	
	/**
	 * Table processing for checking links only.
	 * @param tabDef		Table definition
	 */
	private void checkLinkedTable(TableDefinition tabDef) {
		m_logger.info(tabDef + " processing");
		
		String objectTable = SpiderService.objectsStoreName(tabDef);
		String termsTable = SpiderService.termsStoreName(tabDef);
		
		// Check whether a table has sharded links
		boolean hasShardedLinks = false;
		for (FieldDefinition fieldDef : tabDef.getFieldDefinitions()) {
			if (fieldDef.isLinkField() && fieldDef.isSharded()) {
				hasShardedLinks = true;
				break;
			}
		}
		
		// Object table scanning; only link fields are processed
		Iterator<DRow> iRows = m_dbService.getAllRowsAllColumns(objectTable);
		while (iRows.hasNext()) {
			if (isInterrupted()) {
				return;
			}
			
			DRow row = iRows.next();
			
			boolean sharded = tabDef.isSharded();
			
			String objectID = row.getKey();
			String shardPrefix = "";

			// Calculate shard number for sharded tables
			if (sharded) {
				String shardDate = m_dbService.getColumn(objectTable, objectID, tabDef.getShardingField().getName()).getValue();
				int shardNo = tabDef.computeShardNumber(Utils.dateFromString(shardDate));
				// shard prefix always exists for sharded tables
				shardPrefix = shardNo + "/";
			}
				
			Iterator<DColumn> iColumns = row.getColumns();
			while (iColumns.hasNext()) {
				DColumn column = iColumns.next();
				String columnName = column.getName();
				if (columnName.charAt(0) == '~') {
					// We process link fields only
					collectLinkColumn(tabDef, shardPrefix, objectID, columnName);
				}
			}	// iColumns
		}	// iRows
		
		// It is no sense to scan terms table for links if there are no sharded links.
		if (!hasShardedLinks) {
			return;
		}
		
		// Terms table scanning
		Iterator<DRow> itRows = m_dbService.getAllRowsAllColumns(termsTable);
		while (itRows.hasNext()) {
			if (isInterrupted()) {
				return;
			}
			
			DRow row = itRows.next();
			
			// Sharded links row information
			TermsRowKey rowKey = new TermsRowKey(tabDef, row.getKey());
			
			// Additional information about the row
			int shardNo = 0;					// shard no of the object
			TableDefinition rTabDef = null;		// reversed link table name 
			FieldDefinition rLinkDef = null;	// reversed link field name

			if (rowKey.getObjectId() != null) {
				// Sharded links row; calculate additional information
				if (tabDef.isSharded()) {
					String shardDate = m_dbService.getColumn(objectTable, rowKey.getObjectId(), tabDef.getShardingField().getName()).getValue();
					shardNo = tabDef.computeShardNumber(Utils.dateFromString(shardDate));
				}
				rTabDef = tabDef.getAppDef().getTableDef(rowKey.getFieldDef().getLinkExtent());
				rLinkDef = rTabDef.getFieldDef(rowKey.getFieldDef().getLinkInverse());
			}

			Iterator<DColumn> itColumns = row.getColumns();
			while (itColumns.hasNext()) {
				DColumn column = itColumns.next();
				String columnName = column.getName();
				
				// Store link info and reversed link info.
				String prefix = rowKey.m_shardPrefix + tabDef.getTableName() + "/" + rowKey.getFieldDef().getName() + "/";
				m_dirLinksSet.add(prefix + rowKey.getObjectId() + "/" + columnName);
				
				String rShardPrefix = rLinkDef.isSharded() ? shardNo + "/" : "0/";
				m_revLinksSet.add(rShardPrefix + rTabDef.getTableName() + "/" + 
						rLinkDef.getName() + "/" + columnName + "/" + rowKey.getObjectId());
			}
		};
	}
	
	/**
	 * Parses link column and saves info about this link.
	 * @param tabDef		Table where the link resides;
	 * @param shardPrefix	Shard prefix for sharded links
	 * @param linkFrom		Object ID - source of the link
	 * @param linkTo		Object ID - destination of the link
	 */
	private void collectLinkColumn(TableDefinition tabDef, String shardPrefix, String linkFrom, String linkTo) {
		// Save direct link info: 0/tableName/linkName/linkFrom/linkTo
		Link dirLink = new Link(tabDef, linkTo);
		String dirPrefix = "0/" + tabDef.getTableName() + "/" + dirLink.fieldDef.getName() + "/";
		m_dirLinksSet.add(dirPrefix + linkFrom + "/" + dirLink.objectId);
		
		// Check whether reversed link table name is already added for checks
		String rTabName = dirLink.fieldDef.getLinkExtent();
		if (!m_tables.contains(rTabName)) {
			m_linkedTables.add(rTabName);
		}
		
		// Save reversed link info
		String rLinkName = dirLink.fieldDef.getLinkInverse();
		FieldDefinition rLink = tabDef.getAppDef().getTableDef(rTabName).getFieldDef(rLinkName);
		String rShardPrefix = rLink.isSharded() ? shardPrefix : "0/";
		m_revLinksSet.add(rShardPrefix + rTabName + "/" + rLinkName + "/" + 
				dirLink.objectId + "/" + linkFrom);
	}
	
	/**
	 * Computes shard number for a given object in a given table.
	 * 
	 * @param tabDef	Table that contains the object
	 * @param objectID	Object ID
	 * @return			shard number or 0 if the table is not sharded.
	 */
	private int getObjectShardNo(TableDefinition tabDef, String objectID) {
		if (tabDef.isSharded()) {
			String shardValue = m_dbService.getColumn(SpiderService.objectsStoreName(tabDef), 
					objectID, tabDef.getShardingField().getName()).getValue();
			if (shardValue == null) {
				return 0;
			} else {
				return tabDef.computeShardNumber(
						Utils.dateFromString(shardValue));
			}
		} else {
			return 0;
		}
	}	// getObjectShardNo
	
	/**
	 * Checks whether an object has a field with a given term.
	 * Even if the term exists but the object doesn't belong to expected shard,
	 * the function returns false (to remove error in term positioning).
	 * 
	 * @param tabDef	Table to extract object from
	 * @param shardNo	Expected shard number of the object
	 * @param objectID	Object ID
	 * @param fieldName	Field name
	 * @param term		Term to check
	 * @return
	 */
	private boolean checkTerm(TableDefinition tabDef, int shardNo, String objectID, String fieldName, String term) {
		String value = m_dbService.getColumn(
				SpiderService.objectsStoreName(tabDef), objectID, fieldName).getValue();
		if (value == null) {
			return false;
		}
		if (shardNo != getObjectShardNo(tabDef, objectID)) {
			return false;
		}
		Set<String> fieldTerms = SpiderHelper.getTerms(fieldName, value, tabDef);
		return fieldTerms != null && fieldTerms.contains(term);
	}
	
	/**
	 * Checks whether an object exists and belongs to expected shard.
	 * 
	 * @param tabDef	Table that contains a given object
	 * @param shardNo	Expected shard number
	 * @param objectID	Object ID
	 * @return
	 */
	private boolean checkObjectExists(TableDefinition tabDef, int shardNo, String objectID) {
		String idValue = m_dbService.getColumn(
				SpiderService.objectsStoreName(tabDef), objectID, CommonDefs.ID_FIELD).getValue();
		if (idValue == null) {
			return false;
		}
		return shardNo == getObjectShardNo(tabDef, objectID);
	}
	
	private void addColumn(String columnName, String table, String rowKey) {
		String message = "Integrity error improvement: set " + table +
				"[\'" + rowKey + "\'][\'" + columnName + "\']=\'\';";
		m_logger.info(message);
		DBTransaction transaction = m_dbService.startTransaction();
		transaction.addColumn(table, rowKey, columnName, new byte[0]);
		m_dbService.commit(transaction);
	}
	
	private void removeColumn(String columnName, String table, String rowKey) {
		String message = "Integrity error improvement: del " + table +
				"[\'" + rowKey + "\'][\'" + columnName + "\'];";
		m_logger.info(message);
		DBTransaction transaction = m_dbService.startTransaction();
		transaction.deleteColumn(table, rowKey, columnName);
		m_dbService.commit(transaction);
	}
	
	/**
	 * Checks whether a link has a correct inverted link.
	 * 
	 * @param tabFrom		Table where the source link resides.
	 * @param linkFrom		Link field
	 * @param shardFrom		In case of sharded link - the shard of the link (linked object)
	 * @param objectFrom	Object ID from where the link points
	 * @param objectTo		Object ID where the link points to.
	 * @return
	 */
	private boolean checkReversedLink(
			TableDefinition tabFrom, FieldDefinition linkFrom, int shardFrom,
			String objectFrom, String objectTo) {
		ApplicationDefinition appDef = tabFrom.getAppDef();
		TableDefinition tabTo = appDef.getTableDef(linkFrom.getLinkExtent());
		if (tabTo == null) return false;
		FieldDefinition linkTo = tabTo.getFieldDef(linkFrom.getLinkInverse());
		if (linkTo == null) return false;
		int shardTo = linkTo.isSharded() ? getObjectShardNo(tabFrom, objectFrom) : 0;
		if (shardTo == 0) {
			DColumn column = m_dbService.getColumn(
					SpiderService.objectsStoreName(tabTo), objectTo,
					new Link(linkTo, objectFrom).getColumnName());
			return column != null && !Utils.isEmpty(column.getValue());
		} else {
			DColumn column = m_dbService.getColumn(
					SpiderService.termsStoreName(tabTo),
					shardTo + "/" + new Link(linkTo, objectTo).getColumnName(),
					objectFrom);
			return column != null && !Utils.isEmpty(column.getValue());
		}
	}
}
