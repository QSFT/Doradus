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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.core.ObjectID;
import com.dell.doradus.fieldanalyzer.FieldAnalyzer;
import com.dell.doradus.fieldanalyzer.NullAnalyzer;
import com.dell.doradus.fieldanalyzer.TextAnalyzer;
import com.dell.doradus.search.IDHelper;
import com.dell.doradus.search.util.HeapSet;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.DColumn;
import com.dell.doradus.service.db.DRow;
import com.google.common.collect.Lists;

/**
 * A collection of helper methods for retrieving information from Cassandra database
 * using Spider storage model.
 */
public class SpiderHelper {
    public static final byte[] EMPTY_BYTES = new byte[0];
	
    ///////////////////////////
    // PRIVATE HELPER FUNCTIONS
    ///////////////////////////

	/**
	 * Converting a collection of ObjectIDs to list of strings representing these IDs.
	 * 
	 * @param ids	Collection of IDs
	 * @return		List of corresponding strings
	 */
	private static List<String> objectsToStrings(Collection<ObjectID> ids) {
		List<String> keys = new ArrayList<>(ids.size());
		for(ObjectID id: ids) {
			keys.add(IDHelper.IDToString(id));
		}
		return keys;
	}
	
	/**
	 * Lists union until a given count elements is reached. Guarantees that
	 * minimal elements of the lists would be added to the resulting list.
	 * 
	 * @param collection	Source lists collection
	 * @param count			Number of elements limit
	 * @return
	 */
	private static <T extends Comparable<T>> List<T>
			unionUnique(List<List<T>> collection, int count) {
		HeapSet<T> hl = new HeapSet<T>(count);
		for(List<T> lst : collection) {
			for(T v : lst) hl.Put(v);
		}
		List<T> result = hl.GetValues();
		return result;
	}

	/**
	 * Calculation of a first link value based on a start link object.
	 * 
	 * @param linkDef			Link field definition
	 * @param continuationLink	Starting link or null for starting from the very beginning
	 * @param inclusive			Initial search?
	 * @return					First link value or the next one if inclusive == false.
	 */
	private static String fromLinksStart(FieldDefinition linkDef, ObjectID continuationLink, boolean inclusive) {
		byte[] start = null;
		if(continuationLink == null) {
			start = IDHelper.linkBoundMinimum(linkDef);
		} else {
			start = IDHelper.linkToBytes(linkDef, continuationLink);
			if (!inclusive) {
				// shift to a next value
				start = IDHelper.next(start);
			}
		}
		return Utils.toString(start);
	}
	
	/**
	 * Calculation of a last link value based on a link field definition.
	 * 
	 * @param linkDef	Link field definition
	 * @return			Last link value
	 */
	private static String fromLinksFinish(FieldDefinition linkDef) {
		return Utils.toString(IDHelper.linkBoundMaximum(linkDef));
	}

	/**
	 * Calculation of a first term value based on a start link object.
	 * 
	 * @param continuationObject	First term object
	 * @param inclusive				Is it initial search?
	 * @return						First term value.
	 */
	private static String fromTerms(ObjectID continuationObject, boolean inclusive) {
        byte[] start = EMPTY_BYTES;
        if (continuationObject != null) {
        	start = IDHelper.idToBytes(continuationObject);
        	if (!inclusive) {
        		start = IDHelper.next(start);
        	}
        }
        return Utils.toString(start);
	}

	/**
	 * Generating an object link row key for sharded links.
	 * 
	 * @param shard	Shard number (not null)
	 * @param link	Link field definition
	 * @param id	Object ID
	 * @return		Object links row key generated
	 */
	private static String linkKey(Integer shard, FieldDefinition link, ObjectID id) {
		if (id == null) {
			id = ObjectID.EMPTY;
		}
		return shard.toString() + "/~" + link.getName() + "/" + IDHelper.IDToString(id);
	}
	
	/**
	 * Extracting an object ID from a row key.
	 * 
	 * @param shard	Shard number (not null)
	 * @param link	Link field definition
	 * @param key	Source key.
	 * @return		Object ID.
	 */
	private static ObjectID unlinkKey(Integer shard, FieldDefinition link, String key) {
		String prefix = shard.toString() + "/~" + link.getName() + "/";
		assert key.startsWith(prefix);
		return IDHelper.createID(key.substring(prefix.length()));
	}

	/**
	 * Converting a collection of object IDs to a list of row keys.
	 * 
	 * @param shard	Shard number (not null)
	 * @param link	Link field definition
	 * @param ids	Collection of object IDs
	 * @return		List of corresponding row keys
	 */
	private static List<String> linkKeys(Integer shard, FieldDefinition link, Collection<ObjectID> ids) {
		List<String> keys = new ArrayList<String>(ids.size());
		for (ObjectID id : ids) {
			keys.add(shard.toString() + "/~" + link.getName() + "/" + IDHelper.IDToString(id));
		}
		return keys;
	}
	
	/**
	 * Generating of a row key for a terms table for a sharded table.
	 * 
	 * @param shard	Shard number (not null)
	 * @param field	Field name
	 * @return		Row key of all the terms in the given shard
	 */
	private static String termKey(Integer shard, String field) {
		return shard.toString() + "/_terms/" + field;
	}

	/**
	 * Add the scalar value in the given DColumn to the given map. The column is converted
	 * from binary to String form based on its field definition, if any.
	 * 
	 * @param tableDef     Table that owns scalar field.
	 * @param scalarMap    Map to add scalar name/value pair to.
	 * @param column       DColumn retrieved from object row.
	 */
    private static void addScalarToMap(TableDefinition tableDef, Map<String, String> scalarMap, DColumn column) {
        String fieldName = column.getName();
        FieldDefinition fieldDef = tableDef.getFieldDef(fieldName);
        if (fieldDef != null && fieldDef.isBinaryField()) {
            scalarMap.put(fieldName, fieldDef.getEncoding().encode(column.getRawValue()));
        } else {
            scalarMap.put(fieldName, column.getValue());
        }
    }
    
    ///////////////////
    // SCALAR VALUES
    ///////////////////

	public static Map<ObjectID, Map<String, String>> getScalarValues(TableDefinition tableDef,
			Collection<ObjectID> ids, String continuationField, int count) {
		DBService dbService = DBService.instance();
        Map<ObjectID, Map<String, String>> result = new HashMap<>();
        
		if (continuationField == null && tableDef.isSharded()) {
			continuationField = new String(new char[]{ '!' + 1 }); // next char after ! to skip shards
		} else if (continuationField == null) {
			continuationField = "";
		}
		String tableName = SpiderService.objectsStoreName(tableDef);
		Collection<String> rowKeys = objectsToStrings(ids);
        Iterator<DRow> iRows = dbService.getRowsColumnSlice(tableName, rowKeys, continuationField, "~");
        while (iRows.hasNext()) {
        	DRow row = iRows.next();
        	Map<String, String> scalarValues = new HashMap<>();
        	result.put(IDHelper.createID(row.getKey()), scalarValues);
        	Iterator<DColumn> iColumns = row.getColumns();
        	for (int i = 0; i < count && iColumns.hasNext(); ++i) {
        		DColumn column = iColumns.next();
        		addScalarToMap(tableDef, scalarValues, column);
        	}
        }
        return result;
	}

    public static Map<String, String> getScalarValues(TableDefinition tableDef,
    		ObjectID id, String continuationField, int count) {
		DBService dbService = DBService.instance();
        
		if (continuationField == null && tableDef.isSharded()) {
			continuationField = new String(new char[] { '!' + 1 }); // next char after ! to skip shards
		} else if (continuationField == null) {
			continuationField = "";
		}
		String tableName = SpiderService.objectsStoreName(tableDef);
		Iterator<DColumn> iColumns = dbService.getColumnSlice(tableName, IDHelper.IDToString(id), continuationField, "~");
    	Map<String, String> result = new HashMap<>();
    	for (int i = 0; i < count && iColumns.hasNext(); ++i) {
    		DColumn column = iColumns.next();
    		result.put(column.getName(), column.getValue());
    	}
        return result;
    }
	
	public static Map<ObjectID, Map<String, String>> getScalarValues(TableDefinition tableDef,
			Collection<ObjectID> ids, Collection<String> fields) {
		DBService dbService = DBService.instance();
        Map<ObjectID, Map<String, String>> result = new HashMap<>();
        
		String tableName = SpiderService.objectsStoreName(tableDef);
		Collection<String> rowKeys = objectsToStrings(ids);
        Iterator<DRow> iRows = dbService.getRowsColumns(tableName, rowKeys, fields);
        while (iRows.hasNext()) {
        	DRow row = iRows.next();
        	Map<String, String> scalarValues = new HashMap<>();
        	result.put(IDHelper.createID(row.getKey()), scalarValues);
        	Iterator<DColumn> iColumns = row.getColumns();
        	while (iColumns.hasNext()) {
        		DColumn column = iColumns.next();
        		addScalarToMap(tableDef, scalarValues, column);
        	}
        }
        return result;
	}
	
    public static Map<String, String> getScalarValues(TableDefinition tableDef,
    		ObjectID id, Collection<String> fields) {
		DBService dbService = DBService.instance();
        Map<String, String> result = new HashMap<>();
        
		String tableName = SpiderService.objectsStoreName(tableDef);
        Iterator<DRow> iRows = dbService.getRowsColumns(tableName, Arrays.asList(IDHelper.IDToString(id)), fields);
        if (iRows.hasNext()) {
        	Iterator<DColumn> iColumns = iRows.next().getColumns();
        	while (iColumns.hasNext()) {
        		DColumn column = iColumns.next();
        		result.put(column.getName(), column.getValue());
        	}
        }
        return result;
    }
    
    public static String fetchScalarFieldValue(TableDefinition tableDef, ObjectID id, String field) {
    	List<String> fields = new ArrayList<String>(1);
    	fields.add(field);
    	Map<String, String> result = getScalarValues(tableDef, id, fields);
    	return result.get(field);
    }
    
    ///////////////////
    // LINKS
    ///////////////////

    public static List<ObjectID> getLinks(FieldDefinition linkDef,
    		ObjectID id, ObjectID continuationLink, boolean inclusive, int count) {
    	return getLinks(linkDef, (List<Integer>)null, id, continuationLink, inclusive, count);
    }

	public static Map<ObjectID, List<ObjectID>> getLinks(FieldDefinition linkDef,
			Collection<ObjectID> ids, ObjectID continuationLink, boolean inclusive, int count) {
		return getLinks(linkDef, (List<Integer>)null, ids, continuationLink, inclusive, count);
	}
    
	public static Map<ObjectID, List<ObjectID>> getLinksUnsharded(FieldDefinition linkDef,
			Collection<ObjectID> ids, ObjectID continuationLink, boolean inclusive, int count) {
		DBService dbService = DBService.instance();
		String tableName = SpiderService.objectsStoreName(linkDef.getTableDef());
		Map<ObjectID, List<ObjectID>> result = new HashMap<>();

		String start = fromLinksStart(linkDef, continuationLink, inclusive);
		String finish = fromLinksFinish(linkDef);
        List<String> keys = objectsToStrings(ids);
        Iterator<DRow> iRows = dbService.getRowsColumnSlice(tableName, keys, start, finish);
        while (iRows.hasNext()) {
        	DRow row = iRows.next();
        	List<ObjectID> list = new ArrayList<>();
        	result.put(IDHelper.createID(row.getKey()), list);
        	Iterator<DColumn> iColumns = row.getColumns();
        	for (int i = 0; i < count && iColumns.hasNext(); ++i) {
        		list.add(IDHelper.linkValueToId(Utils.toBytes(iColumns.next().getName())));
        	}
        }
        return result;
	}
	
    public static List<ObjectID> getLinksUnsharded(FieldDefinition linkDef,
    		ObjectID id, ObjectID continuationLink, boolean inclusive, int count) {
		DBService dbService = DBService.instance();
		String tableName = SpiderService.objectsStoreName(linkDef.getTableDef());
		List<ObjectID> result = new ArrayList<>();
		
		String start = fromLinksStart(linkDef, continuationLink, inclusive);
		String finish = fromLinksFinish(linkDef);
        Iterator<DColumn> iColumns = dbService.getColumnSlice(tableName, IDHelper.IDToString(id), start, finish);
        for (int i = 0; i < count && iColumns.hasNext(); ++i) {
        	result.add(IDHelper.linkValueToId(Utils.toBytes(iColumns.next().getName())));
        }
        return result;
    }

	public static Map<ObjectID, List<ObjectID>> getLinks(FieldDefinition linkDef, Integer shard,
			Collection<ObjectID> ids, ObjectID continuationLink, boolean inclusive, int count) {
    	if (shard.intValue() == 0) {
    		return getLinksUnsharded(linkDef, ids, continuationLink, inclusive, count);
    	}
    	
    	DBService dbService = DBService.instance();
		String tableName = SpiderService.termsStoreName(linkDef.getTableDef());
		Map<ObjectID, List<ObjectID>> result = new HashMap<>();
		
		String startCol = fromTerms(continuationLink, inclusive);
		Iterator<DRow> iRows = dbService.getRowsColumnSlice(tableName, linkKeys(shard, linkDef, ids), startCol, "");
		while (iRows.hasNext()) {
			DRow row = iRows.next();
			List<ObjectID> list = new ArrayList<>();
			result.put(unlinkKey(shard, linkDef, row.getKey()), list);
			Iterator<DColumn> iColumns = row.getColumns();
			for (int i = 0; i < count && iColumns.hasNext(); ++i) {
				list.add(IDHelper.createID(iColumns.next().getName()));
			}
		}
        return result;
	}
    
    public static List<ObjectID> getLinks(FieldDefinition linkDef, Integer shard,
    		ObjectID id, ObjectID continuationLink, boolean inclusive, int count) {
    	if (shard.intValue() == 0) {
    		return getLinksUnsharded(linkDef, id, continuationLink, inclusive, count);
    	}
    	
    	DBService dbService = DBService.instance();
		String tableName = SpiderService.termsStoreName(linkDef.getTableDef());
		List<ObjectID> result = new ArrayList<>();

		String startCol = fromTerms(continuationLink, inclusive);
        String key = linkKey(shard, linkDef, id);
		Iterator<DColumn> iColumns = dbService.getColumnSlice(tableName, key, startCol, "");
		for (int i = 0; i < count && iColumns.hasNext(); ++i) {
			result.add(IDHelper.createID(iColumns.next().getName()));
		}
        return result;
    }

	public static Map<ObjectID, List<ObjectID>> getLinks(FieldDefinition linkDef, Collection<Integer> shards,
			Collection<ObjectID> ids, ObjectID continuationLink, boolean inclusive, int count) {
    	if (!linkDef.isSharded()) {
    		return getLinksUnsharded(linkDef, ids, continuationLink, inclusive, count);
    	}
    	ApplicationDefinition app = linkDef.getTableDef().getAppDef();
    	TableDefinition extent = app.getTableDef(linkDef.getLinkExtent());
    	if (shards == null) {
    		shards = SpiderHelper.getShards(extent);
    	}
    	if (shards.size() == 1) {
    		return getLinks(linkDef, shards.toArray(new Integer[1])[0], ids, continuationLink, inclusive, count);
    	}
    	
    	Map<ObjectID, List<List<ObjectID>>> values = new HashMap<ObjectID, List<List<ObjectID>>>(ids.size());
    	for (Integer shard : shards) {
    		Map<ObjectID, List<ObjectID>> res = getLinks(linkDef, shard, ids, continuationLink, inclusive, count);
    		for (Map.Entry<ObjectID, List<ObjectID>> entry : res.entrySet()) {
    			if (values.containsKey(entry.getKey())) {
    				values.get(entry.getKey()).add(entry.getValue());
    			} else {
    				List<List<ObjectID>> lst = new ArrayList<List<ObjectID>>(shards.size());
    				lst.add(entry.getValue());
    				values.put(entry.getKey(), lst);
    			}
    		}
    	}
    	Map<ObjectID, List<ObjectID>> result = new HashMap<ObjectID, List<ObjectID>>(ids.size());
		for (Map.Entry<ObjectID, List<List<ObjectID>>> entry : values.entrySet()) {
			Set<ObjectID> set = new HashSet<>();
			int i = 0;
			extLoop: for (List<ObjectID> list : entry.getValue()) {
				for (ObjectID id : list) {
					if (set.add(id)) {
						if (++i >= count) {
							break extLoop;
						}
					}
				}
			}
			result.put(entry.getKey(), Lists.newArrayList(set));
		}
		return result;
	}
	
    public static List<ObjectID> getLinks(FieldDefinition linkDef, Collection<Integer> shards,
    		ObjectID id, ObjectID continuationLink, boolean inclusive, int count) {
    	if (!linkDef.isSharded()) {
    		return getLinksUnsharded(linkDef, id, continuationLink, inclusive, count);
    	}
    	ApplicationDefinition app = linkDef.getTableDef().getAppDef();
    	TableDefinition extent = app.getTableDef(linkDef.getLinkExtent());
    	if (shards == null) {
    		shards = SpiderHelper.getShards(extent);
    	}
    	List<List<ObjectID>> values = new ArrayList<List<ObjectID>>(shards.size());
    	for(Integer shard: shards) {
    		values.add(getLinks(linkDef, shard, id, continuationLink, inclusive, count));
    	}
    	return unionUnique(values, count);
    }
    
    ///////////////////
    // TERMS
    ///////////////////

    public static List<String> getTerms(TableDefinition tableDef, String field, String prefix, int count) {
    	return getTerms(tableDef, (List<Integer>)null, field, prefix, count);
    }
    
    public static List<String> getTermsUnsharded(TableDefinition tableDef, String field, String prefix, int count) {
    	DBService dbService = DBService.instance();
    	String termsStore = SpiderService.termsStoreName(tableDef);
    	if (prefix == null) prefix = "";
    	
		List<String> result = new ArrayList<String>();
		Iterator<DColumn> iColumns = dbService.getColumnSlice(
				termsStore, "_terms/" + field, prefix, prefix + Character.MAX_VALUE);
		for (int i = 0; i < count && iColumns.hasNext(); ++i) {
			result.add(iColumns.next().getName());
		}
    	return result;
    }

    public static List<String> getTermsUnsharded(TableDefinition tableDef, String field, String from, String to, int count, boolean reversed) {
    	DBService dbService = DBService.instance();
    	String termsStore = SpiderService.termsStoreName(tableDef);
    	if (from == null) from = "";
    	if (to == null) to = "";
    	
		List<String> result = new ArrayList<String>();
		Iterator<DColumn> iColumns = dbService.getColumnSlice(
				termsStore, "_terms/" + field, from, to, reversed);
    	for (int i = 0; i < count && iColumns.hasNext(); ++i) {
    		result.add(iColumns.next().getName());
    	}
    	return result;
    }
    
    public static List<String> getTerms(TableDefinition tableDef, Integer shard, String field, String prefix, int count) {
    	if (shard.intValue() == 0) {
    		return getTermsUnsharded(tableDef, field, prefix, count);
    	}
    	
    	DBService dbService = DBService.instance();
    	String termsStore = SpiderService.termsStoreName(tableDef);
    	if (prefix == null) prefix = "";
    	
		List<String> result = new ArrayList<String>();
		Iterator<DColumn> iColumns = dbService.getColumnSlice(
				termsStore, termKey(shard, field), prefix, prefix + Character.MAX_VALUE);
    	for (int i = 0; i < count && iColumns.hasNext(); ++i) {
    		result.add(iColumns.next().getName());
    	}
    	return result;
    }

    public static List<String> getTerms(TableDefinition tableDef, Integer shard, String field, String from, String to, int count, boolean reversed) {
    	if (shard.intValue() == 0) {
    		return getTermsUnsharded(tableDef, field, from, to, count, reversed);
    	}
    	
    	DBService dbService = DBService.instance();
    	String termsStore = SpiderService.termsStoreName(tableDef);
    	if (from == null) from = "";
    	if (to == null) to = "";
    	
		List<String> result = new ArrayList<String>();
		Iterator<DColumn> iColumns = dbService.getColumnSlice(
				termsStore, termKey(shard, field), from, to, reversed);
    	for (int i = 0; i < count && iColumns.hasNext(); ++i) {
    		result.add(iColumns.next().getName());
    	}
    	return result;
    }
    
    public static List<String> getTerms(TableDefinition tableDef, Collection<Integer> shards, String field, String prefix, int count) {
    	if (!tableDef.isSharded()) {
    		return getTermsUnsharded(tableDef, field, prefix, count);
    	}
    	if (shards == null) {
    		shards = SpiderHelper.getShards(tableDef);
    	}
    	List<List<String>> terms = new ArrayList<List<String>>(shards.size());
    	for(Integer shard: shards) {
    		terms.add(getTerms(tableDef, shard, field, prefix, count));
    	}
    	List<String> result = unionUnique(terms, count);
    	return result;
    }
    
    ///////////////////
    // COUNTERS
    ///////////////////
    
    public static List<String> getFields(TableDefinition tableDef) {
		List<String> result = new ArrayList<String>();
		String tableName = SpiderService.termsStoreName(tableDef);
		Iterator<DColumn> iColumns = DBService.instance().getAllColumns(tableName, "_fields");
		while (iColumns.hasNext()) {
			result.add(iColumns.next().getName());
		}
    	return result;
    }
    
    ///////////////////
    // TERMS
    ///////////////////

    public static List<ObjectID> getTermDocsUnsharded(TableDefinition tableDef,
    		String term, ObjectID continuationObject, boolean inclusive, int count) {
    	DBService dbService = DBService.instance();
    	String store = SpiderService.termsStoreName(tableDef);
    	List<ObjectID> result = new ArrayList<>();
    	
    	String startCol = continuationObject == null ? "" : IDHelper.IDToString(continuationObject);
    	if (!inclusive) {
    		startCol += (char)0;
    	}
    	Iterator<DColumn> iColumns = dbService.getColumnSlice(store, term, startCol, "");
    	for (int i = 0; i < count && iColumns.hasNext(); ++i) {
    		result.add(IDHelper.createID(iColumns.next().getName()));
    	}
	    return result;
    }

    public static Map<String, List<ObjectID>> getTermDocsUnsharded(TableDefinition tableDef,
    		Collection<String> terms, ObjectID continuationObject, boolean inclusive, int count) {
    	DBService dbService = DBService.instance();
    	String store = SpiderService.termsStoreName(tableDef);
    	Map<String, List<ObjectID>> result = new HashMap<>();
    	
    	String startCol = continuationObject == null ? "" : IDHelper.IDToString(continuationObject);
    	if (!inclusive) {
    		startCol += (char)0;
    	}

    	Iterator<DRow> iRows = dbService.getRowsColumnSlice(store, terms, startCol, "");
    	while (iRows.hasNext()) {
    		DRow row = iRows.next();
    		List<ObjectID> list = new ArrayList<>();
    		result.put(row.getKey(), list);
    		Iterator<DColumn> iColumns = row.getColumns();
    		for (int i = 0; i < count && iColumns.hasNext(); ++i) {
    			list.add(IDHelper.createID(iColumns.next().getName()));
    		}
    	}
        return result;
    }
    
    public static List<ObjectID> getTermDocs(TableDefinition tableDef, Integer shard,
    		String term, ObjectID continuationObject, boolean inclusive, int count) {
    	if (shard.intValue() == 0) {
    		return getTermDocsUnsharded(tableDef, term, continuationObject, inclusive, count);
    	}
    	return getTermDocsUnsharded(tableDef, shard.toString() + "/" + term, continuationObject, inclusive, count);
    }

    public static Map<String, List<ObjectID>> getTermDocs(TableDefinition tableDef, Integer shard,
    		Collection<String> terms, ObjectID continuationObject, boolean inclusive, int count) {
    	if (shard.intValue() == 0) {
    		return getTermDocsUnsharded(tableDef, terms, continuationObject, inclusive, count);
    	}
    	List<String> shardTerms = new ArrayList<String>(terms.size());
    	String prefix = shard.toString() + "/";
    	for(String term : terms) shardTerms.add(prefix + term);
    	Map<String, List<ObjectID>> result = getTermDocsUnsharded(tableDef, shardTerms, continuationObject, inclusive, count);
    	Map<String, List<ObjectID>> result2 = new HashMap<String, List<ObjectID>>(result.size());
    	for(Map.Entry<String, List<ObjectID>> e : result.entrySet()) {
    		if (e.getKey().startsWith(prefix)) {
    			result2.put(e.getKey().substring(prefix.length()), e.getValue());
    		} else {
    			result2.put(e.getKey(), e.getValue());
    		}
    	}
    	return result2;
    }
    
    public static Set<Integer> getShards(TableDefinition tableDef) {
    	Set<Integer> results = new HashSet<Integer>(SpiderService.instance().getShards(tableDef).keySet());
    	results.add(0);
    	return results;
    }

	/**
	 * Produces the set of terms of a given field value
	 * @param fieldName		Scalar field name
	 * @param fieldValue	Field value
	 * @param tabDef		Table definition
	 * @return
	 */
	public static Set<String> getTerms(String fieldName, String fieldValue, TableDefinition tabDef) {
		FieldAnalyzer analyzer = TextAnalyzer.instance();	// default analyzer
		FieldDefinition fieldDef = tabDef.getFieldDef(fieldName);
		if (fieldDef != null) {
			// Extract analyzer from the field definition
			FieldAnalyzer fieldAnalyzer = FieldAnalyzer.findAnalyzer(fieldDef);
			if (fieldAnalyzer == NullAnalyzer.instance()) {
				// No terms produced
				return null;
			} else if (fieldAnalyzer != null) {
				analyzer = fieldAnalyzer;
			}
		}
		try {
			return analyzer.extractTerms(fieldValue);
		} catch (IllegalArgumentException e) {
			return new HashSet<String>();
		}
	}
}
