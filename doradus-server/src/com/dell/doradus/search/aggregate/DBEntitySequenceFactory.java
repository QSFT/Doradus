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

package com.dell.doradus.search.aggregate;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.SlicePredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.common.DBObject;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.core.ObjectID;
import com.dell.doradus.search.IDHelper;
import com.dell.doradus.search.util.LRUCache;
import com.dell.doradus.service.spider.SpiderHelper;
import com.dell.doradus.utilities.Timer;
import com.dell.doradus.utilities.TimerGroup;


/**
 * DBEntitySequenceFactory returns the sequences of entities that can be iterated.
 * The factory pre-fetches the scalar and link values of set of entities.
 */
public class DBEntitySequenceFactory implements EntitySequenceFactory {
    static final String ALLSCALARMARK = "*";
    private static final List<String> ALLSCALARFIELDS = new ArrayList<String>(java.util.Arrays.asList(ALLSCALARMARK));
	private Logger log = LoggerFactory.getLogger(DBEntitySequenceFactory.class.getSimpleName());
	TimerGroup timers = new TimerGroup(DBEntitySequenceFactory.class.getSimpleName() + ".timing", 10 * 1000000000l);

	//Cassandra.Client m_client;
	// Map key is "link(field1,filed2,...)"
	private Map<String, LRUCache<ObjectID, LinkList>> m_linkCache;
	// Map key is "table(field1,filed2,...)"
	private Map<String, LRUCache<ObjectID, Map<String, String>>> m_scalarCache;
	// Map key is "table.link"
	private Map<String, LRUCache<String, LinkList>> m_continuationlinkCache;
	private int m_scalarCacheCapacity;
	private int m_linkCacheCapacity;
	private int m_continuationlinkCacheCapacity;
	private DBEntitySequenceOptions m_options;

	/**
	 * Creates the factory with default {@link DBEntitySequenceOptions} options,
	 * the scalar and link caches for 10000 entries of every category.
	 */
	public DBEntitySequenceFactory() {
	    this(10000, 10000, 10000, DBEntitySequenceOptions.defaultOptions);
	}

	/**
	 * Creates the factory.
	 *
	 * @param scalarCacheCapacity scalar cache capacity for every category
	 * @param linkCacheCapacity link cache capacity for every category
	 * @param options pre-fetch buffers settings
	 */
	public DBEntitySequenceFactory(int scalarCacheCapacity, int linkCacheCapacity, int continuationlinkCacheCapacity, DBEntitySequenceOptions options) {
	    m_scalarCache = new HashMap<String, LRUCache<ObjectID, Map<String, String>>>();
	    m_linkCache = new HashMap<String, LRUCache<ObjectID, LinkList>>();
	    m_continuationlinkCache = new HashMap<String, LRUCache<String, LinkList>>();
	    m_scalarCacheCapacity = scalarCacheCapacity;
	    m_linkCacheCapacity = linkCacheCapacity;
	    m_continuationlinkCacheCapacity = continuationlinkCacheCapacity;
	    m_options = options;
	}

	/* (non-Javadoc)
	 * @see com.dell.doradus.search.aggregate.EntitySequenceFactory#getSequence(com.dell.doradus.common.TableDefinition, java.lang.Iterable)
	 */
	@Override
	public <T> EntitySequence getSequence(TableDefinition tableDef, Iterable<T> collection) {
		return getSequence(tableDef, collection, null, null);
	}

    /* (non-Javadoc)
     * @see com.dell.doradus.search.aggregate.EntitySequenceFactory#getSequence(com.dell.doradus.common.TableDefinition, java.lang.Iterable, java.util.List, com.dell.doradus.search.aggregate.EntitySequenceOptions)
     */
    @Override
    public <T> EntitySequence getSequence(TableDefinition tableDef, Iterable<T> collection, List<String> fields) {
        return getSequence(tableDef, collection, fields, null);
    }

	/* (non-Javadoc)
	 * @see com.dell.doradus.search.aggregate.EntitySequenceFactory#getSequence(com.dell.doradus.common.TableDefinition, java.lang.Iterable, java.util.List, com.dell.doradus.search.aggregate.EntitySequenceOptions[])
	 */
	@Override
	public <T> EntitySequence getSequence(TableDefinition tableDef, Iterable<T> collection, List<String> fields,
			EntitySequenceOptions options) {
	    if (fields == null || fields.contains(ALLSCALARMARK))
	        fields = ALLSCALARFIELDS;
		return new DBEntityRootCollection<T>(tableDef, fields, collection, this, tableDef.getTableName(), EntitySequenceOptions.getOptions(options, DBEntitySequenceOptions.defaultOptions));
	}

	/**
	 * Fetches the scalar values of the specified entity.
	 * Also fetches the scalar values of other entities to be returned by the iterators of the same category
	 * Uses {@link #multiget_slice(List, ColumnParent, SlicePredicate)} method with the 'slice list' parameter to perform bulk fetch
	 *
	 * @param tableDef     entity type
	 * @param caller       next entity to be returned by the iterator (must be initialized first)
	 * @param scalarFields list of the fields to be fetched
	 * @param options      defines now many entities should be initialized
	 */
	void initializeScalarFields(DBEntity caller, List<String> scalarFields, DBEntitySequenceOptions options) {

		TableDefinition tableDef = caller.getTableDef();
		String category = toEntityCategory(tableDef.getTableName(), scalarFields);
		LRUCache<ObjectID, Map<String, String>> cache = getScalarCache(category);
		Set<ObjectID> idSet = new HashSet<ObjectID>();

		List<DBEntity> entities = collectUninitializedEntities(caller, cache, idSet, options.adjustEntityBuffer(cache));
		if (idSet.size() == 0){
			// all requested scalar values have been found in the cache, no fetching is required
			return;
		}
		

		Map<ObjectID, Map<String, String>> fetchResult = fetchScalarFields(tableDef, idSet, scalarFields, category);

		for (Map.Entry<ObjectID, Map<String, String>> entry: fetchResult.entrySet()) {
			cache.put(entry.getKey(), entry.getValue());
		}

        // Initialize the entities with the cached scalar values
        for (DBEntity entity : entities) {
            ObjectID key = entity.id();
            Map<String, String> values = cache.get(key);
            if(values == null) {
                values = new HashMap<String, String>();
            }
            entity.initialize(values);
        }
	}

    void initializeAllScalarFields(DBEntity caller, String continuationField, DBEntitySequenceOptions options) {
        TableDefinition tableDef = caller.getTableDef();
        String category = toEntityCategory(tableDef.getTableName(), ALLSCALARFIELDS);
        LRUCache<ObjectID, Map<String, String>> cache = getScalarCache(category);
        Set<ObjectID> idSet = new HashSet<ObjectID>();

        List<DBEntity> entities = collectContinueAllScalarEntities(caller, cache, idSet, continuationField, options.adjustEntityBuffer(cache));
        if (idSet.size() == 0){
            // all scalar values have been found in the cache, no fetching is required
            return;
        }
        int adjustedScalarBuffer = options.entityBuffer * options.initialScalarBuffer / idSet.size();
        Map<ObjectID, Map<String, String>> fetchResult =
                fetchAllScalarFields(tableDef, idSet, continuationField, adjustedScalarBuffer, category);

        // Update the entities and put the fetched results into the cache (if possible)
        for (Map.Entry<ObjectID, Map<String, String>> entry: fetchResult.entrySet()) {
            ObjectID entityid = entry.getKey();
            Map<String, String> newvalues = entry.getValue();
            // add continuation sign if needed
            if (newvalues.size() >= adjustedScalarBuffer) {
            	//find last field
            	String lastColumn = "";
            	for(String v: newvalues.keySet()) {
            		if(lastColumn.compareTo(v) < 0) lastColumn = v;
            	}
                newvalues.put(DBEntity.CONTINUATIONMARK, lastColumn);
            }
        	// find appropriate entity and update scalar map with new values
            for (DBEntity entity : entities) {
                if (entity.id().equals(entityid)) {
                	entity.update(newvalues);
                	break;
                }
            }
            // find cached scalar values
            Map<String, String> cachedvalues = cache.get(entityid);
            if (cachedvalues == null) {
            	// store new values in the cache only if they has been requested from the scratch
            	if (continuationField == null)
            		cache.put(entityid, newvalues);
            }
            else
                cachedvalues.putAll(newvalues);
            // remove continuation sign if needed
            if (!newvalues.containsKey(DBEntity.CONTINUATIONMARK) &&
                    (cachedvalues != null && cachedvalues.containsKey(DBEntity.CONTINUATIONMARK))) {
                cachedvalues.remove(DBEntity.CONTINUATIONMARK);
            }
        }
        
    }

	/**
	 * Fetches first N links of the specified link type. Fetches the links of the specified entity.
	 * Also fetches first N links of other entities to be returned by the iterators of the same category
	 * Uses {@link #multiget_slice(List, ColumnParent, SlicePredicate)} method method with the 'slice range' parameter to perform bulk fetch
     * Or uses {@link #get_slice(ByteBuffer, ColumnParent, SlicePredicate)} for SV links
	 *
	 * @param caller   next entity to be returned by the iterator (must be initialized first)
	 * @param link     link name
	 * @param fields   scalar fields of the linked entities
	 * @param category iterator category that will iterate the linked entities
	 * @param options  defines now many entities should be initialized and how many first links should be fetched
	 */
	void initializeLinks(DBEntity caller, String link, List<String> fields, String category, DBEntitySequenceOptions options) {

		TableDefinition tableDef = caller.getTableDef();
		LRUCache<ObjectID, LinkList> cache = getLinkCache(toIteratorCategory(tableDef.getTableName(),link, fields));
		Set<ObjectID> idSet = new HashSet<ObjectID>();
		TableDefinition linkedTableDef = tableDef.getLinkExtentTableDef(tableDef.getFieldDef(link));

		DBEntitySequenceOptions limitedOptions = options.adjustInitialLinkBufferDimension(cache);
		List<DBEntity> entities = collectUninitializedEntities(caller, category, linkedTableDef,
				fields, link, cache, idSet, limitedOptions);

		if (idSet.size() == 0) return;
		timers.start(category + " links", "Init");
		Timer timer = new Timer();
		int capacity = options.initialLinkBuffer;
		Map<ObjectID, List<ObjectID>> fetchResult = fetchLinks(tableDef, idSet, link, options.initialLinkBuffer);
		timer.stop();
		// Put the fetched results into the cache
		int resultCount = 0;
		for (Map.Entry<ObjectID, List<ObjectID>> entry: fetchResult.entrySet()) {
			cache.put(entry.getKey(), new LinkList(entry.getValue(), capacity));
			resultCount += entry.getValue().size();
		}
		timers.stop(category + " links", "Init", resultCount);

		log.debug("fetch {} {} of {}[{}] links ({})", new Object[]{ resultCount, category, idSet.size(), options.initialLinkBuffer, timer});

		// Initialize the child iterators with the cached link lists
		for (DBEntity entity : entities) {
			ObjectID id = entity.id();
			LinkList columns = cache.get(id);
			if(columns == null)
			{
			    columns = new LinkList(new ArrayList<ObjectID>(0), 1);
			}
			DBLinkIterator linkIterator = new DBLinkIterator(entity, link, columns, m_options.linkBuffer, this, category);
			entity.addIterator(category, new DBEntityIterator(linkedTableDef, entity, linkIterator, fields, this, category, m_options));
		}
	}

    /**
	 * Collects the entities to be initialized using the scalar field bulk fetch.
	 * Visits all the entities buffered by the iterators of the same category.
	 *
	 * @param entity  next entity to be returned by the iterator (must be initialized first)
	 * @param cache   scalar fields cache. If the cache contains the requested scalar values, the visited entity is initialized with the cached values and skipped
	 * @param keys    set of 'rows' to be fetched. The set will be filled by this method
	 * @param options limits the number of rows to be fetched
	 * @return        list of entities to be initialized.
	 *                The set of physical rows to be fetched is returned in the 'keys' set.
	 *                The set size can less than the list size if the list contains duplicates
	 */
	private static List<DBEntity> collectUninitializedEntities(DBEntity entity,
			final Map<ObjectID, Map<String, String>> cache, final Set<ObjectID> keys, final DBEntitySequenceOptions options) {
		DBEntityCollector collector = new DBEntityCollector(entity) {
			@Override
			protected boolean visit(DBEntity entity, List<DBEntity> list){
				if (!entity.initialized()) {
					ObjectID id = entity.id();
					Map<String, String> values = cache.get(id);
					if (values == null) {
						keys.add(id);
						list.add(entity);
						return (keys.size() < options.entityBuffer);
					}
					entity.initialize(values);
				}
				return true;
			}
		};
		return collector.collect();
	}

	/**
	 * Collects the entities to be initialized with the initial list of links using the link list bulk fetch.
	 * Visits all the entities buffered by the iterators of the same category.
	 *
	 * @param entity   next entity to be returned by the iterator (must be initialized first)
	 * @param category the iterator category that will return the linked entities
	 * @param tableDef type of the linked entities
	 * @param fields   scalar fields to be fetched by the linked entities iterators
	 * @param link     link name
	 * @param cache    link list cache. If the cache contains the requested link list, the visited entity is initialized with the cached list and skipped
	 * @param keys     set of 'rows' to be fetched. The set will be filled by this method
	 * @param options  limits the number of rows to be fetched
	 * @return         list of entities to be initialized.
	 *                 The set of physical rows to be fetched is returned in the 'keys' set.
	 *                 The set size can less than the list size if the list contains duplicates
	 */
	private List<DBEntity> collectUninitializedEntities(DBEntity entity, final String category,
			final TableDefinition tableDef, final List<String> fields, final String link, final Map<ObjectID, LinkList> cache, final Set<ObjectID> keys,
			final DBEntitySequenceOptions options) {

		DBEntityCollector collector = new DBEntityCollector(entity) {
			@Override
			protected boolean visit(DBEntity entity, List<DBEntity> list){
				if (entity.findIterator(category) == null) {
					ObjectID id = entity.id();
					LinkList columns = cache.get(id);
					if (columns == null) {
						keys.add(id);
						list.add(entity);
						return (keys.size() < options.initialLinkBufferDimension);
					}
					DBLinkIterator linkIterator = new DBLinkIterator(entity, link, columns, m_options.linkBuffer,
							DBEntitySequenceFactory.this, category);
					entity.addIterator(category, new DBEntityIterator(tableDef, entity, linkIterator, fields, DBEntitySequenceFactory.this, category, m_options));
				}
				return true;
			}
		};
		return collector.collect();
	}

    private static List<DBEntity> collectContinueAllScalarEntities(DBEntity entity,
            final Map<ObjectID, Map<String, String>> cache, final Set<ObjectID> keys, final String continuationField, final DBEntitySequenceOptions options) {
        DBEntityCollector collector = new DBEntityCollector(entity) {
            @Override
            protected boolean visit(DBEntity entity, List<DBEntity> list){
            	ObjectID id = entity.id();
                if (!entity.initialized()) {
                    Map<String, String> values = cache.get(id);
                    if (values == null) {
                        if (continuationField != null)
                            return true;
                        keys.add(id);
                        list.add(entity);
                        return (keys.size() < options.entityBuffer);
                    }
                    entity.initialize(values);
                }
                if (continuationField == null)
                    return true;
                if (continuationField.equals(entity.getContinuationField())) {
                    keys.add(id);
                    list.add(entity);
                    return (keys.size() < options.entityBuffer);
                }
                return true;
            }
        };
        return collector.collect();
    }


	/**
	 * Returns the LRU cache of the specified 'iterator', 'entity' or 'continuationlink' category.
	 *
	 * @param cacheMap either {@link #m_linkCache} or {@link #m_scalarCache}
	 * @param category 'iterator' category (for instance, 'Person.Message.Sender') in case of {@link #m_linkCache}
	 *                 'entity' category (for instance, 'Person[Name,Department]') in case of {@link #m_scalarCache}
	 *                 'continuationlink' category (for instance, 'Person.Message') in case of {@link #m_continuationlinkCache}
	 * @return         LRUCache<ObjectID, LinkList> cache in case of {@link #m_linkCache}
	 *                 LRUCache<String, Map<String, String>> cache in case of {@link #m_scalarCache}
	 *                 LRUCache<String, LinkList> cache in case of {@link #m_continuationlinkCache}
	 */
	private <C, K, T> LRUCache<K, T> getCache(Map<C, LRUCache<K, T>> cacheMap, int capacity, C category) {
		LRUCache<K, T> cache = cacheMap.get(category);
		if (cache == null) {
			cache = new LRUCache<K, T>(capacity);
			cacheMap.put(category, cache);
		}
		return cache;
	}

	public LRUCache<ObjectID, Map<String, String>> getScalarCache(String category) {
		return getCache(m_scalarCache, m_scalarCacheCapacity, category);
	}
	
	public LRUCache<ObjectID, LinkList> getLinkCache(String category) {
		return getCache(m_linkCache, m_linkCacheCapacity, category);
	}
	
	public LRUCache<String, LinkList> getContinuationLinkCache(String category) {
		return getCache(m_continuationlinkCache, m_continuationlinkCacheCapacity, category);
	}

    private Map<ObjectID, Map<String, String>> fetchAllScalarFields(TableDefinition tableDef,
            Collection<ObjectID> ids, String continuationField, int count, String category) {
        String[] timerInfo = new String[] { category, "Init all scalar fields" };
        timers.start(timerInfo[0], timerInfo[1]);
        Map<ObjectID, Map<String, String>> map = SpiderHelper.getScalarValues(tableDef, ids, continuationField, count);
        long time = timers.stop(timerInfo[0], timerInfo[1], ids.size());
        log.debug("fetch {} {}, {} fields from {} ({})", new Object[] {ids.size(), category, count, continuationField, Timer.toString(time)});
        return map;
    }

	/**
	 * Fetches the scalar field values of the specified set of entities
	 *
	 * @param tableDef     entity type
	 * @param keys         entity id list
	 * @param scalarFields field name list
	 * @return             result of {@link #multiget_slice(List, ColumnParent, SlicePredicate)} execution
	 */
	private Map<ObjectID, Map<String, String>> fetchScalarFields(TableDefinition tableDef,
			Collection<ObjectID> ids, List<String> fields, String category) {
		timers.start(category, "Init Fields");
		Map<ObjectID, Map<String, String>> map = SpiderHelper.getScalarValues(tableDef, ids, fields);
		long time = timers.stop(category, "Init Fields", ids.size());
       	log.debug("fetch {} {} ({})", new Object[] {ids.size(), category, Timer.toString(time)});
		return map;
	}

    String fetchScalarFieldValue(TableDefinition tableDef, ObjectID id, String field) {
        String fieldDetails = new StringBuffer().append(tableDef.getTableName()).append('[').append(field).append(']').toString();
        timers.start("Fetch Field Value", fieldDetails);
        String value = SpiderHelper.fetchScalarFieldValue(tableDef, id, field);
        timers.stop("Fetch Field Value", fieldDetails, value == null ? 0 : 1);
        return value;
    }

    /**
	 * Fetches the link list of the specified 'iterator' category of the specified entity
	 *
	 * @param tableDef         entity type
	 * @param id               entity id
	 * @param link             link name
	 * @param continuationLink last fetched link or null if it is the initial fetch.
	 * @param count            maximum size of the link list to be fetched
	 * @param category         category of the iterators that will iterate the linked entities.
	 *
	 * @return                 result of {@link #get_slice(ByteBuffer, ColumnParent, SlicePredicate)} execution
	 */
	List<ObjectID> fetchLinks(TableDefinition tableDef, ObjectID id, String link, ObjectID continuationLink,
			int count, String category) {
		timers.start(category+" links", "Continuation");
		FieldDefinition linkField = tableDef.getFieldDef(link);
		List<ObjectID> list = SpiderHelper.getLinks(linkField, id, continuationLink, true, count);
		int resultCount = (continuationLink == null) ? list.size() : list.size() - 1;
		long time = timers.stop(category + " links", "Continuation", resultCount);
		log.debug("fetch {} {} continuation links ({})", new Object[]{resultCount, category, Timer.toString(time)});
		return list;
	}

	/**
	 * Fetches first N links of the specified type for every specified entity.
	 *
	 * @param tableDef entity type
	 * @param keys     entity id list
	 * @param link     link name
	 * @param count    maximum size of the link list to be fetched for every entity
	 * @return         result of {@link #multiget_slice(List, ColumnParent, SlicePredicate)} execution
	 */
	private Map<ObjectID, List<ObjectID>> fetchLinks(TableDefinition tableDef, Collection<ObjectID> ids,
			String link, int count) {
		FieldDefinition linkField = tableDef.getFieldDef(link);
		return SpiderHelper.getLinks(linkField, ids, null, true, count);
	}


	/**
	 * Converts an entity type and scalar field name list to an 'entity category'
	 * Entity category is represented as String: "table[field1,field2]"
	 *
	 * @param table  entity type
	 * @param fields scalar field names the entity should be initialized with
	 * @return       entity category
	 */
	static String toEntityCategory(String table, List<String> fields) {
		if (fields == null || fields.size() == 0) {
			return table + "[]";
		}
		StringBuffer buffer = new StringBuffer();
		buffer.append(table).append('[').append(fields.get(0));
		for (int i = 1; i < fields.size(); i++) {
			buffer.append(',').append(fields.get(i));
		}
		buffer.append(']');
		return buffer.toString();
	}

	/**
	 * Converts an iterator category and the link name to a child iterator category.
	 * Iterator category is represented as String: "Type.link1.link2...". For instance "Message.Sender.Person"
	 *
	 * @param type parent iterator category
	 * @param link link name
	 * @return     child iterator category
	 */
	static String toIteratorCategory(String type, String link, List<String> fields) {
		return toEntityCategory(type + "." + link, fields);
		//return type + '.' + link;
	}

}

abstract class DBEntityCollector {

	protected List<DBEntity> m_list;
	private List<DBEntityIterator> m_path;
	private DBEntity m_entity;
	private DBEntityIterator m_iterator;
	private int m_counter;
	private int m_entityCategory = 10000;

	DBEntityCollector(DBEntity entity) {
		m_entity = entity;
		m_iterator = m_entity.parentIterator();
	}

	List<DBEntity> collect() {

		m_list = new ArrayList<DBEntity>();
		if (!visit(m_entity, m_list)){
			return m_list;
		}
        // don't collect entities if the required entity has been initialized with the cached data
        if (m_list.size()==0){
            return m_list;
        }
		if (m_iterator != null){
			// Collect the path up to the root iterator
			DBEntityIterator iterator = m_iterator;
			m_path = new ArrayList<DBEntityIterator>();
			while (iterator != null && !iterator.isDisposed()) {
				m_path.add(0, iterator);
				if (!collect(iterator, 0)){
					return m_list;
				}
				if (iterator.parentEntity() != null) {
					iterator = iterator.parentEntity().parentIterator();
				}
				else {
					iterator = null;
				}
			}
		}
		return m_list;
	}

	private boolean collect(DBEntityIterator iterator, int pathIndex){
		if (pathIndex < m_path.size() - 1) {
			DBEntityIterator caller = m_path.get(pathIndex + 1);
			String category = caller.category();
			for (DBEntity entity : iterator.bufferedEntities()) {
				DBEntityIterator childIterator = entity.findIterator(category);
				if (childIterator != null && childIterator != caller) {
					if (!collect(childIterator, pathIndex + 1)){
						return false;
					}
				}
			}
		} else {
			return collect(iterator);
		}
		return true;
	}

	private boolean collect(DBEntityIterator iterator){
		iterator.prefetchBuffer(m_entityCategory - m_counter);
		for (DBEntity entity : iterator.bufferedEntities()) {
			if (++ m_counter > m_entityCategory){
				return false;
			}
			// m_entity is collected during the path constructing.
			// See collect() method.
			if (entity != m_entity && !visit(entity, m_list)){
				return false;
			}
		}
		return true;
	}

	protected abstract boolean visit(DBEntity entity, List<DBEntity> list);
}

class DBEntityIterator implements Iterator<Entity> {

	//private Logger log = LoggerFactory.getLogger(DBEntityIterator.class);

	private static final Map<String,String> EMPTY_MAP = new HashMap<String,String>();
	private TableDefinition m_tableDef;
	private DBEntity m_parentEntity;
	private Iterator<ObjectID> m_idIterator;
	private String m_category;
	private DBEntitySequenceOptions m_options;
	private DBEntitySequenceFactory m_factory;
	private List<String> m_fields;
	private List<DBEntity> m_buffer;
	private boolean m_hasMore;
	private boolean m_isDisposed;
	private EntityCounter m_counter;

	DBEntityIterator(TableDefinition tableDef, DBEntity parentEntity, Iterator<ObjectID> idIterator, List<String> fields, DBEntitySequenceFactory factory, String category,
			DBEntitySequenceOptions options) {
		m_factory = factory;
		m_tableDef = tableDef;
		m_parentEntity = parentEntity;
		m_idIterator = idIterator;
		m_options = options;
		// iterator category is represented as String: "type.link1.link2...". for instance, "Message.Sender.Person"
		m_category = category;
		m_fields = fields;

		m_hasMore = true;
		m_counter = EntityCounter.get(m_category);
		m_buffer = new ArrayList<DBEntity>();
		prefetchAvailableBuffer();
	}

	@Override
	public boolean hasNext() {
		if (m_isDisposed) {
			return false;
		}
		if (m_buffer.size() == 0) {
			prefetchBuffer();
		}
		if (m_buffer.size() == 0) {
			dispose();
			return false;
		}
		return true;
	}

	@Override
	public Entity next() {
		if (!hasNext()) {
			throw new NoSuchElementException();
		}
		DBEntity entity = m_buffer.get(0);
		if (!entity.initialized()) {
            initializeScalarFields(entity);
		}
		m_buffer.remove(0);
		m_counter.decrement();
		//log.debug("get '{}' {}", m_type, entity.id());
		return entity;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	void dispose() {
		m_isDisposed = true;
		// clear internal data just in case if any references to this iterator still exist.
		m_parentEntity = null;
		m_idIterator = null;
		m_fields = null;
		m_buffer = null;
	}

	boolean isDisposed() {
		return m_isDisposed;
	}

	DBEntity parentEntity() {
		return m_parentEntity;
	}

	void prefetchAvailableBuffer() {
		if (m_idIterator instanceof PrefetchIterator) {
			prefetchBuffer(((PrefetchIterator) m_idIterator).bufferSize());
		}
	}

	public void prefetchBuffer() {
		if (m_hasMore && !m_isDisposed) {
			prefetchBuffer(m_options.entityBuffer - m_buffer.size());
		}
	}

	public void prefetchBuffer(int count) {
		while (m_hasMore && m_buffer.size() < m_options.entityBuffer && count > 0) {
			if (m_idIterator.hasNext()) {
				ObjectID id = m_idIterator.next();
				DBEntity entity = new DBEntity(m_tableDef, id, null, m_factory, this, m_category);
				m_counter.increment();
				if (m_fields.size() == 0) {
					entity.initialize(EMPTY_MAP);
				}
				m_buffer.add(entity);
				count--;
			}else {
				m_hasMore = false;
			}
		}
	}

	String category() {
		return m_category;
	}

	List<DBEntity> buffer() {
		return m_buffer;
	}

	EntityCounter counter(){
		return m_counter;
	}

	PrefetchCollection bufferedEntities(){
		return new PrefetchCollection(this);
	}

	void initializeScalarFields(DBEntity entity) {
        // All fields (*) handling
        if (m_fields.size() == 1 && m_fields.get(0).equals(DBEntity.CONTINUATIONMARK)) {
            m_factory.initializeAllScalarFields(entity, null, m_options);
        } else {
            m_factory.initializeScalarFields(entity, m_fields, m_options);
        }
	}

    void initializeAllScalarFields(DBEntity entity, String continuationField) {
        m_factory.initializeAllScalarFields(entity, continuationField, m_options);
    }

    boolean containsField(String field) {
        if (m_fields.contains(DBEntitySequenceFactory.ALLSCALARMARK))
            return true;
        return m_fields.contains(field);
    }
}

class PrefetchCollection implements Iterable<DBEntity>{

	private DBEntityIterator m_sourceIterator;
	PrefetchCollection (DBEntityIterator iterator){
		m_sourceIterator = iterator;
		m_sourceIterator.prefetchBuffer(Math.max(0, 10000 - m_sourceIterator.counter().value()));
	}
	@Override
	public Iterator<DBEntity> iterator() {
		return new PrefetchIterator();
	}
	class PrefetchIterator implements Iterator<DBEntity>{
		int m_index;
		@Override
		public boolean hasNext() {
			return m_index < m_sourceIterator.buffer().size();
		}

		@Override
		public DBEntity next() {
			if (hasNext()){
				return m_sourceIterator.buffer().get(m_index++);
			}
			return null;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

	}

}


abstract class DisposableDBEntityCollection implements EntitySequence {
	private List<DBEntityIterator> m_iterators;

	abstract protected DBEntityIterator createIterator();

	@Override
	public Iterator<Entity> iterator() {
		if (m_iterators == null) {
			m_iterators = new ArrayList<DBEntityIterator>();
		}
		DBEntityIterator iterator = createIterator();
		m_iterators.add(iterator);
		return iterator;
	}

	public void Dispose() {
		if (m_iterators != null) {
			for (DBEntityIterator iterator : m_iterators) {
				iterator.dispose();
			}
			m_iterators.clear();
		}
	}
}

class DBEntityRootCollection<T> extends DisposableDBEntityCollection {
	private DBEntitySequenceFactory m_factory;
	private TableDefinition m_tableDef;
	private Iterable<T> m_idCollection;
	private List<String> m_fields;
	private String m_category;
	private DBEntitySequenceOptions m_options;

	DBEntityRootCollection(TableDefinition tableDef, List<String> fields, Iterable<T> idCollection, DBEntitySequenceFactory factory, String category, DBEntitySequenceOptions options) {
		m_factory = factory;
		m_tableDef = tableDef;
		m_fields = fields;
		m_idCollection = idCollection;
		// iterator category is represented as String: "type.link1.link2...". for instance, "Message.Sender.Person"
		m_category = category;
		m_options = options;
	}

	@Override
	protected DBEntityIterator createIterator() {
		return new DBEntityIterator(m_tableDef, null, new StringIterator(m_idCollection.iterator()), m_fields, m_factory, m_category, m_options);
	}

	class StringIterator extends PrefetchIterator implements Iterator<ObjectID>{

		static final int defaultBufferSize = 1000;
		private Iterator<T> m_iterator;

		StringIterator (Iterator<T> iterator){
			this(iterator, defaultBufferSize);
		}
		StringIterator (Iterator<T> iterator, int bufferSize){
			m_iterator = iterator;
			m_buffer = new ObjectID[bufferSize];
		}

		@Override
		protected void fetchBuffer(){
			for (m_count = 0; m_count < m_buffer.length && m_iterator.hasNext(); m_count++){
				m_buffer[m_count] = getNext();
			}
			m_index = 0;
			m_hasMore = m_count == m_buffer.length;
		}

		private ObjectID getNext() {
			Object obj = m_iterator.next();
			if (obj instanceof Entity){
				return ((Entity)obj).id();
			}
			else if (obj instanceof DBObject){
				return IDHelper.createID(((DBObject)obj).getObjectID());
			}
			else return (ObjectID)obj;
		}
	}
}

class DBEntityCollection extends DisposableDBEntityCollection {
	private DBEntity m_entity;
	private String m_link;
	private List<String> m_fields;
	private DBEntitySequenceOptions m_options;

	DBEntityCollection(DBEntity entity, String link, List<String> fields, DBEntitySequenceOptions options) {
		m_entity = entity;
		m_link = link;
		m_fields = fields;
		m_options = options;
	}

	@Override
	protected DBEntityIterator createIterator() {
		DBEntityIterator iterator = m_entity.createIterator(m_link, m_fields, m_options);
		return iterator;
	}
}

class EntityCounter {
	int m_value;
	private static HashMap<String, EntityCounter> our_counters = new HashMap<String, EntityCounter>();
	public static EntityCounter get(String category){
		EntityCounter counter = our_counters.get(category);
		if (counter == null){
			counter = new EntityCounter();
			our_counters.put(category, counter);
		}
		return counter;
	}
	public int value() {
		return m_value;
	}
	public int increment(){
		return ++ m_value;
	}
	public int decrement(){
		return -- m_value;
	}
}

class DBEntity implements Entity {
    static final String CONTINUATIONMARK = "*";

	private ObjectID m_id;
	private DBEntityIterator m_parentIterator;
	private Map<String, String> m_scalarMap;
	private TableDefinition m_tableDef;
	private String m_category;
	private DBEntitySequenceFactory m_factory;
	private Map<String, DBEntityIterator> m_childIterators;

	/**
	 * Creates a new Entity instance.
	 *
	 * @param tableDef       entity type
	 * @param id             entity id
	 * @param scalarMap      scalar values map. Map can be defined later by {@link #initialize(Map)} method later
	 * @param factory        iterator factory
	 * @param parentIterator iterator that returns this entity
	 * @param category       entity category is represented as a String 'type[field1,field2]'. For example, Person[Name,Department].
	 *                       entity category is used to cache scalar values maps by category.
	 */
	DBEntity(TableDefinition tableDef, ObjectID id, Map<String, String> scalarMap, DBEntitySequenceFactory factory, DBEntityIterator parentIterator, String category) {
		m_factory = factory;
		m_parentIterator = parentIterator;
		m_tableDef = tableDef;
		m_category = category;
		m_id = id;
		m_scalarMap = scalarMap;
	}

	public ObjectID id() {
		return m_id;
	}

	public TableDefinition getTableDef() {
		return m_tableDef;
	}

	public String get(String field) {
        if (!initialized())
            return null;
        if (!parentIterator().containsField(field))
            return m_factory.fetchScalarFieldValue(m_tableDef, id(), field);
	    do {
    		String value = m_scalarMap.get(field);
            String continuationField = m_scalarMap.get(DBEntity.CONTINUATIONMARK);
    		if (value != null || continuationField == null)
    		    return value;
            m_parentIterator.initializeAllScalarFields(this, continuationField);
	    } while (true);
	}

    public Iterable<String> getAllFields() {
        if (!initialized())
            return null;
        String continuationField;
        while ((continuationField = m_scalarMap.get(DBEntity.CONTINUATIONMARK)) != null) {
            m_parentIterator.initializeAllScalarFields(this, continuationField);
        }
        return m_scalarMap.keySet();
    }

	public EntitySequence getLinkedEntities(String link, List<String> fields) {
		return getLinkedEntities(link, fields, null);
	}

	public EntitySequence getLinkedEntities(String link, List<String> fields, EntitySequenceOptions options) {
		return new DBEntityCollection(this, link, fields, EntitySequenceOptions.getOptions(options, DBEntitySequenceOptions.defaultOptions));
	}

	DBEntityIterator parentIterator() {
		return m_parentIterator;
	}

	DBEntityIterator findIterator(String type) {
		if (m_childIterators != null) {
			return m_childIterators.get(type);
		}
		return null;
	}

	void addIterator(String category, DBEntityIterator iterator) {
		if (m_childIterators == null) {
			m_childIterators = new HashMap<String, DBEntityIterator>();
		}
		m_childIterators.put(category, iterator);
	}

	DBEntityIterator createIterator(String link, List<String> fields, DBEntitySequenceOptions options) {

		if (m_childIterators == null) {
			m_childIterators = new HashMap<String, DBEntityIterator>();
		}

		String linkType = DBEntitySequenceFactory.toIteratorCategory(m_category,link, fields);
		DBEntityIterator iterator = m_childIterators.get(linkType);

		if (iterator == null) {
			m_factory.initializeLinks(this, link, fields, linkType, options);
		}

		iterator = m_childIterators.get(linkType);
		// Iterator can be used only once
		m_childIterators.remove(linkType);
		return iterator;
	}

	boolean initialized() {
		return m_scalarMap != null;
	}

    String getContinuationField() {
        if (!initialized())
            return null;
        return m_scalarMap.get(DBEntity.CONTINUATIONMARK);
    }

	void initialize(Map<String,String> values) {
		m_scalarMap = values;
	}

	void update(Map<String,String> values) {
	    if (m_scalarMap == null) {
	        m_scalarMap = values;
	    } else {
	        m_scalarMap.putAll(values);
	        if (m_scalarMap.containsKey(DBEntity.CONTINUATIONMARK) && !values.containsKey(DBEntity.CONTINUATIONMARK)) {
	        	m_scalarMap.remove(DBEntity.CONTINUATIONMARK);
	        }
	    }
	}
}

abstract class PrefetchIterator implements Iterator<ObjectID> {
	protected ObjectID[] m_buffer;
	protected int m_index = 0;
	protected int m_count = 0;
	protected boolean m_hasMore = true;

	abstract protected void fetchBuffer();

	public int bufferSize() {
		return m_count - m_index;
	}

	@Override
	public boolean hasNext() {
		if (m_index == m_count && m_hasMore) {
			fetchBuffer();
		}
		return m_hasMore || m_index < m_count;
	}

	@Override
	public ObjectID next() {
		if (!hasNext()) {
			throw new NoSuchElementException();
		}
		return m_buffer[m_index++];
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
}

class DBLinkIterator extends PrefetchIterator {
	private String m_link;
	private ObjectID m_id;
	private String m_category;
	private String m_cachecategory;

	private int m_capacity;

	private DBEntitySequenceFactory m_factory;
	private TableDefinition m_tableDef;

	/**
	 * Creates a new iterator that returns id of the linked entities
	 * @param entity   entity which links should be iterated
	 * @param link     link name
	 * @param linkList initial link list pre-fetched earlier
	 * @param capacity maximum size of the fetched link buffer
	 * @param factory  cache factory
	 * @param category category of the {@link DBEntityIterator} instance that will return the linked entities
	 */
	DBLinkIterator(Entity entity, String link, LinkList linkList, int capacity,
			DBEntitySequenceFactory factory, String category) {
		m_category = category;
		m_tableDef = entity.getTableDef();
		m_id = entity.id();
		m_link = link;
		m_factory = factory;
		m_capacity = capacity;
		m_cachecategory = DBEntitySequenceFactory.toIteratorCategory(entity.getTableDef().getTableName(), m_link, null);
		setValues(linkList, 0);
	}

	/**
	 * @param values the initial links fetched by {@link DBEntitySequenceFactory#initializeLinks(DBEntity, String, List, String, DBEntitySequenceOptions)}
	 * or the continuation links fetched by {@link #fetchValues()}
	 * @param index index == 1 in case of continuation fetch - the first column is the last column of the previous fetch
	 */
	private void setValues(LinkList values, int index) {
		m_buffer = values.links;
		m_hasMore = values.hasMore;
		m_index = index;
		m_count = m_buffer.length;
	}

	protected void fetchBuffer() {
		ObjectID continuationLink = (m_buffer.length == 0)? null : m_buffer[m_buffer.length-1];
		LRUCache<String, LinkList> cache = m_factory.getContinuationLinkCache(m_cachecategory);
		String key = continuationLink == null ? m_id.toString() : String.format("%s:%s", m_id.toString(), continuationLink.toString());
		LinkList continuationcolumns = cache.get(key);
		if (continuationcolumns == null) {
			List<ObjectID> fetchedLinks = m_factory.fetchLinks(m_tableDef, m_id, m_link, continuationLink, m_capacity, m_category);
			continuationcolumns = new LinkList(fetchedLinks, m_capacity);
			cache.put(key, continuationcolumns);
		}
		setValues(continuationcolumns, continuationLink == null ? 0 : 1);
	}
}

/**
 * Contains the list of 'id' strings
 * and the boolean flag - whether the element may have more links
 *
 */
class LinkList {

	/**
	 *  List of the fetched links
	 */
	ObjectID[] links;

	/**
	 * true if the number of the fetched links was equal to the limit (column family may have more 'link' columns
	 */
	boolean hasMore;

	LinkList(List<ObjectID> ids, int capacity) {
		this.links = new ObjectID[ids.size()];
		this.hasMore = links.length == capacity;
		for (int i = 0; i < links.length; i++ ){
			links[i] = ids.get(i);
		}
	}
	
}
