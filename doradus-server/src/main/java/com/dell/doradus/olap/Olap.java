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

package com.dell.doradus.olap;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.olap.aggregate.AggregationRequest;
import com.dell.doradus.olap.aggregate.AggregationRequestData;
import com.dell.doradus.olap.aggregate.AggregationResult;
import com.dell.doradus.olap.aggregate.DuplicationDetection;
import com.dell.doradus.olap.aggregate.mr.MFAggregationBuilder;
import com.dell.doradus.olap.io.VDirectory;
import com.dell.doradus.olap.merge.Merger;
import com.dell.doradus.olap.search.Searcher;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.olap.store.SegmentStats;
import com.dell.doradus.search.SearchResultList;
import com.dell.doradus.search.util.LRUCache;
import com.dell.doradus.service.db.DBService;
import com.dell.doradus.service.db.Tenant;
import com.dell.doradus.service.olap.OLAPService;
import com.dell.doradus.service.schema.SchemaService;
import com.dell.doradus.service.tenant.TenantService;
import com.dell.doradus.utilities.Timer;


/***
 * Table OLAP
 * 
 * $root/applications/
 *  - app1/
 *  	- shard1/
 *  		- .cube.txt
 *  		- .cube_N/
 *  		- timestamp-guid/
 *  		- timestamp-guid/
 *  		- timestamp-guid/
 *  	- shard2/
 *  	- shard3/
 *  - app2/
 *  - app3/

 */

public class Olap {
    private static Logger LOG = LoggerFactory.getLogger("Olap.Olap");
    private static final int olap_search_threads = OLAPService.instance().getParamInt("olap_search_threads", 0);
    private static ExecutorService search_executor =
            olap_search_threads == 0 ? null : Executors.newFixedThreadPool(olap_search_threads);
	
    private Map<String, Map<String, VDirectory>> m_tenantAppRoots = new HashMap<>();
	private FieldsCache m_fieldsCache =
	        new FieldsCache(OLAPService.instance().getParamInt("olap_cache_size_mb", 100) * 1024L * 1024);
	private LRUCache<String, CubeSearcher> m_cachedSearchers =
	        new LRUCache<>(Math.min(8192, OLAPService.instance().getParamInt("olap_loaded_segments", 8192)));
	private Set<String> m_mergedCubes = new HashSet<String>();
	
	public Olap() { }
	
	public static ExecutorService getSearchThreadPool() { return search_executor; }
	
	/**
	 * Danger: only works for default keyspace
	 */
	public VDirectory createApplication(String appName) {
        Tenant tenant = new Tenant(TenantService.instance().getDefaultTenantDef());
        return createApplication(tenant, appName);
	}
	
	public VDirectory createApplication(Tenant tenant, String appName) {
	    DBService.instance(tenant).createStoreIfAbsent("OLAP", true);
	    VDirectory root = new VDirectory(tenant, "OLAP").getDirectoryCreate("applications").getDirectoryCreate(appName);
	    synchronized (m_tenantAppRoots) {
	        String tenantName = tenant.getName();
	        Map<String, VDirectory> appRoots = m_tenantAppRoots.get(tenantName);
	        if (appRoots == null) {
	            appRoots = new HashMap<>();
	            m_tenantAppRoots.put(tenantName, appRoots);
	        }
	        appRoots.put(appName, root);
	    }
	    return root;
	}
	
	// Returns $root/applications/<appName> from correct keyspace/OLAP CF
	public VDirectory getRoot(ApplicationDefinition appDef) {
	    VDirectory root = null;
	    String tenantName = appDef.getTenantName();
	    if(Utils.isEmpty(tenantName)) tenantName = TenantService.instance().getDefaultTenantName();
	    //Tenant tenant = Tenant.getTenant(appDef);
	    synchronized (m_tenantAppRoots) {
	        Map<String, VDirectory> appRoots = m_tenantAppRoots.get(tenantName);
	        if (appRoots == null) {
	            appRoots = new HashMap<>();
	            m_tenantAppRoots.put(tenantName, appRoots);
	        }
            root = appRoots.get(appDef.getAppName());
	        if (root == null) {
	    	    Tenant tenant = Tenant.getTenant(appDef);
	            root = new VDirectory(tenant, "OLAP").getDirectory("applications").getDirectory(appDef.getAppName());
	            assert root != null;
	            appRoots.put(appDef.getAppName(), root);
	        }
	    }
	    return root;
	}

	public VDirectory getDirectory(ApplicationDefinition appDef, String shard) {
        VDirectory appDir = getRoot(appDef);
        VDirectory shardDir = appDir.getDirectory(shard);
        String segment = getCubeSegment(appDef, shard);
        VDirectory segmentDir = shardDir.getDirectory(segment);
        return segmentDir;
	}
	
	/**
	 * If tenant is null then default tenant is used.
	 */
	public ApplicationDefinition getApplicationDefinition(Tenant tenant, String applicationName) {
		if(tenant == null) tenant = TenantService.instance().getDefaultTenant();
		ApplicationDefinition appDef = SchemaService.instance().getApplication(tenant, applicationName);
		return appDef;
	}
	
	public void deleteApplication(ApplicationDefinition appDef) {
	    VDirectory root = getRoot(appDef);
	    synchronized (m_tenantAppRoots) {
	        root.delete();
	        String tenantName = Tenant.getTenant(appDef).getName();
	        Map<String, VDirectory> appRoots = m_tenantAppRoots.get(tenantName);
	        if (appRoots != null) {
	            appRoots.remove(appDef.getAppName());
	        }
	    }
	}

	public void deleteShard(ApplicationDefinition appDef, String shard) {
		getRoot(appDef).getDirectory(shard).delete();
	}
	
	public List<String> listShards(ApplicationDefinition appDef) {
		return getRoot(appDef).listDirectories();
	}

	public List<String> listSegments(ApplicationDefinition appDef, String shard) {
		VDirectory shardDir = getRoot(appDef).getDirectory(shard);
		return shardDir.listDirectories();
	}
	
	public String getCubeSegment(ApplicationDefinition appDef, String shard) {
		VDirectory shardDir = getRoot(appDef).getDirectory(shard);
		return shardDir.getProperty(".cube.txt");
	}
	
	public SegmentStats getStats(ApplicationDefinition appDef, String shard) {
		String cube = getCubeSegment(appDef, shard);
		if(cube == null) throw new IllegalArgumentException("Application does not exist or does not have merges yet");
		CubeSearcher s = getSearcher(appDef, shard, cube);
		return s.getStats();
	}

	public String addSegment(ApplicationDefinition appDef, String shard, OlapBatch batch) {
		return addSegment(appDef, shard, batch, true);
	}
	
	public String addSegment(ApplicationDefinition appDef, String shard, OlapBatch batch, boolean overwrite) {
		Timer t = new Timer();
		VDirectory shardDir = getRoot(appDef).getDirectoryCreate(shard);
		String prefix = overwrite ? "" : ".before.";
		String guid = prefix + Long.toString(System.currentTimeMillis(), 32) + "-" + UUID.randomUUID().toString();
		VDirectory segmentDir = shardDir.getDirectory(guid);
		batch.flushSegment(appDef, segmentDir);
		segmentDir.create();
		LOG.debug("add {} objects to {}/{} in {}", new Object[] { batch.size(), appDef.getAppName(), shard, t} );
		return guid;
	}

	public AggregationResult aggregate(ApplicationDefinition appDef, String table, OlapAggregate olapAggregate) {
		AggregationRequestData requestData = olapAggregate.createRequestData(this, appDef, table);
		AggregationRequest aggregationRequest = new AggregationRequest(this, appDef, requestData);
		AggregationResult result = MFAggregationBuilder.aggregate(this, appDef, aggregationRequest);
		return result;
	}

	public SearchResultList search(ApplicationDefinition appDef, String table, OlapQuery olapQuery) {
	    return Searcher.search(this, appDef, table, olapQuery);
	}

	// default merge (used in tests): forceMerge enabled
	public void merge(ApplicationDefinition appDef, String shard) {
		MergeOptions options = new MergeOptions(null, 0, true);
		merge(appDef, shard, options);
	}

	public void merge(ApplicationDefinition appDef, String shard, MergeOptions options) {
		if(options == null) {
			options = new MergeOptions();
		}
		String key = appDef.getAppName() + "/" + shard;
		synchronized(m_mergedCubes) {
			if(m_mergedCubes.contains(key)) throw new IllegalArgumentException(key + " is being merged");
			m_mergedCubes.add(key);
		}
		try {
			Timer t = new Timer();
			VDirectory shardDir = getRoot(appDef).getDirectory(shard);
			
			if(options.getExpireDate() != null) {
				shardDir.putProperty("expiration.txt", XType.toString(options.getExpireDate()));
			} else {
				shardDir.putProperty("expiration.txt", "");
			}
			
			List<String> segments = shardDir.listDirectories();
			if(segments.size() == 0) {
				LOG.debug("No segments in {}/{}", appDef.getAppName(), shard);
				return;
			} else if(segments.size() == 1 && segments.get(0).startsWith(".cube.") && !options.getForceMerge()) {
				LOG.debug("Shard {}/{} was not modified", appDef.getAppName(), shard);
				return;
			}
			
			List<VDirectory> sources = new ArrayList<VDirectory>();
			for(String segment : segments) {
				sources.add(shardDir.getDirectory(segment));
			}
			
			String guid = ".cube." + UUID.randomUUID().toString();
			VDirectory destination = shardDir.getDirectory(guid);
			
			Merger.mergeApplication(appDef, sources, destination);
			
			shardDir.putProperty(".cube.txt", guid);
			
			destination.create();
			
			LOG.debug("finished merging {} segments to {}/{} in {}", new Object[]{ segments.size(), appDef.getAppName(), shard, t} );
			
			if(options.getTimeout() > 0) {
				try {
					Thread.sleep(options.getTimeout() * 1000);
				} catch (InterruptedException e) { LOG.warn("sleep interrupted", e); }
			}
			
			for(String segment : segments) {
				shardDir.getDirectory(segment).delete();
			}
			
			LOG.debug("merge {} segments to {}/{} in {}", new Object[]{ segments.size(), appDef.getAppName(), shard, t} );
		} finally {
			synchronized(m_mergedCubes) {
				m_mergedCubes.remove(key);
			}
		}
	}
	
	public void setExpirationDate(ApplicationDefinition appDef, String shard, String expDate) {
        VDirectory shardDir = getRoot(appDef).getDirectory(shard);
        if(expDate != null) {
            Utils.parseDate(expDate); // check that the date is valid
            shardDir.putProperty("expiration.txt", expDate);
        } else {
            shardDir.putProperty("expiration.txt", "");
        }
	}
	
	public Date getExpirationDate(ApplicationDefinition appDef, String shard) {
		VDirectory shardDir = getRoot(appDef).getDirectory(shard);
		String expDateStr = shardDir.getProperty("expiration.txt"); 
		if(expDateStr == null || expDateStr.length() == 0) return null;
		else return Utils.dateFromString(expDateStr);
	}
	
	public CubeSearcher getSearcher(ApplicationDefinition appDef, String shard, String segment) {
		synchronized(m_cachedSearchers) {
			String key = appDef.getAppName() + "/" + shard + "/" + segment;
			CubeSearcher s = m_cachedSearchers.get(key);
			if(s == null) {
				VDirectory dir = getRoot(appDef);
				dir = dir.getDirectory(shard);
				dir = dir.getDirectory(segment);
				s = new CubeSearcher(dir, m_fieldsCache);
				//m_cachedSearchers.put(key, s, s.getStats().memory() + 2 * key.length() + 16);
				m_cachedSearchers.put(key, s);
			}
			return s;
		}
	}

	public SearchResultList getDuplicateIDs(ApplicationDefinition appDef, String table, String shardsRange) {
		String application = appDef.getAppName();
		TableDefinition tableDef = appDef.getTableDef(table);
		if(tableDef == null) throw new IllegalArgumentException("Table " + table + " not found in " + application);
		List<String> shards = getShardsList(appDef, null, shardsRange);
		//VDirectory appDir = getRoot(appDef);
		//List<VDirectory> dirs = new ArrayList<VDirectory>(shards.size());
		//for(String shard : shards) {
		//	String segment = getCubeSegment(appDef, shard);
		//	VDirectory shardDir = appDir.getDirectory(shard);
		//	VDirectory segmentDir = shardDir.getDirectory(segment);
		//	dirs.add(segmentDir);
		//}
		SearchResultList result = DuplicationDetection.getDuplicateIDs(tableDef, shards);
		return result;
	}
	
	public CubeSearcher getSearcher(ApplicationDefinition appDef, String shard) {
		String segment = getCubeSegment(appDef, shard);
		return getSearcher(appDef, shard, segment);
	}
	
	public List<String> getShardsList(ApplicationDefinition appDef, String shards, String shardsRange) {
    	if(shards != null && shardsRange != null) throw new IllegalArgumentException("Both shards and range parameters cannot be set");
    	if(shards == null && shardsRange == null) throw new IllegalArgumentException("shards or range parameter not set");
		List<String> shardsList = new ArrayList<String>();
    	if(shards != null) shardsList = Utils.split(shards, ',');
    	else if(shardsRange != null) {
    		String[] range = Utils.split(shardsRange, ',').toArray(new String[0]);
    		if(range.length == 0 || range.length > 2) throw new IllegalArgumentException("Shards range must be in form start-shard,end-shard or start-shard");
      		List<String> allShards = listShards(appDef);
    		String startShard = range[0];
    		String endShard = range.length == 1 ? null : range[1];
    		for(String shard : allShards) {
    			if(shard.compareToIgnoreCase(startShard) < 0) continue;
    			if(endShard != null && shard.compareToIgnoreCase(endShard) > 0) continue;
    			shardsList.add(shard);
    		}
    	}
    	return shardsList;
	}
	
}
