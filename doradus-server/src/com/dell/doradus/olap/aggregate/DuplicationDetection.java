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

package com.dell.doradus.olap.aggregate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.common.CommonDefs;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.olap.io.BSTR;
import com.dell.doradus.olap.io.FileDeletedException;
import com.dell.doradus.olap.io.VDirectory;
import com.dell.doradus.olap.merge.IxDoc;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.olap.store.IdReader;
import com.dell.doradus.search.FieldSet;
import com.dell.doradus.search.SearchResult;
import com.dell.doradus.search.SearchResultList;
import com.dell.doradus.search.util.HeapList;
import com.dell.doradus.utilities.Timer;

public class DuplicationDetection {
    private static Logger LOG = LoggerFactory.getLogger("DuplicationDetection");

	public static SearchResultList getDuplicateIDs(TableDefinition tableDef, List<VDirectory> dirs, List<String> shards) {
		// repeat if segment was merged
		for(int i = 0; i < 5; i++) {
			try {
				return getDuplicateIDsInternal(tableDef, dirs, shards);
			}catch(FileDeletedException ex) {
				LOG.warn(ex.getMessage() + " - retrying: " + i);
				continue;
			}
		}
		throw new FileDeletedException("All retries to getDuplicateIDs failed");
		//return getDuplicateIDsInternal(tableDef, dirs, shards);
	}
    
	public static SearchResultList getDuplicateIDsInternal(TableDefinition tableDef, List<VDirectory> dirs, List<String> shards) {
		Timer timer = new Timer();
		LOG.debug("Find duplicate ids in {}/{}", tableDef.getAppDef().getAppName(), tableDef.getTableName());
		
		int documentsCount = 0;
		Map<String, List<String>> result = new HashMap<String, List<String>>();
		List<String> curShards = new ArrayList<String>();
		BSTR last_id = new BSTR();
		last_id.length = -1;
		
        HeapList<IxDoc> heap = new HeapList<IxDoc>(dirs.size() - 1);
        IxDoc current = null;
        for(int i = 0; i < dirs.size(); i++) {
            current = new IxDoc(i, new IdReader(dirs.get(i), tableDef.getTableName()));
            current.next();
            current = heap.AddEx(current);
        }

        while (current != null && current.id != null) {
        	documentsCount++;
        	if(!BSTR.isEqual(last_id, current.id)) {
        		if(curShards.size() > 1) {
        			result.put(last_id.toString(), new ArrayList<String>(curShards));
        		}
        		last_id.set(current.id);
        		curShards.clear();
        	}
        	curShards.add(shards.get(current.segment));
            current.next();
            current = heap.AddEx(current);
        }
		if(curShards.size() > 1) {
			result.put(last_id.toString(), new ArrayList<String>(curShards));
		}
		
		SearchResultList res = new SearchResultList();
		FieldSet fs = new FieldSet(tableDef);
		res.documentsCount = documentsCount;
		StringBuilder sb = new StringBuilder();
		for(Map.Entry<String, List<String>> elem : result.entrySet()) {
			SearchResult r = new SearchResult();
			r.fieldSet = fs;
			r.scalars.put(CommonDefs.ID_FIELD, elem.getKey());
			sb.setLength(0);
			for(String shard : elem.getValue()) {
				sb.append(shard);
				sb.append(',');
			}
			sb.setLength(sb.length() - 1);
			r.scalars.put("shards", sb.toString());
			res.results.add(r);
		}
		
		LOG.debug("Found {} duplicate ids {}/{} merged in {}",
				new Object[] { result.size(), tableDef.getAppDef().getAppName(), tableDef.getTableName(), timer });
		
		return res;
	}
	
}
