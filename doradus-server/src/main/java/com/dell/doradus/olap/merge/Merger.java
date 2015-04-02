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

package com.dell.doradus.olap.merge;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.FieldType;
import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.core.ServerConfig;
import com.dell.doradus.olap.io.VDirectory;
import com.dell.doradus.olap.store.FieldSearcher;
import com.dell.doradus.olap.store.FieldWriter;
import com.dell.doradus.olap.store.FieldWriterSV;
import com.dell.doradus.olap.store.IdReader;
import com.dell.doradus.olap.store.IdWriter;
import com.dell.doradus.olap.store.NumSearcherMV;
import com.dell.doradus.olap.store.NumWriter;
import com.dell.doradus.olap.store.NumWriterMV;
import com.dell.doradus.olap.store.SegmentStats;
import com.dell.doradus.olap.store.ValueReader;
import com.dell.doradus.olap.store.ValueWriter;
import com.dell.doradus.search.util.HeapList;
import com.dell.doradus.utilities.Timer;

public class Merger {
    private static Logger LOG = LoggerFactory.getLogger("Olap.Merger");
    private static ExecutorService executor = ServerConfig.getInstance().olap_merge_threads == 0 ? null :
    		Executors.newFixedThreadPool(ServerConfig.getInstance().olap_merge_threads);
    
    private ApplicationDefinition appDef;
    private List<VDirectory> sources;
    private VDirectory destination;
    private SegmentStats stats;
    private Map<String, Remap> remaps = new HashMap<String, Remap>();
    private Object m_syncRoot = new Object();
	
	public static void mergeApplication(ApplicationDefinition appDef, List<VDirectory> sources, VDirectory destination) {
		Merger m = new Merger(appDef, sources, destination);
		if(executor != null) m.mergeApplicationWithThreadPool();
		else m.mergeApplication();
	}

	public Merger(ApplicationDefinition appDef, List<VDirectory> sources, VDirectory destination) {
		this.appDef = appDef;
		this.sources = sources;
		this.destination = destination;
	}

	public void mergeApplicationWithThreadPool() {
		try {
			Timer timer = new Timer();
			LOG.debug("Merging application {}", appDef.getAppName());
			stats = new SegmentStats();
			List<Future<?>> futures = new ArrayList<>();
			for(TableDefinition tableDef : appDef.getTableDefinitions().values()) {
				final TableDefinition fTableDef = tableDef;
				final String table = tableDef.getTableName();
				futures.add(executor.submit(new Runnable() {
					@Override public void run() {
						LOG.debug("   Merging {}", table);
						mergeDocs(fTableDef);
					}}));
			}
			for(Future<?> f: futures) f.get();
			futures.clear();
			for(TableDefinition tableDef : appDef.getTableDefinitions().values()) {
				LOG.debug("   Merging fields of table {}", tableDef.getTableName());
				for(FieldDefinition fieldDef : tableDef.getFieldDefinitions()) {
					final FieldDefinition fFieldDef = fieldDef;
					futures.add(executor.submit(new Runnable() {
						@Override public void run() {
							LOG.debug("      Merging {}/{} ({})", new Object[] {fFieldDef.getTableName(), fFieldDef.getName(), fFieldDef.getType()});
							mergeField(fFieldDef);
						}}));
				}
			}
			for(Future<?> f: futures) f.get();
			futures.clear();
			stats.totalStoreSize = destination.totalLength(false);
			stats.save(destination);
			LOG.debug("Application {} merged in {}", appDef.getAppName(), timer);
		}catch(ExecutionException ee) {
			throw new RuntimeException(ee);
		}catch(InterruptedException ee) {
			throw new RuntimeException(ee);
		}
	}
	
	
	
	public void mergeApplication() {
		Timer timer = new Timer();
		LOG.debug("Merging application {}", appDef.getAppName());
		stats = new SegmentStats();
		for(TableDefinition tableDef : appDef.getTableDefinitions().values()) {
			String table = tableDef.getTableName();
			LOG.debug("   Merging {}", table);
			mergeDocs(tableDef);
		}
		for(TableDefinition tableDef : appDef.getTableDefinitions().values()) {
			String table = tableDef.getTableName();
			LOG.debug("   Merging fields of table {}", table);
			for(FieldDefinition fieldDef : tableDef.getFieldDefinitions()) {
				LOG.debug("      Merging {}/{} ({})", new Object[] {table, fieldDef.getName(), fieldDef.getType()});
				mergeField(fieldDef);
			}
		}
		
		stats.totalStoreSize = destination.totalLength(false);
		stats.save(destination);
		LOG.debug("Application {} merged in {}", appDef.getAppName(), timer);
	}
	
    private void mergeDocs(TableDefinition tableDef) {
    	String table = tableDef.getTableName();
        Remap remap = new Remap(sources.size());
        IdWriter id_writer = new IdWriter(destination, table);
        
        HeapList<IxDoc> heap = new HeapList<IxDoc>(sources.size() - 1);
        IxDoc current = null;
        for(int i = 0; i < sources.size(); i++) {
            current = new IxDoc(i, new IdReader(sources.get(i), table));
            current.next();
            current = heap.AddEx(current);
        }

        while (current.id != null)
        {
        	int dstDoc = id_writer.add(current.id);
            remap.set(current.segment, current.reader.cur_number, dstDoc);
            if(current.reader.is_deleted) {
            	remap.setDeleted(current.segment, current.reader.cur_number, dstDoc);
            	id_writer.removeLastId(current.id);
            }
            current.next();
            current = heap.AddEx(current);
        }
        
        remap.shrink();
        id_writer.close();
        synchronized (m_syncRoot) {
            stats.addTable(table, id_writer.size());
            remaps.put(table, remap);
		}
    }
    
	private void mergeField(FieldDefinition fieldDef) {
		if(fieldDef.getType() == FieldType.TEXT || fieldDef.getType() == FieldType.BINARY) {
			mergeTextField(fieldDef);
		} else if(fieldDef.isLinkField()) {
			mergeLinkField(fieldDef);
		} else if(NumSearcherMV.isNumericType(fieldDef.getType())) {
			mergeNumField(fieldDef);
		} else if(fieldDef.isGroupField() || fieldDef.isXLinkField()) {
			// do nothing
		} else throw new RuntimeException("Unsupported field type: " + fieldDef.getType());
	}
	
    private void mergeNumField(FieldDefinition fieldDef)
    {
		String table = fieldDef.getTableName();
		String field = fieldDef.getName();
		Remap remap = remaps.get(table);
		if(remap.dstSize() == 0) return;
		
        if(fieldDef.isCollection()) {
            NumWriterMV num_writer = new NumWriterMV(remap.dstSize());
	        
	        HeapList<IxNum> heap = new HeapList<IxNum>(sources.size() - 1);
	        IxNum current = null;
	        for(int i = 0; i < sources.size(); i++) {
	            current = new IxNum(i, remap, new NumSearcherMV(sources.get(i), table, field));
	            current.next();
	            current = heap.AddEx(current);
	        }
	
	        while (current.doc != Integer.MAX_VALUE)
	        {
	        	num_writer.add(current.doc, current.num);
	            current.next();
	            current = heap.AddEx(current);
	        }
	        
	        num_writer.close(destination, table, field);
	        stats.addNumField(fieldDef, num_writer);
        }
        else {
            NumWriter num_writer = new NumWriter(remap.dstSize());
            
            for(int i = 0; i < sources.size(); i++) {
            	NumSearcherMV num_searcher = new NumSearcherMV(sources.get(i), table, field);
            	for(int j = 0; j < remap.size(i); j++) {
            		int doc = remap.get(i, j);
            		if(doc < 0) continue;
            		if(num_searcher.sv_isNull(j)) continue;
            		long d = num_searcher.sv_get(j);
            		num_writer.add(doc, d); 
            	}
            }
            
            num_writer.close(destination, table, field);
            synchronized (m_syncRoot) {
                stats.addNumField(fieldDef, num_writer);
    		}
        }
        
    }
	
    private void mergeTextField(FieldDefinition fieldDef) {
		String table = fieldDef.getTableName();
		String field = fieldDef.getName();
        Remap docRemap = remaps.get(table);
		if(docRemap.dstSize() == 0) return;
		Remap valRemap = new Remap(sources.size());
		
		{
	        ValueWriter value_writer = new ValueWriter(destination, table, field);
	        
	        HeapList<IxTerm> heap = new HeapList<IxTerm>(sources.size() - 1);
	        IxTerm current = null;
	        for(int i = 0; i < sources.size(); i++) {
	            current = new IxTerm(i, new ValueReader(sources.get(i), table, field));
	            current.next();
	            current = heap.AddEx(current);
	        }
	
	        while (current.term != null)
	        {
	        	int dstVal = value_writer.add(current.term, current.orig);
	        	valRemap.set(current.segment, current.reader.cur_number, dstVal);
	            current.next();
	            current = heap.AddEx(current);
	        }
	        
	        value_writer.close();
		}
		
        if(fieldDef.isCollection()) {
	        FieldWriter field_writer = new FieldWriter(docRemap.dstSize());
	        
	        HeapList<IxVal> heap = new HeapList<IxVal>(sources.size() - 1);
	        IxVal current = null;
	        for(int i = 0; i < sources.size(); i++) {
	            current = new IxVal(i, docRemap, valRemap, new FieldSearcher(sources.get(i), table, field));
	            current.next();
	            current = heap.AddEx(current);
	        }
	
	        while (current.doc != Integer.MAX_VALUE)
	        {
	        	field_writer.add(current.doc, current.val);
	            current.next();
	            current = heap.AddEx(current);
	        }
	        
	        field_writer.close(destination, table, field);
            synchronized (this) {
    	        stats.addTextField(fieldDef, field_writer);
    		}
        }
        else {
	        FieldWriterSV field_writer = new FieldWriterSV(docRemap.dstSize());
	        
            for(int i = 0; i < sources.size(); i++) {
            	FieldSearcher field_searcher = new FieldSearcher(sources.get(i), table, field);
            	for(int j = 0; j < docRemap.size(i); j++) {
            		int doc = docRemap.get(i, j);
            		if(doc < 0) continue;
            		int d = field_searcher.sv_get(j);
            		if(d < 0) continue;
            		field_writer.set(doc, valRemap.get(i, d));
            	}
            }
	        
	        field_writer.close(destination, table, field);
            synchronized (this) {
    	        stats.addTextField(fieldDef, field_writer);
    		}
        }
    }

    private void mergeLinkField(FieldDefinition fieldDef)
    {
		String table = fieldDef.getTableName();
		String link = fieldDef.getName();
		
        Remap docRemap = remaps.get(table);
        Remap valRemap = remaps.get(fieldDef.getLinkExtent());
		if(docRemap.dstSize() == 0 || valRemap.dstSize() == 0) return;

        //all links are multi-valued because of referential integrity and idempotent updates
        //if(fieldDef.isCollection())
        {
	        FieldWriter field_writer = new FieldWriter(docRemap.dstSize());
	        
	        HeapList<IxVal> heap = new HeapList<IxVal>(sources.size() - 1);
	        IxVal current = null;
	        for(int i = 0; i < sources.size(); i++) {
	            current = new IxVal(i, docRemap, valRemap, new FieldSearcher(sources.get(i), table, link));
	            current.next();
	            current = heap.AddEx(current);
	        }
	
	        while (current.doc != Integer.MAX_VALUE)
	        {
	        	field_writer.add(current.doc, current.val);
	            current.next();
	            current = heap.AddEx(current);
	        }
	        
	        field_writer.close(destination, table, link);
            synchronized (this) {
    	        stats.addLinkField(fieldDef, field_writer);
    		}
        }
    }

}
