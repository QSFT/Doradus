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
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.common.ApplicationDefinition;
import com.dell.doradus.olap.io.FileInfo;
import com.dell.doradus.olap.io.VDirectory;
import com.dell.doradus.olap.io.VInputStream;
import com.dell.doradus.olap.store.FieldSearcher;
import com.dell.doradus.olap.store.IdReader;
import com.dell.doradus.olap.store.NumSearcherMV;
import com.dell.doradus.olap.store.SegmentStats;
import com.dell.doradus.olap.store.ValueReader;


public class CheckDatabase {
    private static Logger LOG = LoggerFactory.getLogger("CheckDatabase");
    

    /**
     * check that all segments in the shard are valid 
     */
    public static void checkShard(Olap olap, ApplicationDefinition appDef, String shard) {
        VDirectory appDir = olap.getRoot(appDef);
        VDirectory shardDir = appDir.getDirectory(shard);
        checkShard(shardDir);
    }
    
    /**
     * delete a particular segment in the shard.  
     */
    public static void deleteSegment(Olap olap, ApplicationDefinition appDef, String shard, String segment) {
        VDirectory appDir = olap.getRoot(appDef);
        VDirectory shardDir = appDir.getDirectory(shard);
        VDirectory segmentDir = shardDir.getDirectory(segment);
        segmentDir.delete();
    }
    
    private static void checkShard(VDirectory shardDir) {
        LOG.info("  Checking shard " + shardDir.getName());
        List<String> segments = shardDir.listDirectories();
        for(String segment: segments) {
            try {
                VDirectory segmentDir = shardDir.getDirectory(segment);
                checkSegment(segmentDir);
            }catch(Exception e) {
                throw new RuntimeException("Error in shard '" + shardDir.getName() + "' segment '" + segment + "'", e);
            }
        }
        LOG.info("  Successfully checked " + segments.size() + " segments");
    }
    
    
    private static void checkSegment(VDirectory segmentDir) {
        LOG.info("    Checking segment " + segmentDir.getName());
        // 1. Check files
        ArrayList<FileInfo> files = new ArrayList<>(segmentDir.listFiles());
        for(FileInfo file: files) {
            VInputStream input = segmentDir.open(file.getName());
            while(!input.end()) input.readByte();
        }
        // 2. Check data
        SegmentStats stats = SegmentStats.load(segmentDir);
        for(SegmentStats.Table table: stats.tables.values()) {
            IdReader idReader = new IdReader(segmentDir, table.name);
            while(idReader.next());
            for(SegmentStats.Table.LinkField field: table.linkFields.values()) {
                FieldSearcher fs = new FieldSearcher(segmentDir, table.name, field.name);
                for(int doc = 0; doc < fs.size(); doc++) {
                    int fc = fs.fieldsCount(doc);
                    for(int fld = 0; fld < fc; fld++) {
                        fs.getField(doc, fld);
                    }
                }
            }
            for(SegmentStats.Table.NumField field: table.numFields.values()) {
                NumSearcherMV ns = new NumSearcherMV(segmentDir, table.name, field.name);
                for(int doc = 0; doc < ns.size(); doc++) {
                    int fc = ns.size(doc);
                    for(int fld = 0; fld < fc; fld++) {
                        ns.get(doc, fld);
                    }
                }
            }
            for(SegmentStats.Table.TextField field: table.textFields.values()) {
                FieldSearcher fs = new FieldSearcher(segmentDir, table.name, field.name);
                for(int doc = 0; doc < fs.size(); doc++) {
                    int fc = fs.fieldsCount(doc);
                    for(int fld = 0; fld < fc; fld++) {
                        fs.getField(doc, fld);
                    }
                }
                ValueReader vr = new ValueReader(segmentDir, table.name, field.name);
                while(vr.next());
            }
        }
    }
    
}
