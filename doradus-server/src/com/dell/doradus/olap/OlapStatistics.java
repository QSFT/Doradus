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

import com.dell.doradus.common.UNode;
import com.dell.doradus.olap.io.VDirectory;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.olap.store.FieldSearcher;
import com.dell.doradus.olap.store.IdSearcher;
import com.dell.doradus.olap.store.NumSearcherMV;
import com.dell.doradus.olap.store.SegmentStats;
import com.dell.doradus.olap.store.ValueSearcher;

public class OlapStatistics {
	
	public static UNode getStatistics(CubeSearcher searcher) {
		//searcher.get
		UNode unode = UNode.createMapNode("statistics");
		UNode fnode = unode.addArrayNode("files");
		VDirectory dir = searcher.getDirectory();
		long total_cl = 0;
		long total_ul = 0;
		//long total
		for(String file: dir.listFiles()) {
			UNode fileNode = fnode.addMapNode("file");
			fileNode.addValueNode("name", file, true);
			long cl = dir.compressedLength(file);
			long ul = dir.fileLength(file);
			fileNode.addValueNode("cmp", fmt(cl), true);
			fileNode.addValueNode("unc", fmt(ul), true);
			total_cl += cl;
			total_ul += ul;
		}
		fnode.addValueNode("cmp", fmt(total_cl), true);
		fnode.addValueNode("unc", fmt(total_ul), true);
		
		SegmentStats stats = searcher.getStats();
		UNode tablesNode = unode.addArrayNode("tables");
		long total_memory = 0;
		for(SegmentStats.Table table: stats.tables.values()) {
			UNode tableNode = tablesNode.addMapNode("table");
			tableNode.addValueNode("name", table.name, true);
			tableNode.addValueNode("docs", fmt(table.documents), true);
			
			long total_num_mem = 0;
			long total_fld_mem = 0;
			long total_val_mem = 0;
			long total_lnk_mem = 0;
			
			UNode idsNode = tableNode.addMapNode("id-field");
			IdSearcher ids = searcher.getIdSearcher(table.name);
			long total_ids_mem = ids.cacheSize();
			
			UNode numsNode = tableNode.addArrayNode("num-fields");
			for(SegmentStats.Table.NumField numField: table.numFields.values()) {
				UNode numNode = numsNode.addMapNode("num");
				NumSearcherMV num = searcher.getNumSearcher(table.name, numField.name);
				long size = num.cacheSize();
				total_num_mem += size;
				numNode.addValueNode("name", numField.name, true);
				numNode.addValueNode("mem", fmt(size), true);
			}
			UNode fldsNode = tableNode.addArrayNode("txt-fields");
			UNode valsNode = tableNode.addArrayNode("val-fields");
			for(SegmentStats.Table.TextField txtField: table.textFields.values()) {
				UNode fldNode = fldsNode.addMapNode("fld");
				UNode valNode = valsNode.addMapNode("val");
				FieldSearcher fld = searcher.getFieldSearcher(table.name, txtField.name);
				long size = fld.cacheSize();
				total_fld_mem += size;
				fldNode.addValueNode("name", txtField.name, true);
				fldNode.addValueNode("mem", fmt(size), true);
				ValueSearcher val = searcher.getValueSearcher(table.name, txtField.name);
				size = val.cacheSize();
				total_val_mem += size;
				valNode.addValueNode("name", txtField.name, true);
				valNode.addValueNode("mem", fmt(size), true);
			}
			UNode lnksNode = tableNode.addArrayNode("lnk-fields");
			for(SegmentStats.Table.LinkField lnkField: table.linkFields.values()) {
				UNode lnkNode = lnksNode.addMapNode("lnk");
				FieldSearcher lnk = searcher.getFieldSearcher(table.name, lnkField.name);
				long size = lnk.cacheSize();
				total_lnk_mem += size;
				lnkNode.addValueNode("name", lnkField.name, true);
				lnkNode.addValueNode("mem", fmt(size), true);
			}

			long total_mem = total_ids_mem + total_num_mem + total_fld_mem + total_lnk_mem + total_val_mem;
			
			tableNode.addValueNode("mem", fmt(total_mem), true);
			idsNode.addValueNode("ids_mem", fmt(total_ids_mem), true);
			numsNode.addValueNode("num_mem", fmt(total_num_mem), true);
			fldsNode.addValueNode("fld_mem", fmt(total_fld_mem), true);
			lnksNode.addValueNode("lnk_mem", fmt(total_lnk_mem), true);
			valsNode.addValueNode("val_mem", fmt(total_val_mem), true);
			total_memory += total_mem;
		}
		tablesNode.addValueNode("memory", fmt(total_memory), true);
		
		
		return unode;
	}
	
	private static String fmt(long val) {
		return String.format("%,8d", val).trim();
	}
}
