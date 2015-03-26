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
import java.util.Collections;
import java.util.Comparator;

import com.dell.doradus.common.UNode;
import com.dell.doradus.olap.io.FileInfo;
import com.dell.doradus.olap.io.VDirectory;
import com.dell.doradus.olap.io.VInputStream;
import com.dell.doradus.olap.store.CubeSearcher;
import com.dell.doradus.olap.store.FieldSearcher;
import com.dell.doradus.olap.store.IdSearcher;
import com.dell.doradus.olap.store.NumSearcherMV;
import com.dell.doradus.olap.store.SegmentStats;
import com.dell.doradus.olap.store.ValueSearcher;

public class OlapStatistics {
	
	public static UNode getStatistics(CubeSearcher searcher, String sort, boolean memoryStats) {
		//searcher.get
		UNode unode = UNode.createMapNode("statistics");
		UNode fnode = unode.addArrayNode("files");
		VDirectory dir = searcher.getDirectory();
		long total_cl = 0;
		long total_ul = 0;
		long total_files = 0;
		long total_docs = 0;
		long total_fields_count = 0;
		
		//long total
		ArrayList<FileInfo> files = new ArrayList<>(dir.listFiles());
		if(sort == null || "name".equals(sort)) {
			Collections.sort(files);
		} else if("cmp".equals(sort)) {
			Collections.sort(files, new Comparator<FileInfo>(){
				@Override public int compare(FileInfo x, FileInfo y) {
					return Long.compare(x.getCompressedLength(), y.getCompressedLength());
				}});
		} else if("unc".equals(sort)) {
			Collections.sort(files, new Comparator<FileInfo>(){
				@Override public int compare(FileInfo x, FileInfo y) {
					return Long.compare(x.getLength(), y.getLength());
				}});
		}
		for(FileInfo file: files) {
			UNode fileNode = fnode.addMapNode("file");
			fileNode.addValueNode("name", file.getName(), true);
			long cl = dir.compressedLength(file);
			long ul = file.getLength();
			fileNode.addValueNode("cmp", fmt(cl), true);
			fileNode.addValueNode("unc", fmt(ul), true);
			total_cl += cl;
			total_ul += ul;
			total_files++;
		}
		fnode.addValueNode("files", fmt(total_files), true);
		fnode.addValueNode("cmp", fmt(total_cl), true);
		fnode.addValueNode("unc", fmt(total_ul), true);
		
		if(!memoryStats) return unode;
		
		SegmentStats stats = searcher.getStats();
		UNode tablesNode = unode.addArrayNode("tables");
		long total_memory = 0;
		for(SegmentStats.Table table: stats.tables.values()) {
			UNode tableNode = tablesNode.addMapNode("table");
			tableNode.addValueNode("name", table.name, true);
			tableNode.addValueNode("docs", fmt(table.documents), true);
			total_docs += table.documents;
			
			long total_num_mem = 0;
			long total_fld_mem = 0;
			long total_val_mem = 0;
			long total_lnk_mem = 0;
			long total_nums = 0;
			long total_flds = 0;
			long total_lnks = 0;
			
			UNode idsNode = tableNode.addMapNode("id-field");
			IdSearcher ids = searcher.getIdSearcher(table.name);
			long total_ids_mem = ids.cacheSize();
			idsNode.addValueNode("ids_mem", fmt(total_ids_mem), true);
			
			UNode numsNode = tableNode.addArrayNode("num-fields");
			for(SegmentStats.Table.NumField numField: table.numFields.values()) {
				UNode numNode = numsNode.addMapNode("num");
				NumSearcherMV num = searcher.getNumSearcher(table.name, numField.name);
				long size = num.cacheSize();
				total_num_mem += size;
				numNode.addValueNode("name", numField.name, true);
				numNode.addValueNode("mem", fmt(size), true);
				total_nums++;
			}
			numsNode.addValueNode("flds", fmt(total_nums), true);
			numsNode.addValueNode("num_mem", fmt(total_num_mem), true);
			
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
				total_flds++;
			}
			fldsNode.addValueNode("cnt", fmt(total_flds), true);
			fldsNode.addValueNode("fld_mem", fmt(total_fld_mem), true);
			valsNode.addValueNode("flds", fmt(total_flds), true);
			valsNode.addValueNode("val_mem", fmt(total_val_mem), true);
			
			UNode lnksNode = tableNode.addArrayNode("lnk-fields");
			for(SegmentStats.Table.LinkField lnkField: table.linkFields.values()) {
				UNode lnkNode = lnksNode.addMapNode("lnk");
				FieldSearcher lnk = searcher.getFieldSearcher(table.name, lnkField.name);
				long size = lnk.cacheSize();
				total_lnk_mem += size;
				lnkNode.addValueNode("name", lnkField.name, true);
				lnkNode.addValueNode("mem", fmt(size), true);
				total_lnks++;
			}
			lnksNode.addValueNode("flds", fmt(total_lnks), true);
			lnksNode.addValueNode("lnk_mem", fmt(total_lnk_mem), true);

			long total_mem = total_ids_mem + total_num_mem + total_fld_mem + total_lnk_mem + total_val_mem;
			long total_cnt = total_nums + total_flds + total_lnks;
			total_fields_count += total_cnt;
			
			tableNode.addValueNode("flds", fmt(total_cnt), true);
			tableNode.addValueNode("mem", fmt(total_mem), true);
			total_memory += total_mem;
		}
		tablesNode.addValueNode("docs", fmt(total_docs), true);
		tablesNode.addValueNode("memory", fmt(total_memory), true);
		tablesNode.addValueNode("fields", fmt(total_fields_count), true);
		
		
		return unode;
	}

	public static UNode getFileData(CubeSearcher searcher, String file) {
		UNode unode = UNode.createMapNode("file");
		VDirectory dir = searcher.getDirectory();
		VInputStream strm = dir.open(file);
		int length = (int)Math.min(16 * 1024, strm.length());
		byte[] buffer = new byte[length];
		int len = strm.read(buffer, 0, buffer.length);
		if(len != length) throw new RuntimeException("Failed to read file data");
		
		StringBuilder sb1 = new StringBuilder(3 * len);
		StringBuilder sb2 = new StringBuilder(3 * len);
		for(int i = 0; i < len; i++) {
			byte b = buffer[i];
			sb1.append(String.format("%d ", b));
			if(b < 32) sb2.append('-');
			else sb2.append((char)b);
		}
		unode.addValueNode("data", sb1.toString(), false);
		unode.addValueNode("txt", sb2.toString(), false);
		
		return unode;
	}
	
	
	private static String fmt(long val) {
		return String.format("%,8d", val).trim();
	}
}
