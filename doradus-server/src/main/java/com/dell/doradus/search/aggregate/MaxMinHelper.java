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
import java.util.Collection;
import java.util.Date;
import java.util.List;

import com.dell.doradus.common.TableDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.search.analyzer.DateTrie;
import com.dell.doradus.service.spider.SpiderHelper;

public class MaxMinHelper {
    
    public static ByteBuffer toByteBuffer(String value) {
        return ByteBuffer.wrap(Utils.toBytes(value));
    }
    public static String toString(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return Utils.toString(bytes);
    }
    
    public static Date getMaxDate(TableDefinition tableDefinition, String dateField) {
    	Collection<Integer> shards = SpiderHelper.getShards(tableDefinition);
    	String max = "";
    	for(Integer shard : shards) {
   			String res = SpiderHelper.getLastTerm(tableDefinition, shard, dateField, "0000", "9999");
			if(max.compareTo(res) < 0) max = res;
    	}
    	if(max.length() == 0) return null;
    	else return new DateTrie().parse(max);
    }
    
    public static Date getMinDate(TableDefinition tableDefinition, String dateField) {
    	List<String> dates = SpiderHelper.getTerms(tableDefinition, dateField, "", 1);
    	if(dates.size() == 0) return null;
    	return new DateTrie().parse(dates.get(0));
    }
}
