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

package com.dell.doradus.search;

import java.nio.ByteBuffer;

import com.dell.doradus.common.FieldDefinition;
import com.dell.doradus.common.Utils;
import com.dell.doradus.core.ObjectID;
import com.dell.doradus.core.ObjectID.IDFormat;

/**
 * @author otarakan
 * helps handling IDs for objects and links in different cases
 */
public class IDHelper {
	
	public static ObjectID createID(String id) {
		return new ObjectID(id, IDFormat.UFT8);
	}
	
	public static String IDToString(ObjectID id) {
		if(id == null) id = ObjectID.EMPTY;
		return id.toString();
	}
	
	public static byte[] next(byte[] value) {
		byte[] next = new byte[value.length + 1];
		for(int i=0; i<value.length; i++) next[i] = value[i];
		next[value.length] = 0;
		return next;
	}
	
	
	public static byte[] idToBytes(ObjectID id) {
		if(id == null) return ObjectID.EMPTY.bytes();
		else return id.bytes();
	}
	
	public static ObjectID bytesToId(byte[] bytes) {
		return new ObjectID(bytes, IDFormat.UFT8);
	}
	
	public static ObjectID bytesToId(ByteBuffer byteBuffer) {
		return bytesToId(Utils.getBytes(byteBuffer));
	}
	
	public static byte[] linkBoundMinimum(FieldDefinition link) {
		return linkToBytes(link, null);
	}

	public static byte[] linkBoundMaximum(FieldDefinition link) {
		return Utils.toBytes("~" + link.getName() + "0");
	}
	
	public static byte[] linkToBytes(FieldDefinition link, ObjectID id) {
		if(id == null)id = ObjectID.EMPTY;
		byte[] header;
		header = Utils.toBytes("~" + link.getName() + "/");
		return Utils.concatenate(header, id.bytes());
	}
	
	public static ObjectID linkValueToId(byte[] value) {
		int offset = 0;
		while(value[offset] != (byte)'/') offset++;
		offset++;
		byte[] lnk = new byte[value.length - offset];
		for(int i = 0; i < lnk.length; i++) {
			lnk[i] = value[i + offset];
		}
		return new ObjectID(lnk, IDFormat.UFT8);
	}
}
