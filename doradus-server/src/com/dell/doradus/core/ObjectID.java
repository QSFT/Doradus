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

package com.dell.doradus.core;

import java.util.Arrays;

import com.dell.doradus.common.Utils;

/**
 * Holds an object ID value as a byte array
 */
final public class ObjectID implements Comparable<ObjectID> {
	public enum IDFormat {
		UNKNOWN,
		UFT8,
		BASE64	
	}
	
	public static final ObjectID EMPTY = new ObjectID(new byte[0], IDFormat.UNKNOWN);
	
	private final IDFormat m_format;
    private final byte[] m_value;
    
    public ObjectID(byte[] value, IDFormat format) {
    	m_value = value;
    	m_format = format;
    }

    public ObjectID(String id, IDFormat format) {
		m_format = format;
    	if(id == null || id.length() == 0) {
    		m_value = new byte[0];
    		return;
    	}
    	switch(m_format) {
	    	case UFT8: m_value = Utils.toBytes(id); break;
	    	case BASE64: {
	    		m_value = Utils.base64ToBinary(id);
	    		break;
	    	}
	    	default: throw new RuntimeException("Unknown ID format: " + m_format.toString());
		}
    }
    
    public byte[] bytes() { return m_value; }
    public IDFormat format() { return m_format; }
    
    @Override public int compareTo(ObjectID other) {
    	int len = m_value.length < other.m_value.length ? m_value.length : other.m_value.length;
    	for(int i = 0; i < len; i++) {
    		if(m_value[i] != other.m_value[i]) return (int)(char)m_value[i] - (int)(char)other.m_value[i];
    	}
        return m_value.length - other.m_value.length;
    }
    
    @Override public boolean equals(Object other) {
        if (!(other instanceof ObjectID)) {
            return false;
        }
        return Arrays.equals(m_value, ((ObjectID)other).m_value);
    }
    
    @Override public int hashCode() {
        return Arrays.hashCode(m_value);
    }
    
    @Override
    public String toString() {
    	if(m_value == null || m_value.length == 0) return "";
    	switch(m_format) {
	    	case UFT8: return Utils.toString(m_value);
	    	case BASE64: return Utils.base64FromBinary(m_value);
	    	default: throw new RuntimeException("Unknown ID format: " + m_format.toString());
    	}
    }
}
