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

package com.dell.doradus.olap.collections;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

import com.dell.doradus.common.Utils;

public final class UTF8 {
    private final CharsetEncoder utf8_encoder = Utils.UTF8_CHARSET.newEncoder();
    private final CharsetDecoder utf8_decoder = Utils.UTF8_CHARSET.newDecoder();

    public static void toLower(char[] src, int srcOffset, int srcLength) {
    	for(int i = 0; i < srcLength; i++) {
    		char c = src[i];
    		if(c < 0x80) {
    			 if(c >= 'A' && c <= 'Z') c += 32;
    			 src[i] = c;
    		}
    		else src[i] = Character.toLowerCase(c);
    	}
    }
    
    
    public int encode(char[] src, int srcOffset, int srcLength, byte[] dst, int dstOffset) {
    	for(int i = 0; i < srcLength; i++) {
    		if(src[srcOffset + i] > 0x80) {
    			int len = encodeInternal(src, srcOffset + i, srcLength - i, dst, dstOffset + i);
    			return i + len;
    		} else {
        		dst[dstOffset + i] = (byte)src[srcOffset + i];
    		}
    	}
    	return srcLength;
    }
    
    public int decode(byte[] src, int srcOffset, int srcLength, char[] dst, int dstOffset) {
    	for(int i = 0; i < srcLength; i++) {
    		if(src[srcOffset + i] > 0x80) {
    			int len = decodeInternal(src, srcOffset + i, srcLength - i, dst, dstOffset + i);
    			return i + len;
    		} else {
        		dst[dstOffset + i] = (char)src[srcOffset + i];
    		}
    	}
    	return srcLength;
    }
    
    private int encodeInternal(char[] src, int srcOffset, int srcLength, byte[] dst, int dstOffset) {
    	CharBuffer cb = CharBuffer.wrap(src, srcOffset, srcLength);
    	ByteBuffer bb = ByteBuffer.wrap(dst, dstOffset, dst.length - dstOffset);
    	utf8_encoder.reset();
    	utf8_encoder.encode(cb, bb, true);
    	utf8_encoder.flush(bb);
    	int length = bb.position();
    	return length;
    }
    
    private int decodeInternal(byte[] src, int srcOffset, int srcLength, char[] dst, int dstOffset) {
    	ByteBuffer bb = ByteBuffer.wrap(src, srcOffset, srcLength);
    	CharBuffer cb = CharBuffer.wrap(dst);
    	utf8_decoder.reset();
    	utf8_decoder.decode(bb, cb, true);
    	utf8_decoder.flush(cb);
    	int length = cb.position();
    	return length;
    }
    
}
