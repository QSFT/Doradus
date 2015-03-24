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

package com.dell.doradus.olap.io;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

import com.dell.doradus.common.Utils;

public final class Utf8Encoder {
    private final CharsetEncoder utf8_encoder = Utils.UTF8_CHARSET.newEncoder();
    private final CharsetDecoder utf8_decoder = Utils.UTF8_CHARSET.newDecoder();
    private char[] charArray = new char[16 * 1024];

    public int encode(String value, byte[] buffer) {
    	utf8_encoder.reset();
    	if(charArray.length < value.length()) charArray = new char[value.length() * 2];
    	ByteBuffer bb = ByteBuffer.wrap(buffer);
    	value.getChars(0,  value.length(), charArray, 0);
    	CharBuffer cb = CharBuffer.wrap(charArray, 0, value.length());
    	utf8_encoder.encode(cb, bb, true);
    	utf8_encoder.flush(bb);
    	int length = bb.position();
    	return length;
    }
    
    public String decode(byte[] buffer, int length) {
    	utf8_decoder.reset();
    	ByteBuffer bb = ByteBuffer.wrap(buffer, 0, length);
    	if(charArray.length < length) charArray = new char[length * 2];
    	CharBuffer cb = CharBuffer.wrap(charArray);
    	utf8_decoder.decode(bb, cb, true);
    	utf8_decoder.flush(cb);
    	return new String(charArray, 0, cb.position());
    }
    
}
