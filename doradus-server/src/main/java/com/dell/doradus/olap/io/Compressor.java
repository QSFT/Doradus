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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.dell.doradus.core.ServerConfig;

public class Compressor {
    private static boolean m_bCompress = ServerConfig.getInstance().olap_internal_compression;
    private static int m_compressionLevel = ServerConfig.getInstance().olap_compression_level;
	
	public static byte[] compress(byte[] data) {
		if(data.length == 0) return data;
		if(!m_bCompress) return data;
		try{
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			GZIPOutputStream gos = new GZIPOutputStream(baos){{def.setLevel(m_compressionLevel);}};
			gos.write(data, 0, data.length);
			gos.close();
			byte[] output = baos.toByteArray();
			return output;
        } catch(IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
	}
		
	public static byte[] uncompress(byte[] data) {
		if(data.length == 0) return data;
		if(!m_bCompress) return data;
		try{
			ByteArrayInputStream bais = new ByteArrayInputStream(data);
			GZIPInputStream gis = new GZIPInputStream(bais);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			byte[] buffer = new byte[4096];
			while(true) {
				int read = gis.read(buffer, 0, buffer.length);
				if(read < 0) break;
				baos.write(buffer, 0, read);
			}
			byte[] output = baos.toByteArray();
			return output;
        } catch(IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
	}
}
