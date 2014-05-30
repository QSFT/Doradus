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

package com.dell.doradus.testprocessor.common;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

public class Utils
{
	static public final String EOL            = System.getProperty("line.separator");
	static public final String PATH_SEPARATOR = System.getProperty("file.separator");

    static public String currentGMTDateTime()
    {
        TimeZone gmtTZ = TimeZone.getTimeZone("Europe/London");
        Calendar gmtCalendar = GregorianCalendar.getInstance();
        gmtCalendar.setTimeZone(gmtTZ);
        Date date = gmtCalendar.getTime();
        return date.toString();
    }

    static public String unwind(Exception ex)
    {
        StringBuilder msg = new StringBuilder();
        for (Exception cause = ex; cause != null; cause = (Exception) cause.getCause()) {
            msg.append(cause.getMessage() + EOL);
        }
        return msg.toString();
    }
    
    static public String urlEncode(String src)
    {
        if (src == null)
        	return null;

        try { return URLEncoder.encode(src, "UTF-8"); }
    	catch (UnsupportedEncodingException e) {
    		// This should never happen since UTF-8 always exists.
    		throw new IllegalArgumentException("UTF-8");
    	}
    }
    
    public static String urlDecode(String src)
    {
        if (src == null)
            return null;

        try { return URLDecoder.decode(src, "UTF-8"); }
        catch (UnsupportedEncodingException e) {
            // This should never happen since UTF-8 always exists.
            throw new IllegalArgumentException("UTF-8");
        }
    }
    
    static public Object convert(String value, Class tgtType)
    throws Exception
    {
        try {
            if (tgtType == String.class)
                return value;
            if (tgtType == char.class || tgtType == Character.class)
                return value.charAt(0);
            if (tgtType == byte.class || tgtType == Byte.class)
                return Byte.parseByte(value);
            if (tgtType == short.class || tgtType == Short.class)
                return Short.parseShort(value);
            if (tgtType == int.class || tgtType == Integer.class)
                return Integer.parseInt(value);
            if (tgtType == long.class || tgtType == Long.class)
                return Long.parseLong(value);
            if (tgtType == boolean.class || tgtType == Boolean.class)
                return Boolean.parseBoolean(value);

            String msg = "Converting to the " + tgtType.getName() + " type is not supported";
            throw new Exception(msg);
        }
        catch(Exception ex) {
            String msg = "Failed to convert \"" + value + "\" to the " + tgtType.getName() + " type";
            throw new Exception(msg, ex);
        }
    }
}
