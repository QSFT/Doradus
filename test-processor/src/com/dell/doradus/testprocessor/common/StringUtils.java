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

import java.util.ArrayList;
import java.util.List;

public class StringUtils
{
    static public String trimStart(String text, String whiteSpaces)
    {
    	if (text == null || text.length() == 0)
    		return text;
    	
    	int ind = 0;
    	while (whiteSpaces.indexOf(text.charAt(ind)) != -1)
    		ind += 1;
    	return text.substring(ind);
    }
    
    static public String trimEnd(String text, String whiteSpaces)
    {
    	if (text == null || text.length() == 0)
    		return text;
    	
    	int len = text.length();
    	while (whiteSpaces.indexOf(text.charAt(len - 1)) != -1)
    		len -= 1;
    	return text.substring(0, len);
    }
    
    static public String trim(String text, String whiteSpaces)
    {
    	if (text == null || text.length() == 0)
    		return text;
    	
    	int ind = 0;
    	int len = text.length();
    	
    	while (whiteSpaces.indexOf(text.charAt(ind)) != -1)
    		ind += 1;
    	while (whiteSpaces.indexOf(text.charAt(len - 1)) != -1)
    		len -= 1;
    	return text.substring(ind, len);
    }

	static public String formatText(String text, String prefix)
	{
        if (prefix == null) prefix = "";

        StringBuilder result = new StringBuilder();

        String[] lines = text.split("\\r?\\n");
        for (int i = 0; i < lines.length; i++) {
            result.append(prefix + lines[i] + Utils.EOL);
        }

        return trimEnd(result.toString(), " \r\n\t");
	}

	static public List<String> split(String src, char separator, boolean trim)
    {
        List<String>  result = new ArrayList<String>();

        for (int i1 = 0; i1 < src.length(); )
        {
            int i2 = src.indexOf(separator, i1);
            if (i2 < 0) i2 = src.length();

            String item = src.substring(i1, i2);
            if (trim) item = item.trim();

            result.add(item);
            i1 = i2 + 1;
        }

        return result;
    }
}
