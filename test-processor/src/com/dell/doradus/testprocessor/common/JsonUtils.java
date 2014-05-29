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

import com.dell.doradus.common.UNode;

public class JsonUtils
{
	static public String formatJson(String jsonText, String prefix)
    throws Exception
	{
		UNode unode = UNode.parseJSON(jsonText);
        return StringUtils.formatText(unode.toJSON(true), prefix);
	}
	
	static public String convertXmlToJson(String xmlText)
    throws Exception
	{
		UNode  unodeRoot = UNode.parseXML(xmlText);
		String jsonText  = unodeRoot.toJSON();

		return jsonText;
	}
}
