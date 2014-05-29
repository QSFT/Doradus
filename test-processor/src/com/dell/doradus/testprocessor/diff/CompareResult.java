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

package com.dell.doradus.testprocessor.diff;

import java.io.FileWriter;
import java.util.List;

import com.dell.doradus.testprocessor.common.Utils;

public class CompareResult
{
    public List<DiffLine> diffLines;
    public boolean        identical;

    public CompareResult(List<DiffLine> diffLines, boolean identical) {
        this.diffLines = diffLines;
        this.identical = identical;
    }
    
    public void write(String filePath)
    throws Exception
    {
    	try {
    		FileWriter writer = new FileWriter(filePath);
    		writer.write(toString());
			writer.close();
    	} catch (Exception ex) {
    		String msg = "Failed to write to \"" + filePath + "\"";
            throw new Exception(msg, ex);
    	}
    }

    public String toString()
    {
        StringBuilder result = new StringBuilder();

        for (int i = 0; i < diffLines.size(); i++) {
        	result.append(diffLines.get(i).toString() + Utils.EOL);
        }
        
        return result.toString();
    }
}
