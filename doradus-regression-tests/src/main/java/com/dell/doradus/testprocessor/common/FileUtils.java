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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileUtils
{
    static public String combinePaths(String baseDir, String relativePath)
    {
    	if (baseDir == null || baseDir.trim().isEmpty())
    		return relativePath;
    	
        Path basePath = Paths.get(baseDir);

        Path path = Paths.get(relativePath);
        path = basePath.resolve(path);
        path = path.normalize();
        
        return path.toString();
    }
    
    static public String getFileName(String filePath)
    {
    	Path path = Paths.get(filePath);
    	return path.getFileName().toString();
    }
    
    static public String getFileNameWithoutExtension(String filePath)
    {
    	String name = getFileName(filePath);
    	return name.replaceFirst("[.][^.]+$", "");
    }

    static public String getTestName(String testFile)
    {
        return FileUtils.getFileNameWithoutExtension(
                FileUtils.getFileNameWithoutExtension(testFile));
    }

    static public String getDirectory(String filePath)
    {
    	Path path = Paths.get(filePath);
    	return path.getParent().toString();
    }
    
    static public boolean fileExists(String filePath)
    {
    	return (new File(filePath)).exists();
    }

    static public String readAllText(String filePath)
    throws Exception
    {
        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        StringBuilder  result = new StringBuilder();

        String line = null;
        while((line = reader.readLine()) != null)
            result.append(line).append("\r\n");

        reader.close();
        return result.toString();
    }
    
    static public void deleteFile(String filePath)
    {
    	(new File(filePath)).delete();
    }
}
