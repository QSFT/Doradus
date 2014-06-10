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

package com.dell.doradus.testprocessor;

import com.dell.doradus.testprocessor.common.FileUtils;
import com.dell.doradus.testprocessor.common.StringUtils;
import com.dell.doradus.testprocessor.common.Utils;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class TestSuiteInfo
{
    private List<TestDirInfo> m_dirsInfoList;

    private int m_cntTestsStarted;
    private int m_cntTestsAborted;
    private int m_cntResultsCreated;
    private int m_cntTestsSucceeded;
    private int m_cntTestsFailed;

    public TestSuiteInfo() {
        m_dirsInfoList      = new ArrayList<TestDirInfo>();
        m_cntTestsStarted   = 0;
        m_cntTestsAborted   = 0;
        m_cntResultsCreated = 0;
        m_cntTestsSucceeded = 0;
        m_cntTestsFailed    = 0;
    }

    public List<TestDirInfo> getTestDirInfoList()
        { return m_dirsInfoList; }

    public int nTestsStarted()   { return m_cntTestsStarted;   }
    public int nTestsAborted()   { return m_cntTestsAborted;   }
    public int nResultsCreated() { return m_cntResultsCreated; }
    public int nTestsSucceeded() { return m_cntTestsSucceeded; }
    public int nTestsFailed()    { return m_cntTestsFailed;    }

    public void incTestsStarted()   { ++m_cntTestsStarted;   }
    public void incTestsAborted()   { ++m_cntTestsAborted;   }
    public void incResultsCreated() { ++m_cntResultsCreated; }
    public void incTestsSucceeded() { ++m_cntTestsSucceeded; }
    public void incTestsFailed()    { ++m_cntTestsFailed;    }

    public void includeDirectory(String absolutePath)
    throws Exception
    {
        TestDirInfo dirInfo = findIncludedDir(absolutePath);
        if (dirInfo == null) {
            dirInfo = new TestDirInfo(this, absolutePath);
            m_dirsInfoList.add(dirInfo);
        }

        File   dir  = new File(absolutePath);
        File[] list = dir.listFiles();
        if (list == null) {
            String msg = "Not a directory: '" + absolutePath + "'";
            throw new Exception(msg);
        }

        List<String> subDirsToInclude = new ArrayList<String>();

        for (File file : list)
        {
            String path = file.getAbsolutePath();
            if (file.isFile() && path.endsWith(Data.TEST_SCRIPT_EXTENSION)) {
                dirInfo.includeTest(FileUtils.getTestName(path));
            } else if (file.isDirectory()) {
                subDirsToInclude.add(path);
            }
        }

        for (String path : subDirsToInclude) {
            includeDirectory(path);
        }
    }

    public void includeTests(String absolutePath, List<String> testNames)
    {
        TestDirInfo dirInfo = findIncludedDir(absolutePath);
        if (dirInfo == null) {
            dirInfo = new TestDirInfo(this, absolutePath);
            m_dirsInfoList.add(dirInfo);
        }

        for (String name : testNames) {
            dirInfo.includeTest(name);
        }
    }

    public void excludeDirectory(String absolutePath, String reason)
    {
        List<TestDirInfo> dirsToRemove = findIncludedDirAndSubDirs(absolutePath);
        for (TestDirInfo dir : dirsToRemove) {
            dir.isExcluded(true);
            dir.reasonToExclude(reason);
        }
    }

    public void excludeTests(String absolutePath, List<String> testNames, List<String> reasons)
    {
        TestDirInfo dirInfo = findIncludedDir(absolutePath);
        if (dirInfo == null) return;

        for (int i = 0; i < testNames.size(); i++) {
            dirInfo.excludeTest(testNames.get(i), reasons.get(i));
        }
    }

    private TestDirInfo findIncludedDir(String absolutePath)
    {
        Path path1 = Paths.get(absolutePath);
        for (TestDirInfo dirInfo : m_dirsInfoList) {
            Path path2 = Paths.get(dirInfo.path());
            if (path2.equals(path1)) return dirInfo;
        }
        return null;
    }

    private List<TestDirInfo> findIncludedDirAndSubDirs(String absolutePath)
    {
        List<TestDirInfo> result = new ArrayList<TestDirInfo>();

        Path path1 = Paths.get(absolutePath);
        for (TestDirInfo dirInfo : m_dirsInfoList) {
            Path path2 = Paths.get(dirInfo.path());
            if (path2.startsWith(path1)) result.add(dirInfo);
        }

        return result;
    }

    public String toString(String prefix)
    {
        if (prefix == null) prefix = "";

        StringBuilder result = new StringBuilder();
        for (TestDirInfo dirInfo : m_dirsInfoList) {
            result.append(dirInfo.toString(prefix) + Utils.EOL);
        }

        return StringUtils.trimEnd(result.toString(), "\r\n");
    }
}
