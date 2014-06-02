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

import com.dell.doradus.testprocessor.common.*;

public class Data
{
    static public String TEST_SCRIPT_EXTENSION     = ".test.xml";
    static public String REQUIRED_RESULT_EXTENSION = ".result.txt";
    static public String OBTAINED_RESULT_EXTENSION = ".xresult.txt";
    static public String DIFF_RESULT_EXTENSION     = ".xdiff.txt";

    static public String    workingDir;
    static public String    configFilePath;
    static public String    logFilePath;
    static public String    reportFilePath;
    static public String    ccnetSummaryFilePath;
    static public String    doradusHost;
    static public int       doradusPort;
    static public boolean   outputEnabled;
    static public String    responseFormat;

    static public TestSuiteInfo testSuiteInfo;

    static public void predefine()
    {
        workingDir      = System.getProperty("user.dir");
        configFilePath  = FileUtils.combinePaths(Data.workingDir, "..\\config\\config.xml");

        logFilePath          = null;
        reportFilePath       = null;
        ccnetSummaryFilePath = null;
        doradusHost          = "localhost";
        doradusPort          = 1123;
        outputEnabled        = true;
        responseFormat       = "xml";
        testSuiteInfo        = null;
    }

    static public String toString(String prefix)
    {
        if (prefix == null) prefix = "";

        StringBuilder result = new StringBuilder();

        result.append(prefix + "Working Directory: " + stringOrNull(workingDir)           + Utils.EOL);
        result.append(prefix + "Config File:       " + stringOrNull(configFilePath)       + Utils.EOL);
        result.append(prefix + "Log File:          " + stringOrNull(logFilePath)          + Utils.EOL);
        result.append(prefix + "Report File:       " + stringOrNull(reportFilePath)       + Utils.EOL);
        result.append(prefix + "ccnetSummary File: " + stringOrNull(ccnetSummaryFilePath) + Utils.EOL);
        result.append(prefix + "Doradus Host:      " + stringOrNull(doradusHost)          + Utils.EOL);
        result.append(prefix + "Doradus Port:      " + doradusPort                        + Utils.EOL);
        result.append(prefix + "Output Enabled:    " + outputEnabled                      + Utils.EOL);
        result.append(prefix + "Response Format:   " + stringOrNull(responseFormat)       + Utils.EOL);
        if (testSuiteInfo == null) {
            result.append(prefix + "Test Suite:        <null>" + Utils.EOL);
        } else {
            result.append(prefix + "Test Suite:" + Utils.EOL +
                testSuiteInfo.toString(prefix + "    ") + Utils.EOL);
        }
        return StringUtils.trimEnd(result.toString(), "\r\n");
    }

    static private String stringOrNull(String str) {
        return (str != null) ? str : "<null>";
    }
}
