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
import com.dell.doradus.testprocessor.config.ConfigReflector;
import com.dell.doradus.testprocessor.xmlreflector.*;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class Config
{
    static public String        logFilePath     = null;
    static public String        reportFilePath  = null;
    static public String        doradusHost     = null;
    static public Integer       doradusPort     = null;
    static public Boolean       outputEnabled   = null;
    static public String        responseFormat  = null;
    static public TestSuiteInfo testSuiteInfo   = null;

    static public void load(String configFile)
    throws Exception
    {
        try {
            XMLReflector xmlReflector = new XMLReflector();

            xmlReflector.ignoreCase(true);
            xmlReflector.skipNotReflected(true);

            xmlReflector.include(MiscReflector.class);
            xmlReflector.include(ControlStmtReflector.class);
            xmlReflector.include(ConfigReflector.class);
            xmlReflector.processXmlFile(configFile);
        }
        catch(Exception ex) {
            String msg = "Failed to process config file \"" + configFile + "\"";
            throw new Exception(msg, ex);
        }
    }

    static public void modifyData()
    {
        if (logFilePath    != null) Data.logFilePath    = logFilePath;
        if (reportFilePath != null) Data.reportFilePath = reportFilePath;
        if (doradusHost    != null) Data.doradusHost    = doradusHost;
        if (doradusPort    != null) Data.doradusPort    = doradusPort;
        if (outputEnabled  != null) Data.outputEnabled  = outputEnabled;
        if (responseFormat != null) Data.responseFormat = responseFormat;
        if (testSuiteInfo  != null) Data.testSuiteInfo  = testSuiteInfo;
    }

    static public String toString(String prefix)
    {
        if (prefix == null) prefix = "";

        StringBuilder result = new StringBuilder();

        if (logFilePath    != null) result.append(prefix + "Log File:        " + logFilePath    + Utils.EOL);
        if (reportFilePath != null) result.append(prefix + "Report File:     " + reportFilePath + Utils.EOL);
        if (doradusHost    != null) result.append(prefix + "Doradus Host:    " + doradusHost    + Utils.EOL);
        if (doradusPort    != null) result.append(prefix + "Doradus Port:    " + doradusPort    + Utils.EOL);
        if (outputEnabled  != null) result.append(prefix + "Output Enabled:  " + outputEnabled  + Utils.EOL);
        if (responseFormat != null) result.append(prefix + "Response Format: " + responseFormat + Utils.EOL);
        if (testSuiteInfo == null) {
            result.append(prefix + "Test Suite:      <null>" + Utils.EOL);
        } else {
            result.append(prefix + "Test Suite:" + Utils.EOL +
                    testSuiteInfo.toString(prefix + "    ") + Utils.EOL);
        }

        return StringUtils.trimEnd(result.toString(), "\r\n");
    }
}
