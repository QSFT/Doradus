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

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Map;

import com.dell.doradus.testprocessor.common.FileUtils;
import com.dell.doradus.testprocessor.common.Utils;
import com.dell.doradus.testprocessor.diff.*;
import com.dell.doradus.testprocessor.xmlreflector.*;

public class TestProcessor
{
    static boolean m_ignoreWhiteSpace = true;
    static boolean m_ignoreCase = true;
    static Differ  m_differ = new Differ(m_ignoreWhiteSpace, m_ignoreCase);

    static public void execute(TestInfo testInfo)
    {
        try
        {
            testInfo.isStarted(true);

            TestDirInfo dirInfo = testInfo.testDirInfo();
            String      dirPath = dirInfo.path();

            String testName = testInfo.name();
            String testPath = dirPath + "\\" + testName + Data.TEST_SCRIPT_EXTENSION;

            String obtainedResultPath = dirPath + "\\" + testName + Data.OBTAINED_RESULT_EXTENSION;
            if (FileUtils.fileExists(obtainedResultPath)) {
                FileUtils.deleteFile(obtainedResultPath);
            }
            String diffResultPath = dirPath + "\\" + testName + Data.DIFF_RESULT_EXTENSION;
            if (FileUtils.fileExists(diffResultPath)) {
                FileUtils.deleteFile(diffResultPath);
            }

            String requiredResultPath = dirPath + "\\" + testName + Data.REQUIRED_RESULT_EXTENSION;
            if (FileUtils.fileExists(requiredResultPath))
            {
                execute(testPath, obtainedResultPath);

                CompareResult compareResult = m_differ.compareFiles(
                        requiredResultPath, obtainedResultPath);

                if (compareResult.identical) {
                    testInfo.isSucceeded(true);
                    FileUtils.deleteFile(obtainedResultPath);
                } else {
                    testInfo.isSucceeded(false);
                    compareResult.write(diffResultPath);
                }
            } else {
                TestProcessor.execute(testPath, requiredResultPath);
                testInfo.isResultCreated(true);
            }

        }
        catch(Exception ex) {
            testInfo.isAborted(true);
            testInfo.reasonToAbort(Utils.unwind(ex));
        }
    }

    static public void execute(String testFile, String resultFile)
    throws Exception
    {
        Writer resultWriter = null;

        try {
            Log.println("*** Executing test script  \"" + testFile + "\"");
            
            OutputStream resultOutputStream = new FileOutputStream(resultFile);
            resultWriter = new OutputStreamWriter(resultOutputStream);
            
            XMLReflector xmlReflector = new XMLReflector();
            XDefinitions definitions  = xmlReflector.definitions();

            xmlReflector.ignoreCase(true);
            xmlReflector.skipNotReflected(false);

            definitions.setString (TestProcessorReflector.VAR_DORADUS_HOST,    Data.doradusHost);
            definitions.setInteger(TestProcessorReflector.VAR_DORADUS_PORT,    Data.doradusPort);
            definitions.setBoolean(TestProcessorReflector.VAR_OUTPUT_ENABLED,  Data.outputEnabled);
            definitions.setString (TestProcessorReflector.VAR_RESPONSE_FORMAT, Data.responseFormat);
            
            xmlReflector.include(MiscReflector.class);
            xmlReflector.include(ControlStmtReflector.class);
            xmlReflector.include(TestProcessorReflector.class);
            
            Map<String, Object> commonData = xmlReflector.commonData();
            TestProcessorReflector xdoradusInstance = (TestProcessorReflector) commonData.get("xdoradus");
            xdoradusInstance.setResultWriter(resultWriter);

            xmlReflector.processXmlFile(testFile);
        }
        catch(Exception ex) {
            String msg = "Failed to execute test script \"" + testFile + "\"";
            throw new Exception(msg, ex);
        }
        finally {
            if (resultWriter != null)
                resultWriter.close();
        }
    }
}
