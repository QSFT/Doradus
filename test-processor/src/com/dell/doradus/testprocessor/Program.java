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

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

public class Program
{
    static public void main(String args[])
    {
        try {
            Data.predefine();

            if (Data.configFilePath == null || Data.configFilePath.trim().isEmpty()) {
                System.out.println("Config file is not define: Using predefined data\"");
            }
            else {
                System.out.println("Loading config \"" + Data.configFilePath + "\"");
                Config.load(Data.configFilePath);
                Config.modifyData();
            }

            Log.toFile(Data.logFilePath);
            if (Log.isOpened()) {
                System.out.println("Log: \"" + Data.logFilePath + "\"");
            }

            Log.println("*** Program: Test Processor Data:");
            Log.println(Data.toString("    "));

            if (Data.testSuiteInfo == null) {
                String msg = "Test suite is not defined";
                throw new Exception(msg);
            }

            Log.println("*** Program: Running Tests");

            System.out.println("Running Tests");
            for (TestDirInfo dirInfo : Data.testSuiteInfo.getTestDirInfoList()) {
                System.out.println("Directory: " + dirInfo.path());
                if (dirInfo.isExcluded()) {
                    System.out.println("  Excluded");
                    continue;
                }

                for (TestInfo testInfo : dirInfo.testInfoList()) {
                    System.out.print("  " + testInfo.name() + ": ");
                    if (testInfo.isExcluded()) {
                        System.out.println("excluded");
                        continue;
                    }

                    TestProcessor.execute(testInfo);
                    System.out.println(
                        !testInfo.isStarted()      ? "not started" :
                        testInfo.isAborted()       ? "aborted" :
                        testInfo.isResultCreated() ? "result file created" :
                        testInfo.isSucceeded()     ? "succeeded" :
                        testInfo.isFailed()        ? "failed" :
                        "???");
                }
            }

            writeHtmlTestReport();
            writeXmlTestTestSummaryForCcnet();
        }
        catch (Exception ex) {
            Log.println("!!! Exception: " + Utils.unwind(ex));
            System.out.println("!!! Exception: " + Utils.unwind(ex));
        }
        finally {
            Log.close();
        }

        System.out.println(Utils.EOL + "Summary:");
        TestsSummary summary = new TestsSummary(Data.testSuiteInfo);
        System.out.println(summary.toString("  "));
    }

    static private void writeHtmlTestReport()
    throws Exception
    {
        if (Data.reportFilePath == null)
            return;

        Log.println("*** Program: Generating tests report:  " + Data.reportFilePath);
        System.out.println(Utils.EOL + "Generated report: \"" + Data.reportFilePath + "\"");

        if (FileUtils.fileExists(Data.reportFilePath))
            FileUtils.deleteFile(Data.reportFilePath);

        OutputStream reportOutputStream = new FileOutputStream(Data.reportFilePath);
        Writer reportWriter = new OutputStreamWriter(reportOutputStream);

        String report = Reporter.generateHtmlReport(Data.testSuiteInfo);

        reportWriter.write(report);
        reportWriter.flush();
        reportWriter.close();
    }

    static private void writeXmlTestTestSummaryForCcnet()
    throws Exception
    {
        if (Data.ccnetSummaryFilePath == null)
            return;

        Log.println("*** Program: Generating tests summary for ccnet:  " + Data.ccnetSummaryFilePath);

        if (FileUtils.fileExists(Data.ccnetSummaryFilePath))
            FileUtils.deleteFile(Data.ccnetSummaryFilePath);

        OutputStream summaryOutputStream = new FileOutputStream(Data.ccnetSummaryFilePath);
        Writer summaryWriter = new OutputStreamWriter(summaryOutputStream);

        String summary = Reporter.generateXmlSummaryForCCNet(Data.testSuiteInfo);

        summaryWriter.write(summary);
        summaryWriter.flush();
        summaryWriter.close();
    }
}
