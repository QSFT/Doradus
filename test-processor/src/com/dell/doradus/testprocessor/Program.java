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
import com.dell.doradus.testprocessor.diff.*;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

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
            for (TestDirInfo testDirInfo : Data.testSuiteInfo.getTestDirInfoList()) {
                System.out.println("Directory: " + testDirInfo.path());

                for (TestInfo testInfo : testDirInfo.testInfoList()) {
                    if (testInfo.isExcluded()) continue;
                    System.out.print("   " + testInfo.name() + ": ");
                    try { TestProcessor.execute(testInfo); }
                    catch(Exception ex) { System.out.println(); }
                    System.out.println(testInfo.resultToString());
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
        displaySummary("  ");
    }

    static public void displaySummary(String prefix)
    {
        if (prefix == null) prefix = "";

        int cntTests         = 0;
        int cntSucceeded     = 0;
        int cntFailed        = 0;
        int cntInterrupted   = 0;
        int cntNotExecuted   = 0;
        int cntResultCreated = 0;
        
        if (Data.testSuiteInfo == null) {
            System.out.println(prefix + "<No test results found>");
            return;
        }

        for (TestDirInfo testDirInfo : Data.testSuiteInfo.getTestDirInfoList()) {
            if (testDirInfo.isExcluded()) continue;

            for (TestInfo testInfo : testDirInfo.testInfoList()) {
                if (testInfo.isExcluded())
                    continue;

                cntTests += 1;
                if (testInfo.isInterrupted())
                    { cntInterrupted += 1; continue; }
                if (testInfo.isSucceeded())
                    { cntSucceeded += 1; continue; }
                if (testInfo.requiredResultFileCreated())
                    { cntResultCreated += 1; continue; }
                if (!testInfo.isExecuted())
                    { cntNotExecuted += 1; continue; }
                cntFailed += 1;
            }
        }

        //System.out.println(prefix + "Number of test(s): " + cntTests);
        System.out.println(prefix + "Succeeded: " + cntSucceeded);
        if (cntFailed > 0)
            System.out.println(prefix + "Failed: " + cntFailed);
        if (cntInterrupted > 0)
            System.out.println(prefix + "Interrupted: " + cntInterrupted);
        if (cntResultCreated > 0)
            System.out.println(prefix + "Result(s) created: " + cntResultCreated);
        if (cntNotExecuted > 0)
            System.out.println(prefix + "Not executed: " + cntNotExecuted);
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
