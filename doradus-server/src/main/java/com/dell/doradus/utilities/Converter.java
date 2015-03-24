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

package com.dell.doradus.utilities;

import java.util.Date;

import com.dell.doradus.common.Utils;

/**
 * Converts any of the following values to all other possible values:
 * </pre>
 *      Binary value in Base64
 *      Binary value in hex
 *      Date.getTime() value as an integer (e.g. 1355356800000)
 *      Timestamp value in string form (e.g., 2012-12-12 12:12:12.123)
 * </pre>
 * Just enter a value and it is converted to all forms that make sense and displayed.
 */
public class Converter {

    /**
     * @param args
     */
    public static void main(String[] args) {
        if (args.length > 0) {
            // Batch mode
            for (String arg : args) {
                convert(arg);
            }
        } else {
            // Interactive mode
            while (true) {
                System.out.print("Enter value: ");
                String str = System.console().readLine();
                if (str == null || str.length() == 0) {
                    break;
                }
                convert(str);
            }
        }
    }   // main

    private static void convert(String str) {
        // Display any value that works
        convertUnicodeToHex(str);
        convertHexToBase64(str);
        convertHexToUnicode(str);
        convertBase64ToHex(str);
        convertDateString(str);
        convertDateValue(str);
    }   // convert

    private static void convertUnicodeToHex(String str) {
        try {
            display("Unicode to hex: " + Utils.toHexBytes(Utils.toBytes(str)));
        } catch (Exception e) {
        }
    }   // convertUnicodeToHex
    
    private static void convertHexToBase64(String str) {
        try {
            display("Hex to base64: " + Utils.base64FromHex(str));
        } catch (Exception e) {
        }
    }   // convertHexToBase64
    
    private static void convertHexToUnicode(String str) {
        try {
            display("Hex to Unicode: " + Utils.toString(Utils.base64ToBinary(Utils.base64FromHex(str))));
        } catch (Exception e) {
        }
    }   // convertHexToUnicode
    
    private static void convertBase64ToHex(String str) {
        try {
            display("Base64 to hex: " + Utils.base64ToHex(str));
        } catch (Exception e) {
        }
    }   // convertBase64ToHex
    
    private static void convertDateString(String str) {
        try {
            Date date = Utils.dateFromString(str);
            display("Date to millis: " + date.getTime());
        } catch (Exception e) {
        }
    }   // convertDateString
    
    private static void convertDateValue(String str) {
        try {
            display("Millis to date: " + Utils.formatDateUTC(new Date(Long.parseLong(str))));
        } catch (Exception e) {
        }
    }   // convertDateString
    
    private static void display(String msg) {
        System.out.println(msg);
    }   // display
    
}   // class Converter
