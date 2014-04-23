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

package com.dell.doradus.management;

/**
 * Compatibility of Cron expressions between Quartz cron expressions and
 * SauronSoftware cron expressions. The class contains static functions to
 * convert cron expressions in both directions.  
 */
public final class CronUtils {
	/**
	 * No instances of the class permitted
	 */
	private CronUtils() {}
	
	/**
	 * Converting valid Quartz cron expression to valid SauronSoftware one.
	 * The conversions are the following:
	 * <ul><li>&quot;seconds&quot; part eliminated;</li>
	 * <li>numbers in &quot;day of week&quot; started from 0, not from 1 as in Quartz;</li>
	 * <li>&quot;value/interval&quot; items converted to
	 * &quot;value-maxValue/interval&quot; items;</li>
	 * <li>&quot;/interval&quot; items converted to &quot;*&#47;interval items&quot;;</li>
	 * <li>W in dates changed to MON-FRI in days of week;</li>
	 * <li>L in day of week changed to 24-L in dates. It is not equivalent conversion,
	 * but at least back conversion is made correctly;</li>
	 * <li>#n part in day of weeks is eliminated. Does anybody use it?</li>
	 * <li>all question marks are changed to asterisks.</li>
	 * </ul>
	 * @param quartzExpr	Valid Quartz cron expression
	 * @return				Similar SauronSoftware cron expression
	 */
	public static String packSchedule(String quartzExpr) {
		if (quartzExpr == null) return null;
		String[] exprElems = quartzExpr.trim().split("\\s+");
		if (exprElems.length > 5) {
			// Quartz cron expression contains secs, mins, hours, dates, months, days [, years]
			// 1. Change days of week numbering
			exprElems[5] = decreaseDoW(exprElems[5]);
			// 2. Add interval in repeated items
			exprElems[1] = extendRepeating(exprElems[1], "59"); 
			exprElems[2] = extendRepeating(exprElems[2], "23"); 
			exprElems[3] = extendRepeating(exprElems[3], "L"); 
			exprElems[4] = extendRepeating(exprElems[4], "12"); 
			exprElems[5] = extendRepeating(exprElems[5], "SAT");
			// 3. Replace "working days" to "MON-FRI"
			if (exprElems[3].indexOf('W') >= 0) {
				exprElems[3] = exprElems[3].replaceAll("W", "");
				exprElems[5] = "MON-FRI";
			}
			// 4. Replace "last day of week" to dates interval 24-L
			if (exprElems[5].indexOf('L') >= 0) {
				exprElems[5] = exprElems[5].replaceAll("L", "");
				exprElems[3] = "24-L";
			}
			// 5. Ignore #n in days of week
			int indSharp = exprElems[5].indexOf('#');
			if (indSharp >= 0) exprElems[5] = exprElems[5].substring(0, indSharp);
			// 6. Change all question marks to asterisks
			if ("?".equals(exprElems[3])) exprElems[3] = "*";
			if ("?".equals(exprElems[5])) exprElems[5] = "*";
			// 7. Ignore seconds and years
			return concat(' ', exprElems[1], exprElems[2], exprElems[3], exprElems[4], exprElems[5]);
		} else {
			return quartzExpr;
		}
	}
	
	/**
	 * Converting valid SauronSoftware cron expression to valid Quartz one.
	 * The conversions are the following:
	 * <ul><li>add &quot;seconds&quot; part;</li>
	 * <li>numbers in &quot;day of week&quot; started from 1, not from 0 as in Sauron;</li>
	 * <li>&quot;*&#47;interval&quot; items converted to
	 * &quot;/interval&quot; items;</li>
	 * <li>one of date and day of week should be a question mark.</li>
	 * </ul>
	 * @param sauronExpr	Valid SauronSoftware cron expression
	 * @return				Similar Quartz cron expression
	 */
	public static String unpackSchedule(String sauronExpr) {
		if (sauronExpr == null) return null;
		String[] exprElems = sauronExpr.trim().split("\\s+");
		if (exprElems.length == 5) {
			// 1. Increase number od days in "days of week"
			exprElems[4] = increaseDoW(exprElems[4]);
			// 2. Cut right end of an interval in repeating items
			exprElems[0] = shrinkRepeating(exprElems[0], "0"); 
			exprElems[1] = shrinkRepeating(exprElems[1], "0"); 
			exprElems[2] = shrinkRepeating(exprElems[2], "1"); 
			exprElems[3] = shrinkRepeating(exprElems[3], "1"); 
			exprElems[4] = shrinkRepeating(exprElems[4], "SUN");
			// 3. "Last" processing and question marks inserting
			if (!"*".equals(exprElems[4])) {
				if (exprElems[2].indexOf('L') >= 0 &&
					exprElems[4].indexOf('-') == -1 &&
					exprElems[4].indexOf('/') == -1) {
					exprElems[4] = exprElems[4] + "L";
				}
				exprElems[2] = "?";
			} else {
				exprElems[4] = "?";
			}
			// 4. Add seconds part
			return concat(' ', "0", exprElems[0], exprElems[1], exprElems[2], exprElems[3], exprElems[4]);
		} else {
			return sauronExpr;
		}
	}
	
	/**
	 * Strings concatenation with a delimeter string. Similar to
	 * {@link com.dell.doradus.common.Utils#concatenate(String[], String)}
	 * but uses variable number of arguments instead of array of strings.
	 * 
	 * @param delim	Strings separator
	 * @param parts	Strings to concatenate
	 * @return		Concatenated string
	 */
	private static String concat(char delim, String... parts) {
		StringBuilder builder = new StringBuilder(parts[0]);
		for (int i = 1; i < parts.length; ++i) {
			builder.append(delim).append(parts[i]);
		}
		return builder.toString();
	}
	
	/**
	 * Replaces in first argument string all the characters of the second argument string
	 * to the characters of the third argument string. Actually the source string
	 * is divided into "parts", and replacing takes place in every part before a slash
	 * symbol (if exists), but not after it.
	 * 
	 * @param e			Source string
	 * @param chars1	Characters to replace
	 * @param chars2	Characters to insert
	 * @return 			String with replacements made.
	 */
	private static String replaceChars(String e, String chars1, String chars2) {
		assert chars1.length() == chars2.length();
		
		String[] parts = e.split(",");
		for (int i = 0; i < parts.length; ++i) {
			String[] elems = parts[i].split("/");
			for (int j = 0; j < chars1.length(); ++j) {
				elems[0] = elems[0].replace(chars1.charAt(j), chars2.charAt(j));
			}
			parts[i] = concat('/', elems);
		}
		return concat(',', parts);
	}
	
	/**
	 * Makes replacements in day-of-week part of the cron expression: day numbers
	 * are to be increased by 1.
	 * 
	 * @param e	Source expression
	 * @return  Modified expression
	 */
	private static String increaseDoW(String e) {
		return replaceChars(e, "6543210", "7654321");
	}
	
	/**
	 * Makes replacements in day-of-week part of the cron expression: day numbers
	 * are to be decreased by 1.
	 * 
	 * @param e	Source expression
	 * @return  Modified expression
	 */
	private static String decreaseDoW(String e) {
		return replaceChars(e, "1234567", "0123456");
	}
	
	/**
	 * Converts repeating segments from "short" Quartz form to "extended"
	 * SauronSoftware form. For example &quot;0/5&quot; will be converted to 
	 * &quot;0-59/5&quot;, &quot;/3&quot; will be converted to &quot;*&#47;3&quot;
	 * 
	 * @param e		Source item
	 * @param max	Maximal value in this item ("59" for minutes, "23" for hours, etc.)
	 * @return 		Modified string
	 */
	private static String extendRepeating(String e, String max) {
		String[] parts = e.split(",");
		for (int i = 0; i < parts.length; ++i) {
			if (parts[i].indexOf('-') == -1 && parts[i].indexOf('*') == -1) {
				int indSlash = parts[i].indexOf('/');
				if (indSlash == 0) {
					parts[i] = "*" + parts[i];
				} else if (indSlash > 0) {
					parts[i] = parts[i].substring(0, indSlash) + "-" + max + parts[i].substring(indSlash);
				}
			}
		}
		return concat(',', parts);
	}
	
	/**
	 * Converts repeating segments from "extended" SauronSoftware to "short"
	 * Quartz form form. For example &quot;0-59/5&quot; will be converted to 
	 * &quot;0/5&quot;, &quot;/3&quot; will be converted to &quot;*&#47;3&quot;
	 * 
	 * @param e		Source item
	 * @param max	Maximal value in this item ("59" for minutes, "23" for hours, etc.)
	 * @return 		Modified string
	 */
	private static String shrinkRepeating(String e, String min) {
		String[] parts = e.split(",");
		for (int i = 0; i < parts.length; ++i) {
			int indSlash = parts[i].indexOf('/');
			int indDash = parts[i].indexOf('-');
			if (indSlash >= 0 && indDash == -1) {
				if (parts[i].indexOf('*') >= 0) {
					parts[i] = parts[i].substring(indSlash);
				} else {
					parts[i] = parts[i].substring(0, indSlash);
				}
			}
		}
		return concat(',', parts);
	}
	
}
