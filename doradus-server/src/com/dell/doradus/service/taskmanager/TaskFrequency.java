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

package com.dell.doradus.service.taskmanager;

import com.dell.doradus.common.Utils;

/**
 * Represents a task frequency such as "1 day" or "12 hours". A frequency consists of
 * a value and a frequency mnemonic, which must be "minute(s)", "hour(s)", or "day(s)". 
 */
public class TaskFrequency {
    public enum Frequency {
        MINUTES,
        HOURS,
        DAYS;
        
        public static Frequency find(String mnemonic) {
            String value = mnemonic.toUpperCase();
            if (!value.endsWith("S")) {
                value = value + "S";
            }
            return Frequency.valueOf(value);
        }
    }

    // The units and value of this TaskFrequency object:
    private final Frequency m_units;
    private final int       m_value;

    /**
     * Create a TaskFrequency by parsing the given value. It be in the form:
     * <pre>
     *      {number} [{units}]
     * </pre>
     * where {number} is an integer and {units} is a valid {@link Frequency} mnemonic
     * (case-insensitive). The trailing 's' on the units is optional.
     *
     * @param  freqString               Value in the format shown above.
     * @throws IllegalArgumentException If the given format is invalid.
     */
    public TaskFrequency(String freqString) throws IllegalArgumentException {
        String[] parts = freqString.trim().split(" +");
        Utils.require(parts.length == 2, "Invalid frequency format: " + freqString);

        try {
            m_value = Integer.parseInt(parts[0]);
            m_units = Frequency.find(parts[1]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid frequency format: " + freqString);
        }
        Utils.require(m_value != 0, "Frequency value cannot be 0");
    }   // constructor

    public Frequency getUnits() {
        return m_units;
    }

    public int getValue() {
        return m_value;
    }

    public int getValueInMinutes() {
        switch (m_units) {
        case MINUTES:
            return m_value;
        case HOURS:
            return m_value * 60;
        case DAYS:
            return m_value * 60 * 24;
        default:
            throw new RuntimeException("Unhandled case: " + m_units);
        }
    }   // getValueInMinutes
    
    // Return "18 DAYS", for example
    @Override
    public String toString() {
        return Integer.toString(m_value) + " " + m_units.toString();
    }   // toString

}   // class TaskFrequency
