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

package com.dell.doradus.common;

import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * Represents a retention age value used for data aging. A retention age has a units
 * member, which is defined by the enum RetentionUnits, and an integer value, which is
 * the number of units with which data should be retained. Objects are immutable once
 * constructed.
 */
public class RetentionAge {
    /**
     * Possible values that can be used for retention units.
     */
    public enum RetentionUnits {
        // Units currently supported:
        DAYS(1),
        MONTHS(2),
        YEARS(3);

        final int value;
        RetentionUnits(int value) {
            this.value = value;
        }
    }   // enum RetentionUnits

    // The units and value of this RetentionAge object:
    private final RetentionUnits m_units;
    private final int            m_value;

    /**
     * Create a RetentionAge by parsing the given retention-age option in string form. It
     * must follow the format:
     * <code>
     *      {number} [{units}]
     * </code>
     * where {number} is an integer and {units} is a valid {@link RetentionUnits} mnemonic
     * (case-insensitive). If not present, the default unit is DAYS. Example:
     * <code>
     *      "1000 days"
     * </code>
     * If the given string is invalid, an exception is thrown.
     *
     * @param  retentionAge             Retention-age in string form.
     * @throws IllegalArgumentException If the given format is invalid.
     */
    public RetentionAge(String retentionAge) throws IllegalArgumentException {
        // Split into space-separated parts. Must be 1 or 2 parts.
        String[] parts = retentionAge.trim().split(" +");
        Utils.require(parts.length >= 1 && parts.length <= 2,
                      "Invalid retention-age value: " + retentionAge);

        // First part must be an integer.
        int value = 0;
        try {
            value = Integer.parseInt(parts[0]);
            // Here, valid integer.
        } catch (NumberFormatException e) {
            // Not a number
            Utils.require(false, "Invalid number format for retention-age: " + parts[0]);
        }
        m_value = value;

        // If units is present, verify.
        if (parts.length == 1) {
            m_units = RetentionUnits.DAYS;
        } else {
            // Throws IllegalArgumentException if invalid.
            m_units = RetentionUnits.valueOf(parts[1].toUpperCase());
        }
    }   // constructor

    /**
     * Find the date on or before which objects expire based on the given date and the
     * retention age specified in this object. The given date is cloned and then adjusted
     * downward by the units and value in this RetentionAge.
     *
     * @param   relativeDate    Reference date.
     * @return  The date relative to the given one at which objects should be considered
     *          expired based on this RetentionAge.
     */
    public GregorianCalendar getExpiredDate(GregorianCalendar relativeDate) {
        // Get today's date and adjust by the specified age.
        GregorianCalendar expiredDate = (GregorianCalendar)relativeDate.clone();
        switch (m_units) {
        case DAYS:
            expiredDate.add(Calendar.DAY_OF_MONTH, -m_value);
            break;
        case MONTHS:
            expiredDate.add(Calendar.MONTH, -m_value);
            break;
        case YEARS:
            expiredDate.add(Calendar.YEAR, -m_value);
            break;
        default:
            // New value we forgot to add here?
            throw new AssertionError("Unknown RetentionUnits: " + m_units);
        }
        return expiredDate;
    }   // getExpiredDate

    /**
     * Get this object's retention age units.
     *
     * @return  This object's retention age units.
     */
    public RetentionUnits getUnits() {
        return m_units;
    }   // getUnits

    /**
     * Get this object's retention age value.
     *
     * @return This object's retention age value.
     */
    public int getValue() {
        return m_value;
    }   // getValue

    // Return "18 MONTHS", for example
    @Override
    public String toString() {
        return Integer.toString(m_value) + " " + m_units.toString();
    }   // toString

}   // class RetentionAge
