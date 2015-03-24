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

import java.beans.ConstructorProperties;
import java.util.concurrent.TimeUnit;

public class EventRate {

	private TimeUnit rateUnit;
	private String eventType;
	private long count;
	private double fifteenMinuteRate;
	private double fiveMinuteRate;
	private double oneMinuteRate;
	private double meanRate;
	
	
	public EventRate() {}
	
	public EventRate(String eventType, TimeUnit rateUnit) {
		this.eventType = eventType;
		this.rateUnit = rateUnit;
	}
	
	@ConstructorProperties({ "eventType", "rateUnit", "count", "meanRate", "fifteenMinuteRate",
		"fiveMinuteRate", "oneMinuteRate" })
	public EventRate(String eventType, TimeUnit rateUnit, long count, 
			double meanRate, double fifteenMinuteRate, double fiveMinuteRate, double oneMinuteRate) {
		this.eventType = eventType;
		this.rateUnit = rateUnit;
		update(count, meanRate, fifteenMinuteRate, fiveMinuteRate, oneMinuteRate);
	}
	
    /**
     * Returns the meter's rate unit.
     *
     * @return the meter's rate unit
     */
    public TimeUnit getRateUnit() {
    	return rateUnit;
    }

    /**
     * Returns the type of events the meter is measuring.
     *
     * @return the meter's event type
     */
    public String getEventType() {
    	return eventType;
    }

    /**
     * Returns the number of events which have been marked.
     *
     * @return the number of events which have been marked
     */
    public long getCount() {
    	return count;
    }

    /**
     * Returns the fifteen-minute exponentially-weighted moving average rate at
     * which events have occurred.
     * <p>
     * This rate has the same exponential decay factor as the fifteen-minute load
     * average in the {@code top} Unix command.
     *
     * @return the fifteen-minute exponentially-weighted moving average rate at
     *         which events have occurred
     */
    public double getFifteenMinuteRate() {
    	return fifteenMinuteRate;
    }

    /**
     * Returns the five-minute exponentially-weighted moving average rate at
     * which events have occurred.
     * <p>
     * This rate has the same exponential decay factor as the five-minute load
     * average in the {@code top} Unix command.
     *
     * @return the five-minute exponentially-weighted moving average rate at
     *         which events have occurred
     */
    public double getFiveMinuteRate() {
    	return fiveMinuteRate;
    }

    /**
     * Returns the mean rate at which events have occurred.
     *
     * @return the mean rate at which events have occurred.
     */
    public double getMeanRate() {
    	return meanRate;
    }

    /**
     * Returns the one-minute exponentially-weighted moving average rate at
     * which events have occurred.
     * <p>
     * This rate has the same exponential decay factor as the one-minute load
     * average in the {@code top} Unix command.
     *
     * @return the one-minute exponentially-weighted moving average rate at
     *         which events have occurred
     */
    public double getOneMinuteRate() {
    	return oneMinuteRate;
    }

    /**
     * 
     * @param count
     * @param mean
     * @param ma15Rate
     * @param ma5Rate
     * @param ma1Rate
     */
    public void update(long count, double mean, double ma15Rate, double ma5Rate, double ma1Rate) {
    	this.count = count;
    	this.meanRate = mean;
    	this.fifteenMinuteRate = ma15Rate;
    	this.fiveMinuteRate = ma5Rate;
    	this.oneMinuteRate = ma1Rate;
    }
}
