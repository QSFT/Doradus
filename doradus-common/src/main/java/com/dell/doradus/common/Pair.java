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

public class Pair<T1, T2>
{
    public final T1 firstItemInPair;
    public final T2 secondItemInPair;

    protected Pair(T1 firstItemInPair, T2 secondItemInPair)
    {
        this.firstItemInPair = firstItemInPair;
        this.secondItemInPair = secondItemInPair;
    }
    

    @Override
    public final int hashCode()
    {
        int hashCode = 31 + (firstItemInPair == null ? 0 : firstItemInPair.hashCode());
        return 31*hashCode + (secondItemInPair == null ? 0 : secondItemInPair.hashCode());
    }

    @SuppressWarnings("rawtypes")
    @Override
    public final boolean equals(Object other)
    {
    	if(other == null) 
    		return false;
    	if(other == this) 
    		return true;
        if(!(other instanceof Pair)) 
        	return false;
        Pair pair = (Pair)other;
        return (pair.firstItemInPair == this.firstItemInPair && pair.secondItemInPair == this.secondItemInPair); 
    }

    @Override
    public String toString()
    {
        return "(" + firstItemInPair + "," + secondItemInPair + ")";
    }

    public static <X, Y> Pair<X, Y> create(X x, Y y)
    {
        return new Pair<X, Y>(x, y);
    }
}