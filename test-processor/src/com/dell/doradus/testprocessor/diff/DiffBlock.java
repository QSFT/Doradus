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

package com.dell.doradus.testprocessor.diff;

public class DiffBlock
{
	public int deleteStartA;	// position where deletions in A begin
	public int deleteCountA;	// number of deletions in A
	public int insertStartB;	// position where insertions in B begin
	public int insertCountB;	// number of insertions in B
	
	public DiffBlock(int deleteStartA, int deleteCountA, int insertStartB, int insertCountB) {
		this.deleteStartA = deleteStartA;
		this.deleteCountA = deleteCountA;
		this.insertStartB = insertStartB;
		this.insertCountB = insertCountB;
	}
}
