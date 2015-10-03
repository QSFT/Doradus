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

package com.dell.doradus.search.aggregate;

import com.dell.doradus.core.ServerParams;
import com.dell.doradus.search.util.LRUCache;


/**
 * When the EntitySequence is asked for the next Entity, DBEntitySequenceFactory pre-fetches the entities
 * that will be iterated in future.
 *
 * DBEntitySequenceOptions define the pre-fetch buffers size.
 *
 */
public class DBEntitySequenceOptions extends EntitySequenceOptions{
	public final int entityBuffer;
	public final int linkBuffer;
	public final int initialLinkBuffer;
	public final int initialLinkBufferDimension;
    public final int initialScalarBuffer;

	public static final DBEntitySequenceOptions defaultOptions = new DBEntitySequenceOptions(
	        ServerParams.instance().getModuleParamInt("DoradusServer", "dbesoptions_entityBuffer", 1000),
	        ServerParams.instance().getModuleParamInt("DoradusServer", "dbesoptions_linkBuffer", 1000),
	        ServerParams.instance().getModuleParamInt("DoradusServer", "dbesoptions_initialLinkBuffer", 10),
	        ServerParams.instance().getModuleParamInt("DoradusServer", "dbesoptions_initialLinkBufferDimension", 100),
	        ServerParams.instance().getModuleParamInt("DoradusServer", "dbesoptions_initialScalarBuffer", 30));

	/**
	 * @param scalarBuffer      Number of entities being pre-fetched with scalar values.
	 * @param linkBuffer        Number of pre-fetched references of the same link field for one entity if the current buffer is empty.
	 * @param initialLinkBuffer Number of pre-fetched references of the same link field for every cached entity when the link values asked for one entity.
	 * @param initalLinkBufferDimension Number of entities the initial links will be pre-fetched for.
	 */
	public DBEntitySequenceOptions(int entityBuffer, int linkBuffer, int initialLinkBuffer, int initalLinkBufferDimension, int initialScalarBuffer) {
		this.entityBuffer = entityBuffer;
		this.linkBuffer = linkBuffer;
		this.initialLinkBuffer = initialLinkBuffer;
		this.initialLinkBufferDimension = initalLinkBufferDimension;
        this.initialScalarBuffer = initialScalarBuffer;
	}

	<K, T> DBEntitySequenceOptions adjustEntityBuffer(LRUCache<K, T> cache) {
		return new DBEntitySequenceOptions(Math.min(cache.getCapacity(), entityBuffer), linkBuffer, initialLinkBuffer,
				initialLinkBufferDimension, initialScalarBuffer);
	}

	<K, T> DBEntitySequenceOptions adjustInitialLinkBufferDimension(LRUCache<K, T> cache) {
		return new DBEntitySequenceOptions(entityBuffer, linkBuffer, initialLinkBuffer,
				Math.min(cache.getCapacity(), initialLinkBufferDimension), initialScalarBuffer);
	}

	DBEntitySequenceOptions setInitialLinkBufferDimension(int value){
		return new DBEntitySequenceOptions(entityBuffer, linkBuffer, initialLinkBuffer, value, initialScalarBuffer);
	}
}
