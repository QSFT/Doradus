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

package com.dell.doradus.core;

import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.Random;


/**
 * Generates 120-bit (15 byte) IDs that are k-sorted (time-based), unique upon successive
 * calls, and unique among multiple nodes in a cluster. That is, every node can use this
 * class to generate IDs without requiring coordination from other nodes. Each ID uses the
 * following format:
 * <pre>
 *       --------------------------------------
 *       | Timestamp | MAC Address | Sequence | 
 *       --------------------------------------
 *          56 bits      48 bits      16 bits
 * </pre>
 * Where:
 * <ul>
 * <li>Timestamp is the current System.currentTimeMillis() value as a long. Only the 7
 *     right-most bytes are used because the left-most byte will be zero for a few million
 *     years!</li>
 * <li>MAC Address is 6 bytes, chosen from (one of) the machine's network interface cards.
 *     In the rare event that no suitable NIC is found, a random number is used.</li>
 * <li>The Sequence number is 2 bytes that is incremented if successive calls are made
 *     within the same millisecond. A RuntimeException is thrown if we somehow generate
 *     more than 65536 values in a single millisecond!</li>
 * </ul>
 * This class is similar to the Boundary Flake algorithm described in this article:
 * http://boundary.com/blog/2012/01/12/flake-a-decentralized-k-ordered-unique-id-generator-in-erlang/.
 * The Boundary Flake algorithm was inspired by Twitter's SnowFlake algorithm, which
 * generates 64-bit IDs but requires node coordination such as ZooKeeper.
 * <p>
 * Why 15 bytes? Because byte[15] IDs converted into base64 values become 20 characters.
 * If we include the left-most timestamp value (which is always 0) to generate byte[16]
 * IDs, the base64 value becomes 24 characters with the left-most characters always
 * "AA" and the right-most characters "==". Hence, we save 4 chars per value with no loss
 * of precision! 
 */
public class IDGenerator {
    // static methods only
    private IDGenerator() {};
    
    // MAC address used for all values on this machine:
    private static final byte[] MAC = chooseMACAddress();
    
    // Dynamic members; must be accessed only via LOCK 
    private static final Object LOCK = new Object();
    private static long         LAST_TIMESTAMP;
    private static final byte[] TIMESTAMP_BUFFER = new byte[7];
    private static short        LAST_SEQUENCE;
    
    /**
     * Return the next unique ID. See the class description for the format of an ID.
     * 
     * @return  The next unique ID as a byte[15]. A new byte[] is allocated with each
     *          call so the caller can modify it.
     */
    public static byte[] nextID() {
        byte[] ID = new byte[15];
        synchronized (LOCK) {
            long timestamp = System.currentTimeMillis();
            if (timestamp != LAST_TIMESTAMP) {
                LAST_TIMESTAMP = timestamp;
                LAST_SEQUENCE = 0;
                TIMESTAMP_BUFFER[0] = (byte)(timestamp >>> 48);
                TIMESTAMP_BUFFER[1] = (byte)(timestamp >>> 40);
                TIMESTAMP_BUFFER[2] = (byte)(timestamp >>> 32);
                TIMESTAMP_BUFFER[3] = (byte)(timestamp >>> 24);
                TIMESTAMP_BUFFER[4] = (byte)(timestamp >>> 16);
                TIMESTAMP_BUFFER[5] = (byte)(timestamp >>>  8);
                TIMESTAMP_BUFFER[6] = (byte)(timestamp >>>  0);
            } else if (++LAST_SEQUENCE == 0) {
                throw new RuntimeException("Same ID generated in a single milliscond!");
            }
            ByteBuffer bb = ByteBuffer.wrap(ID);
            bb.put(TIMESTAMP_BUFFER);
            bb.put(MAC);
            bb.putShort(LAST_SEQUENCE);
        }
        return ID;
    }   // nextID

    // Choose a MAC address from one of this machine's NICs or a random value. 
    private static byte[] chooseMACAddress() {
        byte[] result = new byte[6];
        boolean bFound = false;
        try {
            Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces();
            while (!bFound && ifaces.hasMoreElements()) {
                // Look for a real NIC.
                NetworkInterface iface = ifaces.nextElement();
                try {
                    byte[] hwAddress = iface.getHardwareAddress();
                    if (hwAddress != null) {
                        int copyBytes = Math.min(result.length, hwAddress.length);
                        for (int index = 0; index < copyBytes; index++) {
                            result[index] = hwAddress[index];
                        }
                        bFound = true;
                    }
                } catch (SocketException e) {
                    // Try next NIC
                }
            }
        } catch (SocketException e) {
            // Possibly no NICs on this system?
        }
        if (!bFound) {
            (new Random()).nextBytes(result);
        }
        return result;
    }   // chooseMACAddress

}   // class IDGenerator
