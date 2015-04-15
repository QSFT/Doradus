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

package com.dell.doradus.olap.store;

import com.dell.doradus.olap.io.VInputStream;
import com.dell.doradus.olap.io.VOutputStream;

//numbers packed into 1bit, 1/2/4/8 bytes
public class NumArray {
 public static interface NumA {
     public int cacheSize();
     public int size();
     public long get(int index);
 }

 public static class ZeroA implements NumA {
     private int m_size;
     public ZeroA(int size, VInputStream input) { m_size = size; }
     @Override public int cacheSize() { return 4; }
     @Override public int size() { return m_size; }
     @Override public long get(int index) { return 0; }
     
     public static void writeData(VOutputStream output, long[] values, int size) {}
 }
 
 public static class BitA implements NumA {
     private BitVector m_bitArray;
     public BitA(int size, VInputStream input) {
         m_bitArray = new BitVector(size);
         input.read(m_bitArray.getBuffer(), 0, m_bitArray.getBuffer().length);  
     }
     @Override public int cacheSize() { return 4 + m_bitArray.getBuffer().length; }
     @Override public int size() { return m_bitArray.size(); }
     @Override public long get(int index) { return m_bitArray.get(index) ? 1 : 0; }
     
     public static void writeData(VOutputStream output, long[] values, int size) {
         BitVector bitArray = new BitVector(size);
         for(int i = 0; i < size; i++) {
             if(values[i] != 0) bitArray.set(i); 
         }
         byte[] buf = bitArray.getBuffer();
         output.write(buf, 0, buf.length);
     }
 }

 public static class ByteA implements NumA {
     private byte[] m_array;
     public ByteA(int size, VInputStream input) {
         m_array = new byte[size];
         input.read(m_array, 0, m_array.length);
     }
     @Override public int cacheSize() { return 4 + m_array.length; }
     @Override public int size() { return m_array.length; }
     @Override public long get(int index) { return m_array[index]; }
     
     public static void writeData(VOutputStream output, long[] values, int size) {
         for(int i = 0; i < size; i++) output.writeByte((byte)values[i]);
     }
 }

 public static class ShortA implements NumA {
     private short[] m_array;
     public ShortA(int size, VInputStream input) {
         m_array = new short[size];
         for(int i = 0; i < m_array.length; i++) m_array[i] = input.readShort();
     }
     @Override public int cacheSize() { return 4 + m_array.length * 2; }
     @Override public int size() { return m_array.length; }
     @Override public long get(int index) { return m_array[index]; }

     public static void writeData(VOutputStream output, long[] values, int size) {
         for(int i = 0; i < size; i++) output.writeShort((short)values[i]);
     }
 }
 
 public static class IntA implements NumA {
     private int[] m_array;
     public IntA(int size, VInputStream input) {
         m_array = new int[size];
         for(int i = 0; i < m_array.length; i++) m_array[i] = input.readInt();
     }
     @Override public int cacheSize() { return 4 + m_array.length * 4; }
     @Override public int size() { return m_array.length; }
     @Override public long get(int index) { return m_array[index]; }

     public static void writeData(VOutputStream output, long[] values, int size) {
         for(int i = 0; i < size; i++) output.writeInt((int)values[i]);
     }
 }

 public static class LongA implements NumA {
     private long[] m_array;
     public LongA(int size, VInputStream input) {
         m_array = new long[size];
         for(int i = 0; i < m_array.length; i++) m_array[i] = input.readLong();
     }
     @Override public int cacheSize() { return 4 + m_array.length * 8; }
     @Override public int size() { return m_array.length; }
     @Override public long get(int index) { return m_array[index]; }

     public static void writeData(VOutputStream output, long[] values, int size) {
         for(int i = 0; i < size; i++) output.writeLong(values[i]);
     }
 }
 
 public static class MinA implements NumA {
     private long m_min;
     private NumA m_array;
     public MinA(int size, VInputStream input) {
         m_min = input.readVLong();
         int bits = input.readByte();
         if(bits == 0) m_array = new ZeroA(size, input);
         else if(bits == 1) m_array = new BitA(size, input);
         else if(bits == 8) m_array = new ByteA(size, input);
         else if(bits == 16) m_array = new ShortA(size, input);
         else if(bits == 32) m_array = new IntA(size, input);
         else throw new RuntimeException("Invalid bits in MinA: " + bits);
     }
     @Override public int cacheSize() { return 4 + m_array.cacheSize(); }
     @Override public int size() { return m_array.size(); }
     @Override public long get(int index) { return m_array.get(index) + m_min; }

     public static void writeData(VOutputStream output, long[] values, int size, long min, byte bits) {
         output.writeVLong(min);
         output.writeByte(bits);
         if(bits == 0) ZeroA.writeData(output, values, size);
         else if(bits == 1) BitA.writeData(output, values, size);
         else if(bits == 8) ByteA.writeData(output, values, size);
         else if(bits == 16) ShortA.writeData(output, values, size);
         else if(bits == 32) IntA.writeData(output, values, size);
         else throw new RuntimeException("Invalid bits in MinA: " + bits);
     }
 }
 
 private int m_bits;
 private NumA m_array;

 public NumArray(VInputStream input) {
     int size = input.readVInt();
     m_bits = input.readByte();
     if(m_bits == 0) {
         m_array = new ZeroA(size, input);
     }
     else if(m_bits == 1) {
         m_array = new BitA(size, input);
     }
     else if(m_bits == 8) {
         m_array = new ByteA(size, input);
     }
     else if(m_bits == 16) {
         m_array = new ShortA(size, input);
     }
     else if(m_bits == 32) {
         m_array = new IntA(size, input);
     }
     else if(m_bits == 64) {
         m_array = new LongA(size, input);
     }
     /*
     else if(m_bits == 65) { // min optimization
         m_array = new MinA(size, input);
     }*/
     else {
         throw new RuntimeException("Unknown bits: " + m_bits);
     }
 }
 
 public int size() { return m_array.size(); }
 
 public long get(int index) {
     return m_array.get(index);
 }
 
 public static byte writeArray(VOutputStream output, long[] values, int size, long min, long max) {
     output.writeVInt(size);
     
     byte bits = -1;
     
     if(size == 0 || (max == 0 && min == 0)) { // no data or all zeroes
         bits = 0;
         output.writeByte(bits);
         ZeroA.writeData(output, values, size);
     }
     else if(max <= 1 && min >= 0) { // 1 bit
         bits = 1;
         output.writeByte(bits);
         BitA.writeData(output, values, size);
     }
     else if(max <= Byte.MAX_VALUE && min >= Byte.MIN_VALUE) { // 1 byte
         bits = 8;
         output.writeByte(bits);
         ByteA.writeData(output, values, size);
     }
     else if(max <= Short.MAX_VALUE && min >= Short.MIN_VALUE) { // 2 bytes
         bits = 16;
         output.writeByte(bits);
         ShortA.writeData(output, values, size);
     }
     else if(max <= Integer.MAX_VALUE && min >= Integer.MIN_VALUE) { // 4 bytes
         bits = 32;
         output.writeByte(bits);
         IntA.writeData(output, values, size);
     }
     //temporarily disable min optimization to do more performance tests
     /*
     else if(min > 0 && max - min < Integer.MAX_VALUE) { // min optimization
         bits = 65;
         output.writeByte(bits);
         // if a value is 0 and min > 0 then the value is NULL
         for(int i = 0; i < size; i++) if(values[i] != 0) values[i] -= min;
         if(max - min == 0) bits = 0;
         else if(max - min <= 1) bits = 1;
         else if(max - min <= Byte.MAX_VALUE) bits = 8;
         else if(max - min <= Short.MAX_VALUE) bits = 16;
         else bits = 32;
         MinA.writeData(output, values, size, min, bits);
     }*/
     else {  //8 bytes
         bits = 64;
         output.writeByte(bits);
         LongA.writeData(output, values, size);
     }
     
     return bits;
 }
 
 public long cacheSize() { return 8 + m_array.cacheSize(); }
 
}
