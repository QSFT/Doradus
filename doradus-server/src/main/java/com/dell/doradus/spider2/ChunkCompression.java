package com.dell.doradus.spider2;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

public class ChunkCompression {
    
    public static byte[] compress_fast(byte[] data) {
        LZ4Factory factory = LZ4Factory.fastestInstance();
        final int decompressedLength = data.length;
        //LZ4Compressor compressor = factory.highCompressor();
        LZ4Compressor compressor = factory.fastCompressor();
        int maxCompressedLength = compressor.maxCompressedLength(decompressedLength);
        byte[] compressed = new byte[maxCompressedLength];
        int compressedLength = compressor.compress(data, 0, decompressedLength, compressed, 0, maxCompressedLength);
        byte[] array = new byte[compressedLength + 4];
        //first 4 bytes is decompressed length
        array[0] = (byte)decompressedLength;
        array[1] = (byte)(decompressedLength >> 8);
        array[2] = (byte)(decompressedLength >> 16);
        array[3] = (byte)(decompressedLength >> 24);
        System.arraycopy(compressed, 0, array, 4, compressedLength);
        return array;
    }
    
    public static byte[] compress(byte[] data) {
        LZ4Factory factory = LZ4Factory.fastestInstance();
        final int decompressedLength = data.length;
        LZ4Compressor compressor = factory.highCompressor();
        int maxCompressedLength = compressor.maxCompressedLength(decompressedLength);
        byte[] compressed = new byte[maxCompressedLength];
        int compressedLength = compressor.compress(data, 0, decompressedLength, compressed, 0, maxCompressedLength);
        byte[] array = new byte[compressedLength + 4];
        //first 4 bytes is decompressed length
        array[0] = (byte)decompressedLength;
        array[1] = (byte)(decompressedLength >> 8);
        array[2] = (byte)(decompressedLength >> 16);
        array[3] = (byte)(decompressedLength >> 24);
        System.arraycopy(compressed, 0, array, 4, compressedLength);
        return array;
    }
    
    public static byte[] decompress(byte[] data) {
        LZ4Factory factory = LZ4Factory.fastestInstance();
        // decompress data
        // - method 1: when the decompressed length is known
        int b1 = 0x00FF & data[0], b2 = 0x00FF & data[1], b3 = 0x00FF & data[2], b4 = 0x00FF & data[3];
        int decompressedLength = b1 | (b2 << 8) | (b3 << 16) | (b4 << 24);
        LZ4FastDecompressor decompressor = factory.fastDecompressor();
        byte[] restored = new byte[decompressedLength];
        int compressedLength2 = decompressor.decompress(data, 4, restored, 0, decompressedLength);
        if(compressedLength2 != data.length - 4) throw new RuntimeException("Error decompressing a chunk");
        return restored;
        // compressedLength == compressedLength2

        //  // - method 2: when the compressed length is known (a little slower)
        //  // the destination buffer needs to be over-sized
        //  LZ4SafeDecompressor decompressor2 = factory.safeDecompressor();
        //  int decompressedLength2 = decompressor2.decompress(compressed, 0, compressedLength, restored, 0);
        //  // decompressedLength == decompressedLength2        
        
        
    }
}
