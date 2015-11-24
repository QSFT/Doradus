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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.xml.bind.DatatypeConverter;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Comment;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.InputSource;

/**
 * Static helper functions used by Doradus. No instances are allowed.
 */
final public class Utils {
    /**
     * The Charset object for the UTF-8 character set.
     */
    public static final Charset UTF8_CHARSET = Charset.forName("UTF-8");
    
    /**
     * The UTC timezone (aka GMT or Zulu time).
     */
    public static final TimeZone UTC_TIMEZONE = TimeZone.getTimeZone("GMT");
    
    private static final char[] UID_CHARS = "0123456789abcdefghijklmnopqrstuv".toCharArray();
    
    // Static methods only
    private Utils() {
        throw new AssertionError();
    }

    /**
     * Return true if the given string contains only letters, digits, and underscores.
     *
     * @param   string  String to be tested.
     * @return          True if the string is not null, not empty, and contains only
     *                  letters, digits, and underscores.
     */
    public static boolean allAlphaNumUnderscore(String string) {
        if (string == null || string.length() == 0) {
            return false;
        }
        for (int index = 0; index < string.length(); index++) {
            char ch = string.charAt(index);
            if (!isLetter(ch) && !isDigit(ch) && ch != '_') {
                return false;
            }
        }
        return true;
    }   // allAlphaNumUnderscore

    /**
     * Return true if the given string contains only digits (characters '0' - '9').
     *
     * @param   string  String to be tested.
     * @return          True if the string is not null, not empty, and contains only
     *                  digit characters '0' through '9'. 
     */
    public static boolean allDigits(String string) {
        if (string == null || string.length() == 0) {
            return false;
        }
        for (int index = 0; index < string.length(); index++) {
            char ch = string.charAt(index);
            if (ch < '0' || ch > '9') {
                return false;
            }
        }
        return true;
    }   // allDigits
    
    /**
     * Convert (decode) the given Base64-encoded String to its binary form.
     * 
     * @param base64Value               Base64-encoded string.
     * @return                          Decoded binary value.
     * @throws IllegalArgumentException If the given string is not a valid Base64 value.
     */
    public static byte[] base64ToBinary(String base64Value) throws IllegalArgumentException {
        Utils.require(base64Value.length() % 4 == 0,
                      "Invalid base64 value (must be a multiple of 4 chars): " + base64Value);
        return DatatypeConverter.parseBase64Binary(base64Value);
    }   // base64ToBinary
    
    /**
     * Decode the given Base64-encoded String to binary and then return as a string of hex
     * digits.
     * 
     * @param base64Value               Base64-encoded string.
     * @return                          Decoded binary value re-encoded as a hex string.
     * @throws IllegalArgumentException If the given string is not a valid Base64 value.
     */
    public static String base64ToHex(String base64Value) throws IllegalArgumentException {
        byte[] binary = base64ToBinary(base64Value);
        return DatatypeConverter.printHexBinary(binary);
    }   // base64ToHex
    
    /**
     * Convert (encode) the given binary value using Base64.
     * 
     * @param  value                    A binary value.
     * @return                          Base64-encoded value.
     * @throws IllegalArgumentException If the given value is null.
     */
    public static String base64FromBinary(byte[] value) throws IllegalArgumentException {
        return DatatypeConverter.printBase64Binary(value);
    }   // base64FromBinary
    
    /**
     * Decode the given hex string to binary and then re-encoded it as a Base64 string.
     * 
     * @param hexValue                  String of hexadecimal characters.
     * @return                          Decoded binary value re-encoded with Base64.
     * @throws IllegalArgumentException If the given value is null or invalid.
     */
    public static String base64FromHex(String hexValue) throws IllegalArgumentException {
        byte[] binary = DatatypeConverter.parseHexBinary(hexValue);
        return base64FromBinary(binary);
    }   // base64FromHex
    
    /**
     * Convert (encode) the given binary value, beginning at the given offset and
     * consisting of the given length, using Base64.
     * 
     * @param  value                    A binary value.
     * @param  offset                   Zero-based index where data begins.
     * @param  length                   Number of bytes to encode.
     * @return                          Base64-encoded value.
     * @throws IllegalArgumentException If the given value is null.
     */
    public static String base64FromBinary(byte[] value, int offset, int length) throws IllegalArgumentException {
        return DatatypeConverter.printBase64Binary(Arrays.copyOfRange(value, offset, offset + length));
    }   // base64FromBinary
    
    /**
     * Convert the given String to UTF-8, encode the result with Base64, and return the
     * encoded value as a string. The result string will only contain valid Base64
     * characters.
     * 
     * @param   value   Unicode String value.
     * @return          Base64 encoding of UTF-8 encoded String value.
     */
    public static String base64FromString(String value) {
        return DatatypeConverter.printBase64Binary(toBytes(value));
    }   // base64FromString
    
    /**
     * Decode the given base64 value to binary, then decode the result as a UTF-8 sequence
     * and return the resulting String.
     * 
     * @param base64Value   Base64-encoded value of a UTF-8 encoded string.
     * @return              Decoded string value.
     */
    public static String base64ToString(String base64Value) {
        Utils.require(base64Value.length() % 4 == 0,
                      "Invalid base64 value (must be a multiple of 4 chars): " + base64Value);
        byte[] utf8String = DatatypeConverter.parseBase64Binary(base64Value);
        return toString(utf8String);
    }   // base64ToString
    
    /**
     * Return the Java Unicode escape sequence for the given character. For example, the
     * null character (0x00) is converted to the string "\u0000". This method is useful
     * for creating display-friendly strings that contain hidden non-printable characters.
     *
     * @param ch    Character to be converted.
     * @return      String containing the Java Unicode escape sequence for the given
     *              character.
     */
    public static String charToEscape(char ch) {
        String hexValue = Integer.toHexString(ch);
        if (hexValue.length() == 1) {
            return "\\u000" + hexValue;
        }
        if (hexValue.length() == 2) {
            return "\\u00" + hexValue;
        }
        if (hexValue.length() == 3) {
            return "\\u0" + hexValue;
        }
        return "\\u" + hexValue;
    }   // charToEscape

    /**
     * Silently close the given object and don't complain if it's null or alread closed.
     *
     * @param closeable A closeable object.
     */
    public static void close(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }   // close (Closeable)

    /**
     * Close the given socket and don't complain if it's null, already closed, or
     * socket.close() complains.
     *
     * @param socket    A socket to be closed.
     */
    public static void close(Socket socket) {
        try {
            if (socket != null) {
                socket.close();
            }
        } catch (Exception ex) {
            // ignore
        }
    }   // close (socket)

    // The last value allocated by getTimeMicros(). See below.
    private static long g_lastMicroValue = 0;
    private static final Object g_lastMicroLock = new Object();

    /**
     * Get the current time in microseconds since the epoch. This method is synchronized
     * and guarantees that each successive call, even by different threads, returns
     * increasing values.
     *
     * @return  Current time in microseconds (though not necessarily with microsecond
     *          precision).
     */
    public static long getTimeMicros() {
        // Use use a dedicated lock object rather than synchronizing on the method, which
        // would synchronize on the Utils.class object, which is too coarse-grained.
        synchronized (g_lastMicroLock) {
            // We use System.currentTimeMillis() * 1000 for compatibility with the CLI and
            // other tools. This makes our timestamps "milliseconds since the epoch".
            long newValue = System.currentTimeMillis() * 1000;
            if (newValue <= g_lastMicroValue) {
                // Either two threads called us very quickly or the system clock was set
                // back a little. Just return the last value allocated + 1. Eventually,
                // the system clock will catch up.
                newValue = g_lastMicroValue + 1;
            }
            g_lastMicroValue = newValue;
            return newValue;
        }
    }   // getTimeMicros
    
    /**
     * Get globally unique id as string of 36 characters (same length as UUID.toString).  
     * Subsequent IDs are in almost increasing order (random within same millisecond).
     * Format is the following:
     * <pre>
     *  xxxxxxxxx-yyyyyyyyyyyyyzzzzzzzzzzzzz
     * </pre>
     * where:
     * <ul>
     * <li>x: timestamp in milliseconds (9 characters, encoded with 32 characters keeping the order)
     * <li>y: high 8-byte value of UUID (13 characters, same encoding)
     * <li>z: low 8-byte value of UUID (13 characters, same encoding)
     * </ul>
     * @return Globally unique ID in the format described above.
     */
    public static String getUniqueId() {
        char[] data = new char[36]; 
        long l0 = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        long l1 = uuid.getMostSignificantBits();
        long l2 = uuid.getLeastSignificantBits();
        //we don't use Long.toString(long, radix) because we want to treat values as unsigned
        for(int i = 0; i < 9; i++) {
            data[8 - i] = UID_CHARS[(int)(l0 & 31)];
            l0 >>>= 5;
        }
        if(l0 != 0) throw new RuntimeException("ERROR");
        data[9] = '-';
        for(int i = 0; i < 13; i++) {
            data[22 - i] = UID_CHARS[(int)(l1 & 31)];
            l1 >>>= 5;
        }
        if(l1 != 0) throw new RuntimeException("ERROR");
        for(int i = 0; i < 13; i++) {
            data[35 - i] = UID_CHARS[(int)(l2 & 31)];
            l2 >>>= 5;
        }
        if(l2 != 0) throw new RuntimeException("ERROR");
        
        String v = new String(data);
        return v;
    }
    
    /**
     * Turn the given iterable collection into a simple comma-separated value (CSV)
     * String. For example, ["abc", "def", "xyz"] becomes "abc, def, xyz". Each member in
     * the collection is turned into a string using its toString() method.
     * 
     * @param strIterable   Iterable collection of values.
     * @return              Comma-separated list of values.
     */
    public static String collToCSVString(Iterable<?> strIterable) {
        StringBuilder buffer = new StringBuilder();
        for (Object value : strIterable) {
            if (buffer.length() > 0) {
                buffer.append(", ");
            }
            buffer.append(value.toString());
        }
        return buffer.toString();
    }   // collToCSVString
    
    /**
     * Compress the given message using GZIP, returning the compressed result as a byte[].
     *
     * @param  message      Message to be compressed (must be non-null and length &gt; 0).
     * @return              Decompressed message.
     * @throws IOException  If an error occurs such as a corrupt GZIP format.
     */
    public static byte[] compressGZIP(byte[] message) throws IOException {
        // Write data through a GZIPOutputStream into a ByteArrayOutputStream.
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        GZIPOutputStream gzipOut = new GZIPOutputStream(bytesOut);
        gzipOut.write(message);
        gzipOut.finish();
        return bytesOut.toByteArray();
    }   // compressGZIP

    /**
     * Join together the objects in the given collection into a single string with values
     * separated by the given separator. An empty collection returns an empty String. A
     * collection with a single object returns the toString() of the given object.
     * Otherwise, the toString() value of each object is concatenated in iteration order
     * with the given separation string added between values (but not before the first
     * value or after the last value).
     *
     * @param <T>       Type of object to concatenate.
     * @param values    A Collection of objects.
     * @param sepStr    Separator string to use between values.
     * @return          The Strings concatenated together with the given separate string
     *                  between values.
     */
    public static <T> String concatenate(Collection<T> values, String sepStr) {
        assert values != null;
        assert sepStr != null;

        // Watch for the empty case first.
        if (values.size() == 0) {
            return "";
        }

        // This handles any size >= 1.
        StringBuilder buffer = new StringBuilder();
        boolean bFirst = true;
        for (T value : values) {
            if (bFirst) {
                bFirst = false;
            } else {
                buffer.append(sepStr);
            }
            buffer.append(value.toString());
        }
        return buffer.toString();
    }   // concatenate

    /**
     * Join together the Strings in the given array into a single string with values
     * separated by the given string. An empty array returns an empty String. An array
     * with a single value returns the same value. Otherwise, each value is concatenated
     * in order with the separation string added between values (but not before the first
     * value or after the last value).
     *
     * @param values    An array of Strings.
     * @param sepStr    Separator string to use between values.
     * @return          The Strings concatenated together with the given separate string
     *                  between values.
     */
    public static String concatenate(String[] values, String sepStr) {
        assert values != null;
        assert sepStr != null;

        // Watch for the empty case first.
        if (values.length == 0) {
            return "";
        }

        // This handles any size >= 1.
        StringBuilder buffer = new StringBuilder();
        boolean bFirst = true;
        for (String value : values) {
            if (bFirst) {
                bFirst = false;
            } else {
                buffer.append(sepStr);
            }
            buffer.append(value);
        }
        return buffer.toString();
    }   // concatenate

    /**
     * Returns true if the given string contains any characters that are considered illegal
     * in XML. See http://www.w3.org/TR/xml/#charsets. The legal XML characters XML are:
     * <pre>
     *      #x9 | #xA | #xD | [#x20-#xD7FF] | [#xE000-#xFFFD] | [#x10000-#x10FFFF]
     * </pre>
     * Note that Java String chars can only be up to 0xFFFF 
     *  
     * @param   str Non-null string to be tested.
     * @return      True if the string contains a character considered illegal in XML.
     */
    public static boolean containsIllegalXML(String str) {
        assert str != null;
        for (int index = 0; index < str.length(); index++) {
            char ch = str.charAt(index);
            if ((ch <= 0x08) ||
                (ch >= 0x0B && ch <= 0x0C) ||
                (ch >= 0x0E && ch <= 0x19) ||
                (ch >= 0xD800 && ch <= 0xDFFF) ||
                (ch >= 0xFFFE)) {
                return true;
            }
        }
        return false;
    }   // containsIllegalXML
    
    /**
     * Concatenate the contents of all provided byte[] arrays into a single value from
     * left to right. The result byte[] will equal the sum of all the individual byte[]
     * lengths. The result may be zero-length but it won't be null.
     * 
     * @param  arrays One or more byte[] arrays.
     * @return        Single byte[] with all input byte[] arrays concatenated from left
     *                to right.
     */
    public static byte[] concatenate(byte[]... arrays) {
        // Compute the total size needed.
        int totalLen = 0;
        for (byte[] array : arrays) {
            totalLen += array.length;
        }
        byte[] result = new byte[totalLen];
        
        // Concatenate from left to right.
        int offset = 0;
        for (byte[] array : arrays) {
            for (int inx = 0; inx < array.length; inx++) {
                result[offset++] = array[inx];
            }
        }
        return result;
    }   // concatenate
    
    // This should be greater than any values already in the database since the last run.
    private static AtomicLong g_nextDocID = new AtomicLong(System.currentTimeMillis());

    /**
     * Create a unique value that can be used as an object ID. This method will return a
     * unique value, even in a concurrent environment.
     *
     * @return  A unique value as a string.
     */
    public static String createObjectID() {
        return Long.toString(g_nextDocID.incrementAndGet());
    }   // createObjectID

    /**
     * Convert a date in the format yyyy-MM-dd HH:mm:ss.SSS into a Date object using the
     * UTC timezone. All trailing components are optional in right-to-left order, hence
     * all of the following are valid:
     * <pre>
     *      2012-12-12 12:12:12.123
     *      2012-12-12 12:12:12
     *      2012-12-12 12:12
     *      2012-12-12 12
     *      2012-12-12
     *      2012-12
     *      2012
     * </pre>
     * Omitted time components default to 0; omitted date components default to 1. If the
     * given string is badly formatted, an exception is thrown.
     * 
     * @param  dateString       Date string in the format "yyyy-MM-dd HH:mm:ss.SSS".
     * @return                  Date object in UTC time zone representing the give value.
     * @throws IllegalArgumentException If the given date string is badly formatted.
     */
    public static Date dateFromString(String dateString) throws IllegalArgumentException {
        // parseDate() does all the work.
        return parseDate(dateString).getTime();
    }   // dateFromString

    /**
     * Decompress the given buffer using GZIP, returning the result as a byte[].
     *
     * @param buffer        Buffer contain data to decompress.
     * @return              Decompressed data as a byte[].
     * @throws IOException  If an error occurs reading from the stream, etc.
     */
    public static byte[] decompressGZIP(byte[] buffer) throws IOException {
        // Wrap the buffer in a ByteArrayInputStream and extract the decompressed pieces
        // into a ByteArrayOutputStream.
        ByteArrayInputStream bytesIn = new ByteArrayInputStream(buffer);
        GZIPInputStream gzipIn = new GZIPInputStream(bytesIn);
        ByteArrayOutputStream bytesBuffer = new ByteArrayOutputStream();
        byte[] chunkBuffer = new byte[65536];
        for (int bytesRead = gzipIn.read(chunkBuffer);
             bytesRead > 0;
             bytesRead = gzipIn.read(chunkBuffer)) {
            // Push this chunk into the ByteArrayOutputStream
            bytesBuffer.write(chunkBuffer, 0, bytesRead);
        }
        gzipIn.close();
        return bytesBuffer.toByteArray();
    }   // decompressGZIP

    /**
     * Create and return an InputStream that will read the given byte[] and decompress it
     * as bytes are read from the stream. Reading the byte[] as a decompression stream
     * saves memory when the array is large.
     * 
     * @param buffer        A byte[] containing a GZIP-compressed value.
     * @return              An InputStream that will stream the decompressed value as
     *                      bytes as read.
     * @throws IOException  If an error occurs creating the GZIPInputStream (e.g., the
     *                      byte[] value is not a valid GZIP-compressed message).
     */
    public static InputStream getGZIPDecompressStream(byte[] buffer) throws IOException {
        ByteArrayInputStream bytesIn = new ByteArrayInputStream(buffer);
        GZIPInputStream gzipIn = new GZIPInputStream(bytesIn);
        return gzipIn;
    }   // getGZIPDecompressStream
    
    /**
     * Create a String by converting each byte directly into a character. However, if any
     * byte in the given value is less than a space, a hex string is returned instead.
     *
     * @param value A byte[] to convert.
     * @return      A String of the same values as characters replaced 1-to-1 or converted
     *              into a hex string.
     */
    public static String deWhite(byte[] value) {
        // If the value contains anything less than a space.
        StringBuilder buffer = new StringBuilder();
        boolean bAllPrintable = true;
        for (byte b : value) {
            if ((int)(b & 0xFF) < ' ') {
                bAllPrintable = false;
                break;
            }
        }
        if (bAllPrintable) {
            // All >= space; convert directly to chars.
            for (byte b : value) {
                buffer.append((char)b);
            }
        } else {
            // At least one non-printable. Convert to hex.
            buffer.append("0x");
            for (byte b : value) {
                buffer.append(toHexChar(((int)b & 0xF0) >> 4));
                buffer.append(toHexChar(((int)b & 0xF)));
            }
        }
        return buffer.toString();
    }   // deWhite

    /**
     * Create a String by converting each byte in the given ByteBuffer directly into a
     * character. However, if any byte in the given value is less than a space, a hex
     * string is returned instead. This method calls {@link #copyBytes(ByteBuffer)} to
     * safely copy the bytes in the given buffer and then calls {@link #deWhite(byte[])}
     * to do the work.
     *
     * @param value A ByteBuffer to convert.
     * @return      A String of the same values as characters replaced 1-to-1 or converted
     *              into a hex string.
     */
    public static String deWhite(ByteBuffer value) {
        return deWhite(copyBytes(value));
    }   // deWhite

    /**
     * Indicate if the given string ends with the given character. This is essentially
     * the same as {@link String#endsWith(String)} except that it tests for ending with
     * a character instead of a String. False is returned if the given string is null,
     * empty, or its last character does not match ch.
     *
     * @param str   String to be tested.
     * @param ch    Char to test for.
     * @return      True if the string's last character is ch.
     */
    public static boolean endsWith(String str, char ch) {
        return str != null &&
               str.length() > 0 &&
               str.charAt(str.length() - 1) == ch;
    }   // endsWith

    // Hexadecimal digits
    private final static String hexChars = "0123456789ABCDEF";
    
    /**
     * Converts the given bytes into a hexadecimal representation.
     * 
     * @param bytes Source bytes
     * @return Hexadecimal string
     */
    public static String toHexBytes(byte[] bytes) {
        return toHexBytes(bytes, 0, bytes.length);
    }   // toHexBytes
    
    /**
     * Converts the given bytes into a hexadecimal representation. bytes[offset] through
     * bytes[offset + length - 1] are converted, although the given array's length is
     * never exceeded.
     *
     * @param offset    Index of first byte to convert.
     * @param length    Number of bytes to convert.
     * @param bytes     Source bytes.
     * @return          Hexadecimal string.
     */
    public static String toHexBytes(byte[] bytes, int offset, int length) {
        StringBuilder builder = new StringBuilder();
        for (int index = offset; index < bytes.length && index < (offset + length); index++) {
            byte b = bytes[index];
            int first = (b >> 4) & 15;
            int second = b & 15;
            builder.append(hexChars.charAt(first)).append(hexChars.charAt(second));
        }
        return builder.toString();
    }   // toHexBytes
    
    ///// Calendar formatting

    /**
     * Format a Calendar date with a given precision. 'precision' must be a Calendar
     * "field" value such as Calendar.MINUTE. The allowed precisions and the corresponding
     * string formats returned are:
     * <pre>
     *      Calendar.MILLISECOND:   YYYY-MM-DD hh:mm:ss.SSS 
     *      Calendar.SECOND:        YYYY-MM-DD hh:mm:ss 
     *      Calendar.MINUTE:        YYYY-MM-DD hh:mm 
     *      Calendar.HOUR:          YYYY-MM-DD hh 
     *      Calendar.DATE:          YYYY-MM-DD
     *      Calendar.MONTH:         YYYY-MM
     *      Calendar.YEAR:          YYYY
     * </pre>
     * Note that Calendar.DAY_OF_MONTH is a synonym for Calendar.DATE.
     *
     * @param date      Date as a Calendar to be formatted.
     * @param precision Calendar field value of desired precision.
     * @return          String formatted to the requested precision.
     */
    public static String formatDate(Calendar date, int precision) {
        assert date != null;

        // Remember that the bloody month field is zero-relative!
        switch (precision) {
        case Calendar.MILLISECOND:
            // YYYY-MM-DD hh:mm:ss.SSS
            return String.format("%04d-%02d-%02d %02d:%02d:%02d.%03d",
                                 date.get(Calendar.YEAR), date.get(Calendar.MONTH)+1, date.get(Calendar.DAY_OF_MONTH),
                                 date.get(Calendar.HOUR_OF_DAY), date.get(Calendar.MINUTE), date.get(Calendar.SECOND),
                                 date.get(Calendar.MILLISECOND));
        case Calendar.SECOND:
            // YYYY-MM-DD hh:mm:ss
            return String.format("%04d-%02d-%02d %02d:%02d:%02d",
                                 date.get(Calendar.YEAR), date.get(Calendar.MONTH)+1, date.get(Calendar.DAY_OF_MONTH),
                                 date.get(Calendar.HOUR_OF_DAY), date.get(Calendar.MINUTE), date.get(Calendar.SECOND));
        case Calendar.MINUTE:
            // YYYY-MM-DD hh:mm
            return String.format("%04d-%02d-%02d %02d:%02d",
                                 date.get(Calendar.YEAR), date.get(Calendar.MONTH)+1, date.get(Calendar.DAY_OF_MONTH),
                                 date.get(Calendar.HOUR_OF_DAY), date.get(Calendar.MINUTE));
        case Calendar.HOUR:
            // YYYY-MM-DD hh
            return String.format("%04d-%02d-%02d %02d",
                                 date.get(Calendar.YEAR), date.get(Calendar.MONTH)+1, date.get(Calendar.DAY_OF_MONTH),
                                 date.get(Calendar.HOUR_OF_DAY));
        case Calendar.DATE:
            // YYYY-MM-DD
            return String.format("%04d-%02d-%02d",
                                 date.get(Calendar.YEAR), date.get(Calendar.MONTH)+1, date.get(Calendar.DAY_OF_MONTH));
        case Calendar.MONTH:
            // YYYY-MM
            return String.format("%04d-%02d", date.get(Calendar.YEAR), date.get(Calendar.MONTH)+1);
        case Calendar.YEAR:
            // YYYY
            return String.format("%04d", date.get(Calendar.YEAR));
        }
        throw new IllegalArgumentException("Unknown precision: " + precision);
    }   // formatDate
    
    /**
     * Format a Calendar date as "YYYY-MM-DD HH:mm:ss". This is a convenience method
     * that calles {@link #formatDate(Calendar, int)} with Calendar.SECOND for 'precision'.
     *
     * @param   date    Date as a Calendar to be formatted.
     * @return          "YYYY-MM-DD HH:mm:ss".
     */
    public static String formatDate(Calendar date) {
        return formatDate(date, Calendar.SECOND);
    }   // formatDate

    ///// Date as milliseconds (long) formatting
    
    /**
     * Format a Date.getTime() value in the UTC time zone as a string with a given
     * precision. 'precision' must be a Calendar "field" value such as Calendar.MINUTE.
     * The allowed precisions and the corresponding string formats returned are:
     * <pre>
     *      Calendar.MILLISECOND:   YYYY-MM-DD hh:mm:ss.SSS 
     *      Calendar.SECOND:        YYYY-MM-DD hh:mm:ss 
     *      Calendar.MINUTE:        YYYY-MM-DD hh:mm 
     *      Calendar.HOUR:          YYYY-MM-DD hh 
     *      Calendar.DATE:          YYYY-MM-DD
     *      Calendar.MONTH:         YYYY-MM
     *      Calendar.YEAR:          YYYY
     * </pre>
     * Note that Calendar.DAY_OF_MONTH is a synonym for Calendar.DATE. This is a
     * convenience that converts the given time value to a GregorianCalendar object and
     * then calls {@link #formatDate(Calendar, int)}.
     *
     * @param time      Date.getTime() value to be formatted in UTC time zone.
     * @param precision Calendar field value of desired precision.
     * @return          String formatted to the requested precision.
     */
    public static String formatDateUTC(long time, int precision) {
        // Map date/time to a GregorianCalendar object (GMT time zone).
        GregorianCalendar date = new GregorianCalendar(UTC_TIMEZONE);
        date.setTimeInMillis(time);
        return formatDate(date, precision);
    }   // formatDateUTC
    
    /**
     * Format a Date.getTime() value as "YYYY-MM-DD HH:mm:ss". This method creates a
     * GregorianCalendar object using the <i>local</i> time zone and then calls
     * {@link #formatDate(Calendar)}.
     *
     * @param   time    Date/time in Date.getTime() format (milliseconds since the epoch).
     * @return          "YYYY-MM-DD HH:mm:ss".
     */
    public static String formatDate(long time) {
        // Map date/time to a GregorianCalendar object (local time zone).
        GregorianCalendar date = new GregorianCalendar();
        date.setTimeInMillis(time);
        return formatDate(date, Calendar.SECOND);
    }   // formatDate

    /**
     * Format a Date.getTime() value as "YYYY-MM-DD HH:mm:ss". This method creates a
     * GregorianCalendar object using the GMT time zone and then calls
     * {@link #formatDate(Calendar)}.
     *
     * @param   time    Date/time in Date.getTime() format (milliseconds since the epoch).
     * @return          "YYYY-MM-DD HH:mm:ss".
     */
    public static String formatDateUTC(long time) {
        // Map date/time to a GregorianCalendar object (GMT time zone).
        GregorianCalendar date = new GregorianCalendar(UTC_TIMEZONE);
        date.setTimeInMillis(time);
        return formatDate(date, Calendar.SECOND);
    }   // formatDateUTC

    ///// Date formatting
    
    /**
     * Format a Date in the UTC time zone with a given set of precision. 'precision'
     * must be a Calendar "field" value such as Calendar.MINUTE. The allowed precisions
     * and the corresponding string formats returned are:
     * <pre>
     *      Calendar.MILLISECOND:   YYYY-MM-DD hh:mm:ss.SSS 
     *      Calendar.SECOND:        YYYY-MM-DD hh:mm:ss 
     *      Calendar.MINUTE:        YYYY-MM-DD hh:mm 
     *      Calendar.HOUR:          YYYY-MM-DD hh 
     *      Calendar.DATE:          YYYY-MM-DD
     *      Calendar.MONTH:         YYYY-MM
     *      Calendar.YEAR:          YYYY
     * </pre>
     * Note that Calendar.DAY_OF_MONTH is a synonym for Calendar.DATE. This is a
     * convenience that converts the given Date to a GregorianCalendar object and then
     * calls {@link #formatDate(Calendar, int)}.
     *
     * @param time      Java.util.Date value to be formatted in UTC time zone.
     * @param precision Calendar field value of desired precision.
     * @return          String formatted to the requested precision.
     */
    public static String formatDateUTC(Date time, int precision) {
        // Map date/time to a GregorianCalendar object (GMT time zone).
        GregorianCalendar date = new GregorianCalendar(UTC_TIMEZONE);
        date.setTimeInMillis(time.getTime());
        return formatDate(date, precision);
    }   // formatDateUTC
    
    /**
     * Format a Date value as "YYYY-MM-DD HH:mm:ss". This method creates a
     * GregorianCalendar object using the GMT time zone and then calls
     * {@link #formatDate(Calendar)}.
     *
     * @param   time    Date value.
     * @return          "YYYY-MM-DD HH:mm:ss".
     */
    public static String formatDateUTC(Date time) {
        // Map date/time to a GregorianCalendar object (GMT time zone).
        GregorianCalendar date = new GregorianCalendar(UTC_TIMEZONE);
        date.setTimeInMillis(time.getTime());
        return formatDate(date);
    }   // formatDateUTC
    
    /**
     * Format the given elapsed time in milliseconds as a nice readable string. For
     * example:
     *      3721000 returns "1 hour, 2 minutes, 1 second"
     *      1500    returns "2 seconds"
     *      7201000 returns "2 hours, 1 second"
     * Milliseconds are rounded to the nearest second.
     *
     * @param   millis  Elapsed time in milliseconds.
     * @return          A string in the format "[[h hour[s][, ]][m minute[s][, ]][s second[s]]"
     */
    public static String formatElapsedTime(long millis) {
        // Round to the nearest second.
        long secs = (millis + 500) / 1000;
        StringBuilder buffer = new StringBuilder();
        if (secs > 3600) {
            // >= 1 hour.
            long hours = secs / 3600;
            if (hours == 1) {
                buffer.append("1 hour");
            } else {
                buffer.append("" + hours + " hours");
            }
            secs -= hours * 3600;
        }
        if (secs > 60) {
            // Non-zero minutes.
            if (buffer.length() > 0) {
                buffer.append(", ");
            }
            long mins = secs / 60;
            if (mins == 1) {
                buffer.append("1 minute");
            } else {
                buffer.append("" + mins + " minutes");
            }
            secs -= mins * 60;
        }
        if (secs > 0 || buffer.length() == 0) {
            // Non-zero seconds or the enter value is zero.
            if (buffer.length() > 0) {
                buffer.append(", ");
            }
            if (secs == 1) {
                buffer.append("1 second");
            } else {
                buffer.append("" + secs + " seconds");
            }
        }
        return buffer.toString();
    }   // formatElapsedTime

    /**
     * Extract the bytes in the given ByteBuffer and return it as a byte[] without
     * affecting the mark, position, or limit of the given buffer. This method should be
     * used instead of {@link #getBytes(ByteBuffer)} when the ByteBuffer might be re-read
     * again.
     *
     * @param   bytes   ByteBuffer.
     * @return          Contents between 'position' and 'limit' (aka 'remaining') as a
     *                  byte[]. Parameter object is unaffected.
     */
    public static byte[] copyBytes(ByteBuffer bytes) {
        ByteBuffer copy = bytes.duplicate();
        byte[] result = new byte[copy.remaining()];    // bytes between position and limit
        copy.get(result);
        return result;
    }   // getBytes
    
    /**
     * Extract the bytes in the given ByteBuffer and return it as a byte[]. CAUTION: this
     * method calls ByteBuffer.get(), which <i>transfers</i> bytes from the ByteBuffer to
     * the result buffer. Hence, it is "destructive" in the sense that the value cannot be
     * examined again without calling ByteBuffer.rewind() or something else. 
     *
     * @param   bytes   ByteBuffer.
     * @return          Contents between 'position' and 'limit' (aka 'remaining') as a
     *                  byte[].
     * @see     #copyBytes(ByteBuffer)
     */
    public static byte[] getBytes(ByteBuffer bytes) {
        byte[] result = new byte[bytes.remaining()];    // bytes between position and limit
        bytes.get(result);
        return result;
    }   // getBytes

    /**
     * Verify that the given value is either "true" or "false" and return the corresponding
     * boolean value. If the value is invalid, an IllegalArgumentException is thrown.
     * 
     * @param value Candidate boolean value in string form.
     * @return      Boolean value of string if valid.
     * @throws IllegalArgumentException If the valie is not "true" or "false".
     */
    public static boolean getBooleanValue(String value) throws IllegalArgumentException {
        require("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value),
                "'true' or 'false' expected: " + value);
        return "true".equalsIgnoreCase(value);
    }   // getBooleanValue
    
    /**
     * Compute the MD5 of the given byte[] array. If the MD5 algorithm is not available
     * from the MessageDigest registry, an IllegalArgumentException will be thrown.
     * 
     * @param  src  Binary value to compute the MD5 digest for. Can be empty but not null.
     * @return      16-byte MD5 digest value.
     */
    public static byte[] getMD5(byte[] src) {
        assert src != null;
        try {
            return MessageDigest.getInstance("MD5").digest(src);
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalArgumentException("Missing 'MD5' algorithm", ex);
        }
    }   // getMD5
    
    /**
     * Compute the MD5 of the given Unicode string. Because the MD5 algorithm is
     * byte-oriented, this method first converts the string to bytes using UTF-8 and then
     * calls {@link #getMD5(byte[])}.
     * 
     * @param  src  String value to compute the MD5 digest for. Can be empty but not null.
     * @return      16-byte MD5 digest value.
     */
    public static byte[] getMD5(String src) {
        assert src != null;
        return getMD5(toBytes(src));    // converts via UTF-8
    }   // getMD5
    
    /**
     * Get the stack trace from the given exception or error as a string. By default,
     * Throwable objects only allow accessing the stack trace as an StackTraceElement[]
     * or via a PrintStream or PrintWriter.
     *
     * @param   ex  Exception, Error, or other object that implements Throwable.
     * @return      The exception's stack trace as a string (with embedded newlines).
     */
    public static String getStackTrace(Throwable ex) {
        // Wrap a ByteArrayOutputStream with a PrintStream and write the stack trace to
        // the PrintStream.
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        PrintStream prnStrm = new PrintStream(outStream);
        ex.printStackTrace(prnStrm);
        prnStrm.close();    // flushes content to the byte stream

        // Extract the byte stream as a single string.
        return outStream.toString();
    }   // getStackTrace

    /**
     * Get the concatenated value of all Text nodes that are immediate children of the
     * given Element. If the element has no content, it will not have a child Text node.
     * If it does have content, it will usually have a single child Text node. But in
     * rare cases it could have multiple child Text nodes. If multiple child Text nodes
     * are found, their content is concatenated into a single string, each separated by a
     * single space. The value returned is trimmed of beginning and ending whitespace.
     * If the element has no child Text nodes, or if all child Text nodes are empty or
     * have whitespace-only values, an empty string is returned.
     *
     * @param  elem Element to examine.
     * @return      Concatenated text of all child Text nodes. An empty string is returned
     *              if there are no child Text nodes or they are all empty or contain only
     *              whitespace.
     */
    public static String getElementText(Element elem) {
        StringBuilder result = new StringBuilder();
        NodeList nodeList = elem.getChildNodes();
        for (int index = 0; index < nodeList.getLength(); index++) {
            Node childNode = nodeList.item(index);
            if (childNode != null && (childNode instanceof Text)) {
                result.append(" ");
                result.append(((Text)childNode).getData());
            }
        }
        return result.toString().trim();
    }   // getElementText

    /**
     * Convert (encode) the given binary value to a hex string. 
     * 
     * @param  value                    Binary value.
     * @return                          Hex string representation of same value.
     * @throws IllegalArgumentException If the given value is null.
     */
    public static String hexFromBinary(byte[] value) throws IllegalArgumentException {
        return DatatypeConverter.printHexBinary(value);
    }   // hexFromBinary
    
    /**
     * Convert (decode) the given Hex-encoded String to its binary form.
     * 
     * @param hexValue                  Hex-encoded string.
     * @return                          Decoded binary value.
     * @throws IllegalArgumentException If the given string is not a valid hex value.
     */
    public static byte[] hexToBinary(String hexValue) throws IllegalArgumentException {
        Utils.require(hexValue.length() % 2 == 0,
                      "Invalid hex value (must be a multiple of 2 chars): " + hexValue);
        return DatatypeConverter.parseHexBinary(hexValue);
    }   // hexToBinary
    
    /**
     * Return the first index where the given character occurs in the given buffer or -1
     * if is not found. This is like String.indexOf() but it works on byte[] arrays. If
     * the given buffer is null or empty, -1 is returned.
     * 
     * @param buffer    byte[] to search.
     * @param ch        Character to find.
     * @return          Zero-relative index where character was first found or -1 if the
     *                  character does not occur or is not found.
     */
    public static int indexOf(byte[] buffer, char ch) {
        if (buffer == null) {
            return -1;
        }
        for (int index = 0; index < buffer.length; index++) {
            if (buffer[index] == ch) {
                return index;
            }
        }
        return -1;
    }   // indexOf

    /**
     * Return true if the given character is a decimal digit: 0-9. Compared to
     * Character.isDigit(), this method is stricter and allow recognizes ISO-LATIN-1
     * digits.
     * 
     * @param   ch  Char to test.
     * @return      True if the given character is a valid digit: '0' to '9'.
     */
    public static boolean isDigit(char ch) {
        return (ch >= '0' && ch <= '9');
    }   // isHexDigit
    
    /**
     * Return true if the given character is a valid hex digit: 0-9, a-z, or A-Z.
     * 
     * @param   ch  Char to test.
     * @return      True if the given character is a valid hex digit: 0-9, a-z, or A-Z.
     */
    public static boolean isHexDigit(char ch) {
        return (ch >= '0' && ch <= '9') ||
               (ch >= 'a' && ch <= 'f') ||         
               (ch >= 'A' && ch <= 'F');
    }   // isHexDigit
    
    /**
     * Convenience method that tests if the given string is null or empty. This prevents
     * having to write (str == null || str.isEmpty()).
     * 
     * @param str   String to test, possible null.
     * @return      True if str == null || str.isEmpty()
     */
    public static boolean isEmpty(String str) {
        return str == null || str.isEmpty();
    }   // isEmpty 
    
    /**
     * Return true if the given character is an upper or lower case letter. Compared to
     * Character.isLetter(), this method is stricter and only recognizes ASCII characters
     * "A" to "Z" and "a" to "z" as letters.
     * 
     * @param   ch  Char to test.
     * @return      True if the given character is an upper- or lower-case letter.
     */
    public static boolean isLetter(char ch) {
        return (ch >= 'a' && ch <= 'z') ||         
               (ch >= 'A' && ch <= 'Z');
    }   // isLetter
    
    /**
     * Indicate if the given character is considered a wildchar ('*' or '?').
     *
     * @param ch    Character to test
     * @return      True if the given character is considered a wildchar ('*' or '?').
     */
    public static boolean isWildcardChar(char ch) {
        return ch == '?' || ch == '*';
    }   // isWildcardChar

    /**
     * Indicate if the given string matches the given pattern, which can contain '*'
     * and/or '?' wildcards. The strings are compared case-insensitive. If the given
     * string is null or empty, false is returned. The given pattern must have a value.
     *
     * @param strIn     String to be tested.
     * @param patternIn Pattern to be matched.
     * @return          True if the string matches the pattern.
     * @throws          IllegalArgumentException If the given pattern is null or empty.
     */
    public static boolean matchesPattern(String strIn, String patternIn)
            throws IllegalArgumentException {
        if (patternIn == null || patternIn.length() == 0) {
            throw new IllegalArgumentException();
        }

        // If the test string is empty, we say that it doesn't match the given pattern.
        if (strIn == null || strIn.length() == 0) {
           return false;
        }

        // Upcase both strings so that we perform a case-insensitive comparison.
        String str = strIn.toUpperCase();
        String pattern = patternIn.toUpperCase();

        // Move through string as it matches pattern.
        int strInx = 0;
        int patInx = 0;
        while (strInx < str.length()) {
            // Did we consume all pattern chars?
            if (patInx >= pattern.length()) {
                // Pattern ended but more chars in string
                return false;
            }

            if (pattern.charAt(patInx) == '*') {
                // Multi-char wildcard; start by skipping all next wildcard chars
                do patInx++;
                while (patInx < pattern.length() && isWildcardChar(pattern.charAt(patInx)));
                if (patInx >= pattern.length()) {
                    // Rest of pattern was wildcards; string is considered matched.
                    return true;
                }

                // See if string contains the current non-wildcard pattern char subset
                boolean bSubsetMatched = false;
                int strStartInx = strInx;
                do {
                    // Skip to next string char that matches current char in pattern
                    strInx = strStartInx;
                    while (strInx < str.length() && str.charAt(strInx) != pattern.charAt(patInx)) {
                        strInx++;
                    }
                    if (strInx >= str.length()) {
                        // Hit end of string without finding a match.
                        return false;
                    }

                    // See how far string and pattern characters match.
                    int subPatInx = patInx;
                    do {
                        // Current string and subset chars match; skip both.
                        subPatInx++;
                        strInx++;
                    } while (strInx < str.length() &&
                             subPatInx < pattern.length() &&
                             pattern.charAt(subPatInx) != '*' &&
                             (str.charAt(strInx) == pattern.charAt(subPatInx) ||
                              pattern.charAt(subPatInx) == '?'));
                    if ((subPatInx >= pattern.length() && strInx >= str.length()) ||
                        (subPatInx < pattern.length() && pattern.charAt(subPatInx) == '*')) {
                        // String matched pattern subset (*) or entire rest of pattern.
                        bSubsetMatched = true;
                        patInx = subPatInx;
                    } else {
                        strStartInx++;
                    }
                } while (!bSubsetMatched);
            } else if (pattern.charAt(patInx) == '?' ||
                       str.charAt(strInx) == pattern.charAt(patInx)) {
                // single char matched; advance to next char
                strInx++;
                patInx++;
            } else {
                return false;                 // String char didn't match pattern char
            }
        }

        // If we get here, we hit the end of string; it matches the pattern if the
        // rest of the pattern consists only of '*'
        while (patInx < pattern.length() && pattern.charAt(patInx) == '*') {
            patInx++;
        }
        return patInx >= pattern.length();
    }   // matchesPattern

    /**
     * Compute the MD5 of the given string and return it as a Base64-encoded value. The
     * string is first converted to bytes using UTF-8, and the MD5 is computed on that
     * value. The MD5 value is 16 bytes, but the Base64-encoded string is 24 chars.
     *  
     * @param strIn A Unicode string.
     * @return      Base64-encoded value of the strings UTF-8 encoded value.
     */
    public static String md5Encode(String strIn) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("md5");
            byte[] bin = toBytes(strIn);
            byte[] bout = md5.digest(bin);
            String strOut = javax.xml.bind.DatatypeConverter.printBase64Binary(bout);
            return strOut;
        }catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }   // md5Encode

    /**
     * Convert a date in the format yyyy-MM-dd HH:mm:ss.SSS into a GregorianCalendar object
     * using the UTC timezone. All trailing components are optional in right-to-left order,
     * hence all of the following are valid:
     * <pre>
     *      2012-12-12 12:12:12.123
     *      2012-12-12 12:12:12
     *      2012-12-12 12:12
     *      2012-12-12 12
     *      2012-12-12
     *      2012-12
     *      2012
     * </pre>
     * Omitted time components default to 0; omitted date components default to 1. If the
     * given string is badly formatted, an exception is thrown.
     * 
     * @param  dateString       Date string in the format "yyyy-MM-dd HH:mm:ss.SSS".
     * @return                  GregorianCalendar object in UTC time zone representing the
     *                          given value.
     * @throws IllegalArgumentException If the given date string is badly formatted.
     */
    public static GregorianCalendar parseDate(String dateString) throws IllegalArgumentException {
        // SimpleDateFormat is kinda slow and not thread safe, so we'll parse manually.
        Utils.require(dateString != null, "Date string cannot be null");
        String str = dateString.trim();
        Utils.require(str.length() >= 0, "Invalid date format: " + dateString);
        AtomicInteger pos = new AtomicInteger();
        //otarakanov: log4j ISO8601 format writes date as "1999-11-27 15:49:37,459"
        //with comma instead of point (see 
        //https://logging.apache.org/log4j/1.2/apidocs/org/apache/log4j/helpers/ISO8601DateFormat.html)
        if(str.length() >= 20 && str.charAt(19) == ',') {
            str = str.replace(',', '.');
        }
        
        try {
            // Scan elements
            int year = scanDatePart('\0', 0, str, pos, 4, 4, 1, 9999);
            int month = scanDatePart('-', 1, str, pos, 1, 2, 1, 12);
            int day = scanDatePart('-', 1, str, pos, 1, 2, 1, 31);
            int hour = scanDatePart(' ', 0, str, pos, 1, 2, 0, 23);
            int min = scanDatePart(':', 0, str, pos, 1, 2, 0, 59);
            int sec = scanDatePart(':', 0, str, pos, 1, 2, 0, 59);
            int milli = scanDatePart('.', 0, str, pos, 0, 3, 0, 999);
            
            // Assemble parts into a GregorianCalendar in UTC timezone.
            GregorianCalendar date = new GregorianCalendar(UTC_TIMEZONE);
            date.set(Calendar.YEAR, year);
            date.set(Calendar.MONTH, month - 1);    // 0-relative
            date.set(Calendar.DAY_OF_MONTH, day);
            date.set(Calendar.HOUR_OF_DAY, hour);   // 0-23
            date.set(Calendar.MINUTE, min);
            date.set(Calendar.SECOND, sec);
            date.set(Calendar.MILLISECOND, milli);
            return date;
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid date format: " + dateString);
        }
    }   // parseDate

    /**
     * Parse the given URI and return its path nodes, query, and fragment parts as
     * separate components. The extracted parts are not decoded. For example, if the
     * URI string is:
     * <pre>
     *      /foo%20bar/baz?x=%20+y=2%20#thisbethefragment
     * </pre>
     * the extracted parts are:
     * <pre>
     *      path list:  {"foo%20bar", "baz"}
     *      query:      "x=%20+y=2%20"
     *      fragment:   "thisbethefragment"
     * </pre>
     * All parameters must be non-null.
     *
     * @param uriStr        URI string to be split.
     * @param uriPathList   Will contain path nodes extracted from URI in order, not decoded.
     * @param uriQuery      Will contain query extracted from URI, if any, not decoded.
     * @param uriFragment   Will contain fragment extracted from URI, if any, not decoded.
     * @see   #splitURI(String, StringBuilder, StringBuilder, StringBuilder)
     */
    public static void parseURI(String uriStr, List<String> uriPathList,
                                StringBuilder uriQuery, StringBuilder uriFragment) {
        assert uriStr != null;
        assert uriPathList != null;
        assert uriQuery != null;
        assert uriFragment != null;

        // Start with everything empty.
        uriPathList.clear();
        uriQuery.setLength(0);
        uriFragment.setLength(0);

        // Find location of query (?) and fragment (#) markers, if any.
        int quesInx = uriStr.indexOf('?');
        int hashInx = uriStr.indexOf('#');
        if (hashInx >= 0 && quesInx >= 0 && hashInx < quesInx) {
            // Technically this is an invalid URI since the fragment should always follow
            // the query. We'll just pretend we didn't see the hash.
            hashInx = -1;
        }

        // The path starts at index 0. Point to where it ends.
        int pathEndInx = quesInx >= 0 ? quesInx :
                         hashInx >= 0 ? hashInx : uriStr.length();

        // Split path into nodes based on "/". Append non-empty nodes to path list.
        String[] pathNodes = uriStr.substring(0, pathEndInx).split("/");
        for (String pathNode : pathNodes) {
            if (pathNode.length() > 0) {
                uriPathList.add(pathNode);
            }
        }

        // Extract the query part, if any.
        if (quesInx >= pathEndInx) {
            int quesEndInx = hashInx > quesInx ? hashInx : uriStr.length();
            uriQuery.append(uriStr.substring(quesInx + 1, quesEndInx));
        }

        // Extract the fragment part, if any.
        if (hashInx >= 0) {
            uriFragment.append(uriStr.substring(hashInx + 1, uriStr.length()));
        }
    }   // parseURI

    /**
     * Parse the given XML document, creating a DOM tree whose root Document object is
     * returned. An IllegalArgumentException is thrown if the XML is malformed.
     *
     * @param   xmlDoc      XML document as a String.
     * @return              Root document element of the parsed DOM tree.
     * @throws IllegalArgumentException  If the XML is malformed.
     */
    public static Element parseXMLDocument(String xmlDoc) throws IllegalArgumentException {
        // Parse the given XML document returning its root document Element if it parses.
        // Wrap the document payload as an InputSource.
        Reader stringReader = new StringReader(xmlDoc);
        InputSource inputSource = new InputSource(stringReader);

        // Parse the document into a DOM tree.
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder parser = null;
        Document doc = null;
        try {
            parser = dbf.newDocumentBuilder();
            doc = parser.parse(inputSource);
        } catch (Exception ex) {
            // Turn ParserConfigurationException, SAXException, etc. into an IllegalArgumentException
            throw new IllegalArgumentException("Error parsing XML document: " + ex.getMessage());
        }
        return doc.getDocumentElement();
    }   // parseXMLDocument

    /**
     * Parse an XML document from the given Reader, creating a DOM tree whose root
     * Document object is returned. An IllegalArgumentException is thrown if the XML is
     * malformed.
     *
     * @param  reader       Reader from which XML text is read.
     * @return              Root document element of the parsed DOM tree.
     * @throws IllegalArgumentException  If the XML is malformed.
     */
    public static Element parseXMLDocument(Reader reader) throws IllegalArgumentException {
        // Wrap the document payload as an InputSource.
        InputSource inputSource = new InputSource(reader);
        
        // Parse the document into a DOM tree.
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder parser = null;
        Document doc = null;
        try {
            parser = dbf.newDocumentBuilder();
            doc = parser.parse(inputSource);
        } catch (Exception ex) {
            // Turn ParserConfigurationException, SAXException, etc. into an IllegalArgumentException
            throw new IllegalArgumentException("Error parsing XML document: " + ex.getMessage());
        }
        return doc.getDocumentElement();
    }   // parseXMLDocument
    
    /**
     * Read all data from the given reader into a buffer and return it as a single String.
     * If an I/O error occurs while reading the reader, it is passed through to the caller.
     * The reader is closed when before returning.
     * 
     * @param  reader       Open character reader.
     * @return              All characters read from reader accumulated as a single String.
     * @throws IOException  If an error occurs reading from the reader.
     */
    public static String readerToString(Reader reader) throws IOException {
        assert reader != null;
        
        StringWriter strWriter = new StringWriter();
        char[] buffer = new char[65536];
        int charsRead = reader.read(buffer);
        while (charsRead > 0) {
            strWriter.write(buffer, 0, charsRead);
            charsRead = reader.read(buffer);
        }
        reader.close();
        return strWriter.toString();
    }   // readerToString

    /**
     * Assert that the given expression and throw an IllegalArgumentException if it is
     * false. This check is performed even if -enableassertions is not in effect.
     *
     * @param  assertion                Boolean expression that must be true.
     * @param  errMsg                   String used to in the IllegalArgumentException
     *                                  constructor if thrown.
     * @throws IllegalArgumentException If the expression is false.
     * @see    #require(boolean, String, Object...)
     */
    public static void require(boolean assertion, String errMsg)
            throws IllegalArgumentException {
        if (!assertion) {
            throw new IllegalArgumentException(errMsg);
        }
    }   // require

    /**
     * Assert that the given expression and throw an IllegalArgumentException if it is
     * false. This check is performed even if -enableassertions is not in effect. This
     * method allows the IllegalArgumentException text to be formatted using a
     * String.format() format string and a variable argument list.
     *
     * @param  assertion                Boolean expression that must be true.
     * @param  errMsgFormat             Format string used to compose the error message.
     *                                  Must follow the conventions of String.format()
     *                                  (e.g., "Error on '%s': occurred: %d).
     * @param  args                     Variable argument list passed to String.format().
     * @throws IllegalArgumentException If the expression is false.
     * @see    #require(boolean, String)
     */
    public static void require(boolean assertion, String errMsgFormat, Object... args)
                    throws IllegalArgumentException {
        if (!assertion) {
            throw new IllegalArgumentException(String.format(errMsgFormat, args));
        }
    }   // require
    
    /**
     * Assert that the given org.w3c.doc.Node is a comment element or a Text element and
     * that it ontains whitespace only, otherwise throw an IllegalArgumentException using
     * the given error message. This is helpful when nothing is expected at a certain
     * place in a DOM tree, yet comments or whitespace text nodes can appear.
     *
     * @param node                      A DOM Node object.
     * @param errMsg                    String used to in the IllegalArgumentException
     *                                  constructor if thrown.
     * @throws IllegalArgumentException If the expression is false.
     */
    public static void requireEmptyText(Node node, String errMsg)
            throws IllegalArgumentException {
        require((node instanceof Text) || (node instanceof Comment),
                errMsg + ": " + node.toString());
        if (node instanceof Text) {
            Text text = (Text)node;
            String textValue = text.getData();
            require(textValue.trim().length() == 0, errMsg + ": " + textValue);
        }
    }   // requireEmptyText

    /**
     * Split the given string using the given separate, returning the components as a
     * set. This method does the opposite as {@link #concatenate(Collection, String)}.
     * If a null or empty string is passed, an empty set is returned.
     *
     * @param str       String to be split.
     * @param sepStr    Separator string that lies between values.
     * @return          Set of separated substrings. The set may be empty but it will
     *                  not be null.
     */
    public static Set<String> split(String str, String sepStr) {
        // Split but watch out for empty substrings.
        Set<String> result = new HashSet<String>();
        if (str != null) {
            for (String value : str.split(sepStr)) {
                if (value.length() > 0) {
                    result.add(value);
                }
            }
        }
        return result;
    }   // split

    /**
     * Split the given string using the given separate, returning the components as a
     * SortedSet. This method is similar to {@link #split(String, String)} except that
     * values are sorted by string value. If a null or empty string is passed, an empty
     * set is returned.
     *
     * @param str       String to be split.
     * @param sepStr    Separator string that lies between values.
     * @return          SortedSet of separated substrings. The set may be empty but it
     *                  will not be null.
     */
    public static SortedSet<String> splitSorted(String str, String sepStr) {
        // Split but watch out for empty substrings.
        SortedSet<String> result = new TreeSet<String>();
        if (str != null) {
            for (String value : str.split(sepStr)) {
                if (value.length() > 0) {
                    result.add(value);
                }
            }
        }
        return result;
    }   // splitSorted
    
    /**
     * Split the given string by a separator char. Unlike split(String,String), doesn't use RegEx
     *
     * @param str       String to be split.
     * @param sepChr    Separator character that lies between values.
     * @return          List of separated substrings
     */
    public static List<String> split(String str, char sepChr) {
    	List<String> result = new ArrayList<String>();
    	int idx = 0;
    	while(true) {
    		int idx2 = str.indexOf(sepChr, idx);
    		if(idx2 < 0) {
    			result.add(str.substring(idx));
    			break;
    		}
    		result.add(str.substring(idx, idx2));
    		idx = idx2 + 1;
    	}
        return result;
    }
    
    
    /**
     * Split-out the path, query, and fragment parts of the given URI string. The URI is
     * expected to that obtained from a GET or other HTTP request. The extracted parts
     * are not decoded. For example, if the URI string is:
     * <pre>
     *      /foo/bar?x=%20+y=2%20#thisbethefragment
     * </pre>
     * the extract parts are:
     * <pre>
     *      path:       /foo/bar
     *      query:      x=%20+y=2%20
     *      fragment:   thisbethefragment
     * </pre>
     * All parameters must be non-null.
     *
     * @param uriStr        URI string to be split.
     * @param uriPath       Will contain path extracted from URI.
     * @param uriQuery      Will contain query extracted from URI, if any, not decoded.
     * @param uriFragment   Will contain fragment extracted from URI, if any, not decoded.
     */
    public static void splitURI(String uriStr, StringBuilder uriPath,
                                StringBuilder uriQuery, StringBuilder uriFragment) {
        assert uriStr != null;
        assert uriPath != null;
        assert uriQuery != null;
        assert uriFragment != null;

        // Find location of query (?) and fragment (#) markers, if any.
        int quesInx = uriStr.indexOf('?');
        int hashInx = uriStr.indexOf('#');
        if (hashInx >= 0 && quesInx >= 0 && hashInx < quesInx) {
            // Technically this is an invalid URI since the fragment should always follow
            // the query. We'll just pretend we didn't see the hash.
            hashInx = -1;
        }

        // The path starts at index 0. Point to where it ends.
        uriPath.setLength(0);
        int pathEndInx = quesInx >= 0 ? quesInx :
                         hashInx >= 0 ? hashInx :
                         uriStr.length();
        uriPath.append(uriStr.substring(0, pathEndInx));

        // Extract the query part, if any.
        uriQuery.setLength(0);
        if (quesInx >= pathEndInx) {
            int quesEndInx = hashInx > quesInx ? hashInx : uriStr.length();
            uriQuery.append(uriStr.substring(quesInx + 1, quesEndInx));
        }

        // Extract the fragment part, if any.
        uriFragment.setLength(0);
        if (hashInx >= 0) {
            uriFragment.append(uriStr.substring(hashInx + 1, uriStr.length()));
        }
    }   // splitURI

    /**
     * Split the given query component of a URI into its decoded parts. First, parts
     * delimited by non-encoded '{@literal &}'s are separated. Then, each unencoded '+' is replaced
     * with a space within each part. Finally, the parts are URL-decoded and stored in the
     * result string array. For example, if the URI is:
     * <pre>
     *      "/foo/bar?a=cat{@literal &}b=dog+%24sheep"
     * </pre>
     * The query component can be extracted using
     * {@link #splitURI(String, StringBuilder, StringBuilder, StringBuilder)}, yielding:
     * <pre>
     *    "a=cat{@literal &}b=dog+%24sheep".
     * </pre>
     * If this string is then passed to this method, it would return the following length-
     * two array of Strings:
     * <pre>
     *    result[0] = "a=cat"
     *    result[1] = "b=dog $sheep"
     * </pre>
     *
     * @param uriQuery  Query component of a URI (cannot be null).
     * @return          An array of separated, decoded strings, one per part.
     */
    public static String[] splitURIQuery(String uriQuery) {
        assert uriQuery != null;

        // Separate '&' parts into separate strings.
        String[] parts = uriQuery.split("&");
        for (int inx = 0; inx < parts.length; inx++) {
            // Replace '+' signs with ' ' and decode this part.
            parts[inx] = Utils.urlDecode(parts[inx].replace('+', ' '));
        }
        return parts;
    }   // splitURIQuery

    /**
     * Split the given query component of a URI into its decoded parts and return them as
     * a name/value map. First, parts delimited by non-encoded '{@literal &}'s are separated. Then,
     * each unencoded '+' is replaced with a space within each part. Finally, the parts
     * are URL-decoded and stored in the result map keyed by name. For example, if the URI
     * is:
     * <pre>
     *      "/foo/bar?a=cat{@literal &}b=dog+%24sheep{@literal &}c"
     * </pre>
     * The query component can be extracted using
     * {@link #splitURI(String, StringBuilder, StringBuilder, StringBuilder)}, yielding:
     * <pre>
     *    "a=cat{@literal &}b=dog+%24sheep{@literal &}c".
     * </pre>
     * If this string is then passed to this method, it would return the following Map:
     * <pre>
     *    "a": "cat"
     *    "b": "dog $sheep"
     *    "c": ""
     * </pre>
     * As shown, if a parameter has no "=" sign, the whole parameter is used as the name
     * and the value is an empty string. This method calls {@link #splitURIQuery(String)}.
     *
     * @param uriQuery                  Query component of a URI (cannot be null).
     * @return                          A map of decoded parameter name/value pairs.
     * @throws IllegalArgumentException If a parameter is specified twice.
     */
    public static Map<String, String> parseURIQuery(String uriQuery) throws IllegalArgumentException {
    	if (uriQuery == null || uriQuery.isEmpty()) {
    	    return new HashMap<String, String>(0);
    	}
        String[] queryParts = Utils.splitURIQuery(uriQuery);
        Map<String, String> map = new HashMap<String, String>(queryParts.length);
        for (String queryPart : queryParts) {
            int eqInx = queryPart.indexOf('=');
            String paramName = eqInx < 0 ? queryPart : queryPart.substring(0, eqInx);
            String paramValue = eqInx < 0 ? "" : queryPart.substring(eqInx + 1);
            require(map.put(paramName, paramValue) == null,
                    "Query parameter can only be specified once: " + paramName);
        }
        return map;
    }  // parseURIQuery

    /**
     * Concatenate and encode the given name/value pairs into a valid URI query string.
     * This method is the complement of {@link #parseURIQuery(String)}.
     * 
     * @param uriParams Unencoded name/value pairs.    
     * @return          URI query in the form {name 1}={value 1}{@literal &}...{@literal &}{name}={value n}.
     */
    public static String joinURIQuery(Map<String, String> uriParams) {
        StringBuilder buffer = new StringBuilder();
        for (String name : uriParams.keySet()) {
            String value = uriParams.get(name);
            if (buffer.length() > 0) {
                buffer.append("&");
            }
            buffer.append(Utils.urlEncode(name));
            if (!Utils.isEmpty(value)) {
                buffer.append("=");
                buffer.append(Utils.urlEncode(value));
            }
        }
        return buffer.toString();
    }   // joinURIQuery 
    
    /**
     * Indicate if the given string starts with the given prefix. This is a more compact
     * way of writing string.regionMatches(true, 0, prefix, 0, prefix.length()).
     *
     * @param string    String to be tested.
     * @param prefix    Prefix to compare against string.
     * @return          True if string starts with prefix, case-insensitive.
     */
    public static boolean startsWith(String string, String prefix) {
        return string.regionMatches(true, 0, prefix, 0, prefix.length());
    }   // startsWith

    /**
     * Convert the given String to a byte[] value using UTF-8 and wrap in a ByteBuffer.
     * 
     * @param value String value.
     * @return      UTF-8 converted value wrapped in a ByteBuffer.
     */
    public static ByteBuffer toByteBuffer(String value) {
        return ByteBuffer.wrap(toBytes(value));
    }   // toByteBuffer
    
    /**
     * Convert the given string to a byte[] in using the {@link #UTF8_CHARSET} encoder.
     * This is the inverse of {@link #toString(byte[])}. A null value is allowed, which
     * begets a null result.
     *
     * @param str   String value to be converted.
     * @return      Lossless, encoded value as a byte[], or null if str is null.
     */
    public static byte[] toBytes(String str) {
        if (str == null) {
            return null;
        }
        //optimization for ascii strings
        byte[] ascii = toAsciiBytes(str);
        if(ascii != null) return ascii;
        
        ByteBuffer bb = UTF8_CHARSET.encode(str);
        return getBytes(bb);
    }   // toBytes

    
    // return string as bytes if it has only ascii symbols, or null
    private static byte[] toAsciiBytes(String str) {
        for(int i = 0; i < str.length(); i++) {
            if(str.charAt(i) > 127) return null;
        }
        byte[] bytes = new byte[str.length()];
        for(int i = 0; i < str.length(); i++) {
            bytes[i] = (byte)str.charAt(i);
        }
        return bytes;
    }
    
    /**
     * Convert a long to a byte[] using the format Cassandra wants for a column or
     * supercolumn name. The antimethod for this one is {@link #toLong(byte[])}.
     *
     * @param value Long value to be converted.
     * @return      Same value encoded into an byte[8] array.
     */
    public static byte[] toBytes(long value) {
        byte[] bytes = new byte[8];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.putLong(value);
        return bytes;
    }   // toBytes

    /**
     * Convert the given value, which must be between 0 and 15, into its equivalent hex
     * character between 0 and F.
     * 
     * @param  value                    Integer value between 0 and 15.
     * @return                          Equivalent hex character between 0 and F.
     * @throws IllegalArgumentException If the value is out of range.
     */
    public static char toHexChar(int value) {
        switch (value) {
        case 0:  return '0';
        case 1:  return '1';
        case 2:  return '2';
        case 3:  return '3';
        case 4:  return '4';
        case 5:  return '5';
        case 6:  return '6';
        case 7:  return '7';
        case 8:  return '8';
        case 9:  return '9';
        case 10: return 'A';
        case 11: return 'B';
        case 12: return 'C';
        case 13: return 'D';
        case 14: return 'E';
        case 15: return 'F';
        default:
            throw new IllegalArgumentException("Value must be between 0 and 15: " + value);
        }
    }   // toHexChar 
    
    /**
     * Convert the given hex character (0-9, A-Z, or a-z) into its decimal equivalent value.
     * 
     * @param  ch   A hex character.
     * @return      Decimal equivalent of value (0-15).
     */
    public static int fromHexChar(char ch) {
        switch (ch) {
        case '0': return 0;
        case '1': return 1;
        case '2': return 2;
        case '3': return 3;
        case '4': return 4;
        case '5': return 5;
        case '6': return 6;
        case '7': return 7;
        case '8': return 8;
        case '9': return 9;
        case 'a': case 'A': return 10;
        case 'b': case 'B': return 11;
        case 'c': case 'C': return 12;
        case 'd': case 'D': return 13;
        case 'e': case 'E': return 14;
        case 'f': case 'F': return 15;
        default:
            throw new IllegalArgumentException("Must be a hex char: " + ch);
        }
    }   // fromHexChar 
    
    /**
     * Parse the given string and return a list of the whitespace-delimited tokens that it
     * contains mapped to the number of occurrences of each token. Only whitespace
     * characters (SP, CR, LF, TAB, FF, VT) are used to delimit tokens.
     *
     * @param string    String to be tokenized.
     * @return          Map of tokens to occurrence counts.
     */
    public static Map<String, AtomicInteger> tokenize(String string) {
        Map<String, AtomicInteger> result = new HashMap<String, AtomicInteger>();
        String[] tokens = string.split("\\s");  // regular expression for "all whitespace"
        for (String token : tokens) {
            // For some reasons, sometimes split() creates empty values.
            if (token.length() == 0) {
                continue;
            }

            // Tokens are returned down-cased.
            String tokenDown = token.toLowerCase();
            AtomicInteger count = result.get(tokenDown);
            if (count == null) {
                result.put(tokenDown, new AtomicInteger(1));
            } else {
                count.incrementAndGet();
            }
        }
        return result;
    }   // tokenize

    /**
     * Convert a long value encoded as a byte[] back into its long value. This method is
     * the opposite of {@link #toBytes(long)}.
     *
     * @param bytes byte[8] array with an encoded long value.
     * @return      Decoded long value.
     */
    public static long toLong(byte[] bytes) {
        // Extract a long stored in a byte[] using a ByteBuffer method.
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return buffer.getLong();
    }   // toLong

    /**
     * Convert the given byte[] to a String using the {@link #UTF8_CHARSET} decoder. This
     * is the inverse of {@link #toBytes(String)}. As with that method, null begets null.
     *
     * @param bytes A byte[] representing a String value.
     * @return      The decoded String value, or null if null is given.
     */
    public static String toString(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        //optimization for ASCII string
        String ascii = toAsciiString(bytes);
        if(ascii != null) return ascii;
        
        return new String(bytes, UTF8_CHARSET);
    }   // toString

    // return string if bytes have only ascii symbols, or null
    private static String toAsciiString(byte[] bytes) {
        for(int i = 0; i < bytes.length; i++) {
            if(bytes[i] < 0) return null;
        }
        char[] chars = new char[bytes.length];
        for(int i = 0; i < bytes.length; i++) {
            chars[i] = (char)bytes[i];
        }
        return new String(chars);
    }
    
    /**
     * Extract the byte[] within the given ByteBuffer and decode into a String using UTF-8.
     * This method calls {@link #copyBytes(ByteBuffer)}, which examines the ByteBuffer
     * without side-effects, therefore allowing it to be read again.
     * 
     * @param bytes ByteBuffer object.
     * @return      Internal byte[] value converted to a String using UTF-8.
     */
    public static String toString(ByteBuffer bytes) {
        return toString(copyBytes(bytes));
    }   // toString

    /**
     * Convert the a subset of given byte[] starting at index 'offset' for 'length' bytes
     * to a String using the reverse process used by {@link #toBytes(String)}. As with
     * that method, null begets null.
     *
     * @param bytes     Byte[] to convert.
     * @param offset    Index of first byte to convert.
     * @param length    Number of bytes to convert.
     * @return          Decoded string, or null if null is given.
     */
    public static String toString(byte[] bytes, int offset, int length) {
        if (bytes == null) {
            return null;
        }
        //optimization for ASCII string
        String ascii = toAsciiString(bytes, offset, length);
        if(ascii != null) return ascii;
        
        return new String(bytes, offset, length, UTF8_CHARSET);
    }   // toString

    // return string if bytes have only ascii symbols, or null
    private static String toAsciiString(byte[] bytes, int offset, int length) {
        for(int i = 0; i < length; i++) {
            if(bytes[offset + i] < 0) return null;
        }
        char[] chars = new char[length];
        for(int i = 0; i < length; i++) {
            chars[i] = (char)bytes[offset + i];
        }
        return new String(chars);
    }
    
    
    /**
     * Ensure that the given string is no longer than the given max length, truncating it
     * if necessary. If string.length() is &lt;= maxLength, the same string is returned.
     * Otherwise, a substring of the first maxLength characters is returned.
     * 
     * @param string       String to test.
     * @param maxLength    Maximum length.
     * @return             Same or truncated string as described above.
     */
    public static String truncateTo(String string, int maxLength) {
        if (string.length() <= maxLength) {
            return string;
        }
        return string.substring(0, maxLength);
    }   // truncateTo
    
    /**
     * Truncate the given GregorianCalendar date to the nearest week. This is done by
     * cloning it and rounding the value down to the closest Monday. If the given date
     * already occurs on a Monday, a copy of the same date is returned.
     * 
     * @param   date    A GregorianCalendar object.
     * @return          A copy of the same value, truncated to the nearest Monday.
     */
    public static GregorianCalendar truncateToWeek(GregorianCalendar date) {
        // Round the date down to the MONDAY of the same week.
        GregorianCalendar result = (GregorianCalendar)date.clone();
        switch (result.get(Calendar.DAY_OF_WEEK)) {
            case Calendar.TUESDAY:   result.add(Calendar.DAY_OF_MONTH, -1); break;
            case Calendar.WEDNESDAY: result.add(Calendar.DAY_OF_MONTH, -2); break;
            case Calendar.THURSDAY:  result.add(Calendar.DAY_OF_MONTH, -3); break;
            case Calendar.FRIDAY:    result.add(Calendar.DAY_OF_MONTH, -4); break;
            case Calendar.SATURDAY:  result.add(Calendar.DAY_OF_MONTH, -5); break;
            case Calendar.SUNDAY:    result.add(Calendar.DAY_OF_MONTH, -6); break;
            default: break;
        }
        return result;
    }   // truncateToWeek

    /**
     * Decode the given string by replacing %-escape sequences with the corresponding
     * real characters. For example "A%20B" returns "A B". If a null string is passed,
     * the result is null. This method calls URLDecoder.decode("UTF-8", strIn), which
     * converts UTF-8 sequences to Unicode characters. It also converts non-escaped
     * '+' signs into a space.
     *
     * @param  strIn    Input string to decode.
     * @return          Ouput string with %hh sequences replaced by the corresponding
     *                  character, or null if strIn is null.
     * @see             #urlEncode(String)
     */
    public static String urlDecode(String strIn) {
        // Null begets null.
        if (strIn == null) {
            return null;
        }

        try {
            return URLDecoder.decode(strIn, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            // This should never happen since UTF-8 always exists.
            throw new IllegalArgumentException("UTF-8");
        }
    }   // urlDecode

    /**
     * Encode the given string by replacing characters not legal in URLs with the
     * appropriate escape sequences. For example "A B" becomes "A%20B". If a null string
     * is passed, the result is null. This method calls URLEncoder.decode("UTF-8", strIn),
     * which converts non-ASCII characters to UTF-8 sequences and then escapes the UTF-8
     * sequences. It also converts spaces into '+' signs.
     *
     * @param  strIn    Input string to encode.
     * @return          URL-encoded version of the same string, or null if strIn is null.
     * @see             #urlDecode(String)
     */
    public static String urlEncode(String strIn) {
        // Null begets null.
        if (strIn == null) {
            return null;
        }

        try {
            return URLEncoder.encode(strIn, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            // This should never happen since UTF-8 always exists.
            throw new IllegalArgumentException("UTF-8");
        }
    }   // urlEncode

    ////// Private methods
    
    /**
     * Scan a date/time part, verify its value, and return it. All parts are considered
     * optional, so if pos.get() is >= str.length(), the default value is returned and
     * nothing is parsed. Otherwise, the part must begin with given prefix, consist of
     * digits in the required range, and denote a value in the given range. pos is
     * incremented to reflect characters parsed.
     * 
     * @param prefix        If not '\0', the part must begin with this character.
     * @param defaultValue  If all characters are consumed, this value is returned.
     * @param str           The string to be parsed.
     * @param pos           The current parse position (index into str).
     * @param minDigits     Minimum number of digits the part must have.
     * @param maxDigits     Maximum number of digits parsed for part.
     * @param minValue      Minimum value part can have.
     * @param maxValue      Maximum value part can have.
     * @throws              IllegalArgumentException If the part is present but has the
     *                      wrong prefix, too few digits, or is out of range.
     * @return              Value of scanned date/time part.
     */
    private static int scanDatePart(char prefix, int defaultValue, String str, AtomicInteger pos,
                                    int minDigits, int maxDigits, int minValue, int maxValue) 
            throws IllegalArgumentException {
        // If all characters are consumed, just return the default value.
        if (pos.get() >= str.length()) {
            return defaultValue;
        }
        
        // If there's a prefix character, require it.
        if (prefix != '\0') {
            require(str.charAt(pos.getAndIncrement()) == prefix, "'" + prefix + "' expected");
        }
        
        // Scan up to maxDigits into a numeric value.
        int value = 0;
        int digitsScanned = 0;
        while (pos.get() < str.length() && digitsScanned < maxDigits) {
            char ch = str.charAt(pos.get());
            if (ch >= '0' && ch <= '9') {
                value = value * 10 + (ch - '0');
                digitsScanned++;
                pos.incrementAndGet();
            } else {
                break;
            }
        }
        
        // Ensure we got the required minimum digits and the value is within range.
        Utils.require(digitsScanned >= minDigits && value >= minValue && value <= maxValue,
                      "Invalid value for date/time part");
        return value;
    }   // scanDatePart

	/**
	 * Deletes a directory recursively.
	 * 
	 * @param dir  Directory to delete.
	 * @return     True if the delete was successful.
	 */
	public static boolean deleteDirectory(final File dir) {
		boolean success = true;
		if (dir != null && dir.exists()) {
			try {
				if (dir.isDirectory()) {
					for (final File file : dir.listFiles()) {
						if(file == null) {
							return false;
						}
						if (!deleteDirectory(file)) {
							success = false;
							return success;
						}
					}
				}
				if (!dir.delete()) {
					success = false;
					return success;
				}
				return success;
			} catch (Exception e) {
				// Failed to delete files or directory
			}
		}
		return false;
	}

}   // class Utils
