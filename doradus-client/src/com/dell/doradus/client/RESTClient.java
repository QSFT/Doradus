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

package com.dell.doradus.client;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dell.doradus.common.ContentType;
import com.dell.doradus.common.HttpCode;
import com.dell.doradus.common.HttpDefs;
import com.dell.doradus.common.HttpMethod;
import com.dell.doradus.common.RESTResponse;
import com.dell.doradus.common.Utils;

/**
 * Provides a wrapper class around a network socket for sending REST commands and reading
 * responses. RESTClient can create a secured (HTTPS) or secured (HTTP) connection. Input
 * entities are automatically compressed if desired. If a socket is disconnected during a
 * request, up to {@link #MAX_SOCKET_RETRIES} attempts are made to reconnect the socket to
 * the same server.
 * <p>
 * A RESTClient can be used directly to access a Doradus server. Alternatively, use a
 * {@link Client} object, which automatically creates and uses a RESTClient connection.
 * 
 * @see com.dell.doradus.client.RESTConnector
 */
public class RESTClient implements Closeable {
    // Maximum retries on a socket error:
    private static final int MAX_SOCKET_RETRIES = 3;
    
    // Custom buffer size and Nagle's option:
    private static final int     NET_BUFFER_SIZE = 65536;
    private static final boolean USE_CUSTOM_BUFFER_SIZE = true;
    private static final boolean DISABLE_NAGLES = true;
    
    // Members the reflect the Doradus server we're talking to:
    private final String            m_host;
    private final int               m_port;
    private Socket                  m_socket;
    private InputStream             m_inStream;
    private OutputStream            m_outStream;
    private SSLTransportParameters  m_sslParams;
    private boolean                 m_bCompress;
    private Credentials             m_credentials;
    
    // Current "Accept" format we use until changed.
    private ContentType m_acceptFormat = ContentType.APPLICATION_JSON;  // default
    
    // Logging interface:
    private Logger m_logger = LoggerFactory.getLogger(getClass().getSimpleName());
    
    /**
     * Create a RESTClient while opening a connection to the specified host and port.
     * This constructor requests an unsecured connection (no TLS/SSL).
     *  
     * @param host  Host name or IP address to use.
     * @param port  Port number to use.
     */
    public RESTClient(String host, int port) {
        this(null, host, port);
    }   // constructor
    
    /**
     * Create a RESTClient by opening a connection to the specified host and port,
     * optionally using TLS/SSL.
     *  
     * @param sslParams {@link SSLTransportParameters} needed to use TLS/SSL for this
     *                  connection. Can be null to request a non-secured connection.
     * @param host      Host name or IP address to use.
     * @param port      Port number to use.
     */
    public RESTClient(SSLTransportParameters sslParams, String host, int port) {
        m_sslParams = sslParams;
        m_host      = host;
        m_port      = port;
        
        // Open a socket to the specified server, throwing an exception if it won't open
        // and capture the input/output streams.
        try {
            createSocket();
            m_inStream  = m_socket.getInputStream();
            m_outStream = m_socket.getOutputStream();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }   // constructor
    
    /**
     * Create a new RESTClient using the same parameters used to create this one: host,
     * port, and TLS parameters. The given RESTClient's other connection parameters are
     * also copied.
     * 
     * @param restClient    RESTClient object to copy.
     */
    public RESTClient(RESTClient restClient) {
        this(restClient.m_sslParams, restClient.m_host, restClient.m_port);
        setAcceptType(restClient.m_acceptFormat);
        setCompression(restClient.m_bCompress);
        setCredentials(restClient.m_credentials);
    }   // constructor
    
    /**
     * Return the {@link SSLTransportParameters} used for this RESTClient connection, if
     * any.
     * 
     * @return  SSLTransportParameters used for this RESTClient connection, or null if an
     *          unsecured (HTTP) connection was created.
     */
    public SSLTransportParameters getSSLParams() { return m_sslParams; }

    /**
     * Get the compression option for this RESTClient.
     * 
     * @return  True if compression is set for all message bodies.
     */
    public boolean getCompression() { return m_bCompress; }
    
    /**
     * Get the credentials being used to authenticate requests made by this RESTClient. It
     * may be null to indicate that requests are being made unauthenticated.
     * 
     * @return  {@link Credentials} being used to authorize requests made by this
     *          RESTClient. May be null.
     */
    public Credentials getCredentials() { return m_credentials; }
    
    /**
     * Get the REST API host name/IP address used for this RESTClient connection.
     * 
     * @return  The host parameter used for this RESTClient connection.
     */
    public String getHost() { return m_host; }
    
    /**
     * Get the REST API port number used for this RESTClient connection.
     * 
     * @return  The port number used for this RESTClient connection.
     */
    public int getPort() { return m_port; }

    /**
     * Set the "accept" type used in requests to the given value. This value is used for
     * all requests until it is changed.
     * 
     * @param acceptType    Accept type such as {@link ContentType#APPLICATION_JSON}.
     */
    public void setAcceptType(ContentType acceptType) {
        m_acceptFormat = acceptType;
    }   // setAcceptType
    
    /**
     * Enable or disable compression of message entities sent to and received from the
     * server. When enabled, input entities are compressed with GZIP before sending to the
     * server, and compressed output entities are requested from the server. When disabled,
     * entities are sent and received as-is.
     * 
     * @param bCompress Enable (TRUE) or disable (FALSE) message compression.
     */
    public void setCompression(boolean bCompress) {
        m_bCompress = bCompress;
    }   // setCompression
    
    /**
     * Set the credentials to be used for requests made through this RESTClient.
     * 
     * @param credentials {@link Credentials} to be used for future REST commands. If
     *                    null is passed, no credentials will be used.
     */
    public void setCredentials(Credentials credentials) {
        m_credentials = credentials;
    }   // setCredentials
    
    /**
     * Send a REST command with the given method and URI (but no entityt) to the
     * server and return the response in a {@link RESTResponse} object.
     * 
     * @param method        HTTP method such as "GET" or "POST".
     * @param uri           URI such as "/foo/bar?baz"
     * @return              {@link RESTResponse} containing response from server.
     * @throws IOException  If an I/O error occurs on the socket.
     */
    public RESTResponse sendRequest(HttpMethod method, String uri) throws IOException {
        return sendRequest(method, uri, null, null);
    }   // sendRequest
    
    /**
     * Send a REST command with the given method, URI, content-type, and body to the
     * server and return the response in a {@link RESTResponse} object.
     * 
     * @param method        HTTP method such as "GET" or "POST".
     * @param uri           URI such as "/foo/bar?baz"
     * @param contentType   Content-Type of body such as "application/json".
     * @param body          Input entity to send with request in binary.
     * @return              {@link RESTResponse} containing response from server.
     * @throws IOException  If an error occurs on the socket. 
     */
    public RESTResponse sendRequest(HttpMethod method, String uri,
                                    ContentType contentType, byte[] body)
            throws IOException {
        Map<String, String> headers = new HashMap<>();
        if (contentType != null) {
            headers.put(HttpDefs.CONTENT_TYPE, contentType.toString());
        }
        
        // Compress body using GZIP and add a content-encoding header if compression is requested.
        byte[] entity = body;
        if (m_bCompress && body != null && body.length > 0) {
            entity = Utils.compressGZIP(body);
            headers.put(HttpDefs.CONTENT_ENCODING, "gzip");
        }
        return sendAndReceive(method, uri, headers, entity);
    }   // sendRequest

    /**
     * Send a REST command with the given method, URI, content-type, and compressed
     * message body to the server and return the response in a {@link RESTResponse}
     * object. Since the message body is compressed, the Content-Encoding header
     * is added regardless of whether {@link #setCompression(boolean)} was called.
     * 
     * @param method        HTTP method such as "GET" or "POST".
     * @param uri           URI such as "/foo/bar?baz"
     * @param contentType   Content-Type of body such as "application/json".
     * @param body          Input entity to send, compressed with GZIP.
     * @return              {@link RESTResponse} containing response from server.
     * @see                 #setCompression(boolean)
     * @throws IOException  If a socket error occurs
     */
    public RESTResponse sendRequestCompressed(HttpMethod  method,
                                              String      uri,
                                              ContentType contentType,
                                              byte[]      body) throws IOException {
        Map<String, String> headers = new HashMap<>();
        headers.put(HttpDefs.CONTENT_TYPE, contentType.toString());
        headers.put(HttpDefs.CONTENT_ENCODING, "gzip");
        return sendAndReceive(method, uri, headers, body);
    }   // sendRequestCompressed
    
    /**
     * Closet this object's socket, making it unable to send any more requests.
     */
    @Override
    public void close() {
        Utils.close(m_socket);
        m_socket = null;
    }   // close

    /**
     * Return true if the socket connection associated with this RESTClient is closed.
     * 
     * @return  True if the socket connection associated with this RESTClient is closed.
     */
    public boolean isClosed() {
        return m_socket == null || m_socket.isClosed();
    }   // isClosed
    
    //----- Private methods
    
    // If credentials have been specified with a tenant, append then the query string
    // "?tenant={tenant}" or "&tenant={tenant}" to the given URI.
    private String addTenantParam(String uri) {
        if (m_credentials != null && m_credentials.getTenant() != null) {
            if (uri.indexOf("?") > 0) {
                return uri + "&tenant=" + m_credentials.getTenant();
            } else {
                return uri + "?tenant=" + m_credentials.getTenant();
            }
        }
        return uri;
    }   // addTenantParam
    
    // Attempt to reconnect to the Doradus server
    private void reconnect() throws IOException {
        // First ensure we're closed.
        close();
        
        // Attempt to re-open.
        try {
			createSocket();
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException("Cannot connect to server", e);
		}
        m_inStream = m_socket.getInputStream();
        m_outStream = m_socket.getOutputStream();
    }   // reconnect
    
    // Add standard headers to the given request and send it.
    private RESTResponse sendAndReceive(HttpMethod          method,
                                        String              uri,
                                        Map<String, String> headers,
                                        byte[]              body) throws IOException {
        // Add standard headers
        assert headers != null;
        headers.put(HttpDefs.HOST, m_host);
        headers.put(HttpDefs.ACCEPT, m_acceptFormat.toString());
        headers.put(HttpDefs.CONTENT_LENGTH, Integer.toString(body == null ? 0 : body.length));
        if (m_bCompress) {
            headers.put(HttpDefs.ACCEPT_ENCODING, "gzip");
        }
        if (m_credentials != null) {
            String authString =
                "Basic " + Utils.base64FromString(m_credentials.getUserid() + ":" + m_credentials.getPassword());
            headers.put("Authorization", authString);
        }
        
        // Form the message header.
        StringBuilder buffer = new StringBuilder();
        buffer.append(method.toString());
        buffer.append(" ");
        buffer.append(addTenantParam(uri));
        buffer.append(" HTTP/1.1\r\n");
        for (String name : headers.keySet()) {
            buffer.append(name);
            buffer.append(": ");
            buffer.append(headers.get(name));
            buffer.append("\r\n");
        }
        buffer.append("\r\n");

        m_logger.debug("Sending request to uri '{}'; message length={}",
                       uri, headers.get(HttpDefs.CONTENT_LENGTH));
        return sendAndReceive(buffer.toString(), body);
    }   // sendAndReceive 
    
    // Send the given request and read the corresponding response. If a socket error occurs,
    // we reconnect and retry up to MAX_SOCKET_RETRIES before giving up.
    private RESTResponse sendAndReceive(String header, byte[] body) throws IOException {
        // Fail before trying if socket has been closed.
        if (isClosed()) {
            throw new IOException("Socket has been closed");
        }
        
        Exception lastException = null;
        for (int attempt = 0; attempt < MAX_SOCKET_RETRIES; attempt++) {
            try {
                sendRequest(header, body);
                return readResponse();
            } catch (IOException e) {
                // Attempt to reconnect; if this fails, the server's probably down and we
                // let reconnect's IOException pass through.
                lastException = e;
                m_logger.warn("Socket error occurred -- reconnecting", e);
                reconnect();
            }
        }
        // Here, all reconnects succeeded but retries failed; something else is wrong with
        // the server or the request.
        throw new IOException("Socket error; all retries failed", lastException);
    }   // sendAndReceive
    
    
    // Send the request represented by the given header and optional body.
    private void sendRequest(String header, byte[] body) throws IOException {
        // Send entire message in one write, else suffer the fate of weird TCP/IP stacks.
        byte[] headerBytes = Utils.toBytes(header);
        byte[] requestBytes = headerBytes;
        if (body != null && body.length > 0) {
            requestBytes = new byte[headerBytes.length + body.length];
            System.arraycopy(headerBytes, 0, requestBytes, 0, headerBytes.length);
            System.arraycopy(body, 0, requestBytes, headerBytes.length, body.length);
        }
        
        // Send the header and body (if any) and flush the result.
        m_outStream.write(requestBytes);
        m_outStream.flush();
    }   // sendRequest
    
    // Read the response to a REST message and return the headers and body encapsulated in
    // a RESTResponse object.
    private RESTResponse readResponse() throws IOException {
        // Read response code from the header line.
        HttpCode resultCode = readStatusLine();
        
        // Read and save headers, keeping track of content-length if we find it.
        Map<String, String> headers = new HashMap<String, String>();
        int contentLength = 0;
        String headerLine = readHeader();
        while (headerLine.length() > 2) {
            // Read header and split into parts based on ":" separator. Both parts are
            // trimmed, and the header is up-cased.
            int colonInx = headerLine.indexOf(':');
            String headerName =     // Use the whole line if there's no colon
                colonInx <= 0 ? headerLine.trim().toUpperCase()
                              : headerLine.substring(0, colonInx).trim().toUpperCase();
            String headerValue =    // Use an empty string if there's no colon
                colonInx <= 0 ? "" : headerLine.substring(colonInx + 1).trim();
            headers.put(headerName, headerValue);
            if (headerName.equals(HttpDefs.CONTENT_LENGTH)) {
                try {
                    contentLength = Integer.parseInt(headerValue);
                } catch (NumberFormatException e) {
                    // Turn into IOException
                    throw new IOException("Invalid content-length value: " + headerLine);
                }
            }
            headerLine = readHeader();
        }

        // Final header line should be CRLF
        if (!headerLine.equals("\r\n")) {
            throw new IOException("Header not properly terminated: " + headerLine);
        }
        
        // If we have a response entity, read that now.
        byte[] body = null;
        if (contentLength > 0) {
            body = readBody(contentLength);
            
            // Decompress output entity if needed.
            String contentEncoding = headers.get(HttpDefs.CONTENT_ENCODING);
            if (contentEncoding != null) {
                Utils.require(contentEncoding.equalsIgnoreCase("gzip"),
                              "Unrecognized output Content-Encoding: " + contentEncoding);
                body = Utils.decompressGZIP(body);
            }
        }
        return new RESTResponse(resultCode, body, headers);
    }   // readResponse

    // Read a REST response status line and return the status code from it.
    private HttpCode readStatusLine() throws IOException {
        // Read a line of text, which should be in the format HTTP/<version <code> <reason>
        String statusLine = readHeader();
        String[] parts = statusLine.split(" +");
        if (parts.length < 3) {
            throw new IOException("Badly formed response status line: " + statusLine);
        }
        try {
            // Attempt to convert the return code into an enum.
            int code = Integer.parseInt(parts[1]);
            return HttpCode.findByCode(code);
        } catch (NumberFormatException e) {
            // Turn into a bad response line error.
            throw new IOException("Badly formed response status line: " + statusLine);
        }
    }   // readStatusLine
    
    // Read and return a line of text from the socket. Bytes are lazily converted to chars
    // since we are reading header lines and expect no UTF-8/Unicode issues.
    private String readHeader() throws IOException {
        StringBuffer buffer = new StringBuffer();
        int aChar;
        do {
            aChar = m_inStream.read();
            if (aChar == -1) {
                // Unexpected EOF
                throw new IOException("Unexpected EOF");
            }

            // here, got a character; append it to the line
            buffer.append((char)aChar);
        } while (aChar != '\n');
        
        // Buffer could be empty.
        return buffer.toString();
    }   // readHeader

    // Read the response body, which should be the given number of bytes, and return it as
    // a byte[].
    private byte[] readBody(int contentLength) throws IOException {
        // Read directly into an array but keep reading until we get it all.
        byte[] buffer = new byte[contentLength];
        int bytesLeft = contentLength;
        int offset = 0;
        while (bytesLeft > 0) {
            int bytesGot = m_inStream.read(buffer, offset, bytesLeft);
            bytesLeft -= bytesGot;
            offset += bytesGot;
        }
        return buffer;
    }   // readBody

    // Create a socket connection, setting m_socket, using configured parameters.
    private void createSocket() throws Exception {
        // Some socket options, notably setReceiveBufferSize, must be set before the
        // socket is connected. So, first create the socket, then set options, then connect.
        if (m_sslParams != null) {
            SSLSocketFactory factory = m_sslParams.createSSLContext().getSocketFactory();
            m_socket = factory.createSocket(m_host, m_port);
            setSocketOptions();
            ((SSLSocket)m_socket).startHandshake();
        } else {
            m_socket = new Socket();
            setSocketOptions();
            SocketAddress sockAddr = new InetSocketAddress(m_host, m_port);
            m_socket.connect(sockAddr);
        }
    }   // createSocket
    
    // Customize socket options
    private void setSocketOptions() throws SocketException {
        if (DISABLE_NAGLES) {
            // Disable Nagle's algorithm (significant on Windows).
            m_socket.setTcpNoDelay(true);
            m_logger.debug("Nagle's algorithm disabled.");
        }
        if (USE_CUSTOM_BUFFER_SIZE) {
            // Improve default send/receive buffer sizes from default (often 8K).
            if (m_socket.getSendBufferSize() < NET_BUFFER_SIZE) {
                m_logger.debug("SendBufferSize increased from {} to {}",
                               m_socket.getSendBufferSize(), NET_BUFFER_SIZE);
                m_socket.setSendBufferSize(NET_BUFFER_SIZE);
            }
            if (m_socket.getReceiveBufferSize() < NET_BUFFER_SIZE) {
                m_logger.debug("ReceiveBufferSize increased from {} to {}",
                               m_socket.getReceiveBufferSize(), NET_BUFFER_SIZE);
                m_socket.setReceiveBufferSize(NET_BUFFER_SIZE);
            }
        }
    }   // setSocketOptions

}   // class RESTClient
