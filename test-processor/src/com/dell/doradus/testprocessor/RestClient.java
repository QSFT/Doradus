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

package com.dell.doradus.testprocessor;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

public class RestClient
{
    private static boolean  USE_CUSTOM_BUFFER_SIZE  = true;
    private static int      NET_BUFFER_SIZE         = 65536;
    private static boolean  DISABLE_NAGLES          = true;

    private String          m_host      = null;
    private int             m_port      = -1;
    private Socket          m_socket    = null;
    private InputStream     m_inStream  = null;
    private OutputStream    m_outStream = null;

    public void connect(String host, int port)
    throws Exception
    {
        disconnect();

        m_host = host;
        m_port = port;

        m_socket = new Socket();
        if (m_socket == null) {
            throw new Exception("Failed to create socket");
        }

        try  {
            if (DISABLE_NAGLES) {
                m_socket.setTcpNoDelay(true);
            }
            if (USE_CUSTOM_BUFFER_SIZE) {
                if (m_socket.getSendBufferSize() < NET_BUFFER_SIZE) {
                    m_socket.setSendBufferSize(NET_BUFFER_SIZE);
                }
                if (m_socket.getReceiveBufferSize() < NET_BUFFER_SIZE) {
                    m_socket.setReceiveBufferSize(NET_BUFFER_SIZE);
                }
            }

            SocketAddress addr = new InetSocketAddress(m_host, m_port);
            m_socket.connect(addr);

            m_inStream  = m_socket.getInputStream();
            m_outStream = m_socket.getOutputStream();
        }
        catch(Exception ex) {
            disconnect();
            String msg = "Failed to connect to " + m_host + ":" + m_port;
            throw new Exception(msg, ex);
        }
    }

    public boolean notConnected()
    {
        return m_socket == null
                || m_socket.isClosed()
                || !m_socket.isConnected();
    }

    public void disconnect()
    {
        if (m_socket == null)
            return;

        try { m_socket.close(); }
        catch (Exception ex) { /* ignore */  }

        m_socket = null;
    }

    public void sendRequest(RestRequest request)
    throws Exception
    {
        if (m_socket == null) {
            String msg = "No connection to Doradus server (possibly <connect> statement is missed)";
            throw new Exception(msg);
        }
        try {
            StringBuilder header = new StringBuilder();

            header.append(request.getRest() + " HTTP/1.1" + "\r\n");
            header.append("Host: " + m_host + "\r\n");

            String acceptType = request.getAcceptType();
            if (acceptType != null && !acceptType.isEmpty()) {
                header.append("Accept: " + acceptType + "\r\n");
            }
            String contentType = request.getContentType();
            if (contentType != null && !contentType.isEmpty()) {
                header.append("Content-type: " + contentType + "\r\n");
            }
            
            String body = request.getBody();
            int contentLength = (body == null) ? 0 : body.length();
            
            header.append("Content-length: " + contentLength + "\r\n");
            header.append("x-api-version: 2" + "\r\n");
            header.append("\r\n");

            byte[] headerBytes = header.toString().getBytes("UTF-8");
            byte[] requestBytes = headerBytes;

            if (contentLength > 0)
            {
                byte[] bodyBytes = body.getBytes("UTF-8");

                requestBytes = new byte[headerBytes.length + bodyBytes.length];
                System.arraycopy(headerBytes, 0, requestBytes, 0, headerBytes.length);
                System.arraycopy(bodyBytes, 0, requestBytes, headerBytes.length, bodyBytes.length);
            }

            m_outStream.write(requestBytes);
            m_outStream.flush();
        }
        catch(Exception ex) {
            String msg = "Failed to send request";
            throw new Exception(msg, ex);
        }
    }

    public RestResponse readResponse()
    throws Exception
    {
        try {
            RestResponse response = new RestResponse();

            List<String> headerLines = readHeaderLines();
            response.parseHeader(headerLines);

            readBody(response);

            return response;
        }
        catch(Exception ex) {
            String msg = "Failed to read response";
            throw new Exception(msg, ex);
        }
    }

    private List<String> readHeaderLines()
    throws Exception
    {
        try {
            List<String> lines = new ArrayList<String>();
            while (true)
            {
                String line = readHeaderLine();
                if (line.length() <= 2)
                {
                    if (!line.equals("\r\n")) {
                        String msg = "Response header is not properly terminated: \"" + line + "\"";
                        throw new Exception(msg);
                    }
                    break;
                }
                lines.add(line.trim());
            }
            return lines;
        }
        catch(Exception ex) {
            String msg = "Failed to read response header";
            throw new Exception(msg, ex);
        }
    }

    private String readHeaderLine()
    throws Exception
    {
        try {
            StringBuilder line = new StringBuilder();
            int buf = ' ';
            while (buf != '\n')
            {
                buf = m_inStream.read();
                if (buf == -1) {
                    String msg = "Unexpected EOF";
                    throw new Exception(msg);
                }
                line.append((char) buf);
            }

            return line.toString();
        }
        catch(Exception ex) {
            String msg = "Failed to read response header line";
            throw new Exception(msg, ex);
        }
    }

    private void readBody(RestResponse response)
    throws Exception
    {
        int bytesLeft = response.getContentLength();

        if (bytesLeft <= 0) {
            response.setBody("");
            return;
        }

        byte[] buffer = new byte[bytesLeft];
        int offset = 0;

        while (bytesLeft > 0) {
            int bytesGot = m_inStream.read(buffer, offset, bytesLeft);
            bytesLeft -= bytesGot;
            offset += bytesGot;
        }

        String body = (new String(buffer, "UTF-8")).trim();
        /*
        if ("xml".equals(response.contentType()) && body.indexOf("<?xml") > -1) {
            int pos = body.indexOf("?>");
            if (pos > -1) {
                body = body.substring(pos + 2, body.length() - (pos + 2)).trim();
            }
        }
        */
        response.setBody(body);
    }
}
