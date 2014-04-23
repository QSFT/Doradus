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

package com.dell.doradus.olap.io;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BEFORE USING, EXECUTE THE FOLLOWING STATEMENT IN SQL:
 * use DoradusDB;
 *
 * DROP TABLE OLAP;
 * CREATE TABLE OLAP (keyid varchar(256), colid varchar(128), data varbinary(max), PRIMARY KEY CLUSTERED(keyid, colid));
 *  
 * @author otarakan
 *
 */

public class MSSqlIO implements IO {
    private static Logger LOG = LoggerFactory.getLogger(MSSqlIO.class);
    private Connection m_connection;
    
	
	public MSSqlIO() {
		try {
			String url = "jdbc:sqlserver://localhost\\SQLEXPRESS;databaseName=DoradusDB";
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			m_connection = DriverManager.getConnection(url, "sa", "admin");
		}catch(Exception e) {
			LOG.error("Error initializing SQL", e);
			throw new RuntimeException("Error initializing SQL", e);
		}
	}
	
	@Override public byte[] getValue(String app, String key, String column) {
		try {
			PreparedStatement stmt = m_connection.prepareStatement("SELECT data FROM OLAP WHERE keyid=? AND colid=?");
			String keyid = app + "/" + key;
			String colid = column;
			stmt.setString(1, keyid);
			stmt.setString(2, colid);
			ResultSet rs = stmt.executeQuery();
			byte[] data = null;
			if(rs.next()) {
				data = rs.getBytes(1);
			}
			rs.close();
			stmt.close();
			return data;
		}catch(SQLException e) {
			LOG.error("Sql", e);
			throw new RuntimeException("sql", e);
		}
	}
	
	@Override public List<ColumnValue> get(String app, String key, String prefix) {
		try {
			PreparedStatement stmt = m_connection.prepareStatement("SELECT colid, data FROM OLAP WHERE keyid=? and colid >= ?");
			List<ColumnValue> result = new ArrayList<ColumnValue>();
			String keyid = app + "/" + key;
			stmt.setString(1, keyid);
			stmt.setString(2, prefix);
			ResultSet rs = stmt.executeQuery();
			while(rs.next()) {
				String colid = rs.getString(1);
				if(!colid.startsWith(prefix)) break;
				byte[] data = rs.getBytes(2);
				result.add(new ColumnValue(colid.substring(prefix.length()), data));
			}
			rs.close();
			stmt.close();
			return result;
		}catch(SQLException e) {
			LOG.error("Sql", e);
			throw new RuntimeException("sql", e);
		}
	}
	
	@Override public void createCF(String name) {
	}

	@Override public void deleteCF(String name) {
		try {
			PreparedStatement stmt = m_connection.prepareStatement("DELETE FROM OLAP WHERE keyid LIKE ?");
			stmt.setString(1, name + "%");
			stmt.execute();
			stmt.close();
		}catch(SQLException e) {
			LOG.error("Sql", e);
			throw new RuntimeException("sql", e);
		}
	}
	
	@Override public void write(String app, String key, List<ColumnValue> values) {
		try {
			PreparedStatement stmt = m_connection.prepareStatement(
"if exists (SELECT 1 FROM OLAP WHERE keyid=? and colid=?) UPDATE OLAP SET data=? WHERE keyid=? and colid=? ELSE INSERT INTO OLAP VALUES(?,?,?)");
			String keyid = app + "/" + key;
			for(ColumnValue v: values) {
				stmt.setString(1, keyid);
				stmt.setString(2, v.columnName);
				stmt.setBytes(3, v.columnValue);
				stmt.setString(4, keyid);
				stmt.setString(5, v.columnName);
				stmt.setString(6, keyid);
				stmt.setString(7, v.columnName);
				stmt.setBytes(8, v.columnValue);
				stmt.executeUpdate();
			}
			stmt.close();
		}catch(SQLException e) {
			LOG.error("Sql", e);
			throw new RuntimeException("sql", e);
		}
	}
	
	@Override public void delete(String columnFamily, String sKey, String columnName) {
		String keyid = columnFamily + "/" + sKey;
		if(columnName == null) {
			try {
				PreparedStatement stmt = m_connection.prepareStatement("DELETE FROM OLAP WHERE keyid=?");
				stmt.setString(1, keyid);
				stmt.execute();
				stmt.close();
			}catch(SQLException e) {
				LOG.error("Sql", e);
				throw new RuntimeException("sql", e);
			}
		} else {
			try {
				PreparedStatement stmt = m_connection.prepareStatement("DELETE FROM OLAP WHERE keyid=? AND colid=?");
				stmt.setString(1, keyid);
				stmt.setString(2, columnName);
				stmt.execute();
				stmt.close();
			}catch(SQLException e) {
				LOG.error("Sql", e);
				throw new RuntimeException("sql", e);
			}
		}
	}

}
