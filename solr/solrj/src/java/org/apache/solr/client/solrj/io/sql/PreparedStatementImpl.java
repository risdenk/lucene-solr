/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.client.solrj.io.sql;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class PreparedStatementImpl extends StatementImpl implements PreparedStatement {
  private static final String PARAMETER_REGEX = "\\?";
  private static final Pattern p = Pattern.compile(PARAMETER_REGEX);
  private final String sql;
  private final ArrayList<Object> parameters;
  final int numParameters;

  PreparedStatementImpl(ConnectionImpl connection, String sql) {
    super(connection);
    this.sql = sql;
    this.numParameters = getNumberOfParameters(sql);
    this.parameters = new ArrayList<>(this.numParameters);
  }

  private int getNumberOfParameters(String sql) {
    Matcher m = p.matcher(sql);
    int count = 0;
    while (m.find()){
      count +=1;
    }
    return count;
  }

  private void addParameter(int parameterIndex, Object x) throws SQLException {
    checkClosed();

    this.parameters.add(parameterIndex - 1, x);
  }

  private String replaceParameters(String sql, List<Object> parameters) {
    for (Object obj : parameters) {
      sql = sql.replaceFirst(PARAMETER_REGEX, String.valueOf(obj));
    }
    return sql;
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    return super.executeQuery(this.replaceParameters(this.sql, this.parameters));
  }

  @Override
  public int executeUpdate() throws SQLException {
    return super.executeUpdate(this.replaceParameters(this.sql, this.parameters));
  }

  @Override
  public boolean execute() throws SQLException {
    return super.execute(this.replaceParameters(this.sql, this.parameters));
  }

  @Override
  public void clearParameters() throws SQLException {
    checkClosed();

    this.parameters.clear();
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void addBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    addParameter(parameterIndex, null);
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    addParameter(parameterIndex, x);
  }

  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException {
    addParameter(parameterIndex, x);
  }

  @Override
  public void setShort(int parameterIndex, short x) throws SQLException {
    addParameter(parameterIndex, x);
  }

  @Override
  public void setInt(int parameterIndex, int x) throws SQLException {
    addParameter(parameterIndex, x);
  }

  @Override
  public void setLong(int parameterIndex, long x) throws SQLException {
    addParameter(parameterIndex, x);
  }

  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException {
    addParameter(parameterIndex, x);
  }

  @Override
  public void setDouble(int parameterIndex, double x) throws SQLException {
    addParameter(parameterIndex, x);
  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    addParameter(parameterIndex, x);
  }

  @Override
  public void setString(int parameterIndex, String x) throws SQLException {
    addParameter(parameterIndex, x);
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    addParameter(parameterIndex, x);
  }

  @Override
  public void setDate(int parameterIndex, Date x) throws SQLException {
    addParameter(parameterIndex, x);
  }

  @Override
  public void setTime(int parameterIndex, Time x) throws SQLException {
    addParameter(parameterIndex, x);
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    addParameter(parameterIndex, x);
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException {
    addParameter(parameterIndex, x);
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setRef(int parameterIndex, Ref x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setBlob(int parameterIndex, Blob x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setClob(int parameterIndex, Clob x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setArray(int parameterIndex, Array x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setURL(int parameterIndex, URL x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setNString(int parameterIndex, String value) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setNClob(int parameterIndex, NClob value) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setClob(int parameterIndex, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  // Methods below cannot be called from a PreparedStatement based on JDBC spec

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    throw new SQLException("Cannot be called from PreparedStatement");
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    throw new SQLException("Cannot be called from PreparedStatement");
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    throw new SQLException("Cannot be called from PreparedStatement");
  }

  @Override
  public void addBatch( String sql ) throws SQLException {
    throw new SQLException("Cannot be called from PreparedStatement");
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLException("Cannot be called from PreparedStatement");
  }

  @Override
  public int executeUpdate(String sql, int columnIndexes[]) throws SQLException {
    throw new SQLException("Cannot be called from PreparedStatement");
  }

  @Override
  public int executeUpdate(String sql, String columnNames[]) throws SQLException {
    throw new SQLException("Cannot be called from PreparedStatement");
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLException("Cannot be called from PreparedStatement");
  }

  @Override
  public boolean execute(String sql, int columnIndexes[]) throws SQLException {
    throw new SQLException("Cannot be called from PreparedStatement");
  }

  @Override
  public boolean execute(String sql, String columnNames[]) throws SQLException {
    throw new SQLException("Cannot be called from PreparedStatement");
  }

  @Override
  public long executeLargeUpdate(String sql) throws SQLException {
    throw new SQLException("Cannot be called from PreparedStatement");
  }

  @Override
  public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLException("Cannot be called from PreparedStatement");
  }

  @Override
  public long executeLargeUpdate(String sql, int columnIndexes[]) throws SQLException {
    throw new SQLException("Cannot be called from PreparedStatement");
  }

  @Override
  public long executeLargeUpdate(String sql, String columnNames[]) throws SQLException {
    throw new SQLException("Cannot be called from PreparedStatement");
  }
}