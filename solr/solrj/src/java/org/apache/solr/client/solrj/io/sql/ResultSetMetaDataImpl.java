package org.apache.solr.client.solrj.io.sql;

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

import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;

public class ResultSetMetaDataImpl implements ResultSetMetaData {
  private final Map<String, String> functionTypes = new HashMap<>();
  private final ResultSetImpl resultSet;

  ResultSetMetaDataImpl(ResultSetImpl resultSet) {
    this.resultSet = resultSet;
    addFunctionTypes();
  }

  private void addFunctionTypes() {
    functionTypes.put("count", "double");
    functionTypes.put("sum", "double");
    functionTypes.put("min", "double");
    functionTypes.put("max", "double");
    functionTypes.put("avg", "double");
  }

  @Override
  public int getColumnCount() throws SQLException {
    return this.resultSet.getFields().size();
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isSearchable(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isCurrency(int column) throws SQLException {
    return false;
  }

  @Override
  public int isNullable(int column) throws SQLException {
    return columnNullableUnknown;
  }

  @Override
  public boolean isSigned(int column) throws SQLException {
    return false;
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    return getColumnLabel(column).length();
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {
    return getColumnName(column);
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    if(column > 0 && column <= this.resultSet.getFields().size()) {
      return this.resultSet.getFields().get(column - 1);
    } else {
      throw new SQLException("Invalid column:" + column);
    }
  }

  @Override
  public String getSchemaName(int column) throws SQLException {
    return null;
  }

  @Override
  public int getPrecision(int column) throws SQLException {
    return 0;
  }

  @Override
  public int getScale(int column) throws SQLException {
    return 0;
  }

  @Override
  public String getTableName(int column) throws SQLException {
    // TODO pass info from result/connection?
    return "";
  }

  @Override
  public String getCatalogName(int column) throws SQLException {
    return null;
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    String type = getColumnTypeName(column);
    switch(type) {
      case "string":
        return Types.VARCHAR;
      case "int":
        return Types.INTEGER;
      case "float":
        return Types.FLOAT;
      case "double":
        return Types.DOUBLE;
      default:
        return Types.OTHER;
    }
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {
    String columnName = this.resultSet.lookupColumnLabel(column);

    String columnType = getFunctionType(columnName);
    if(columnType.isEmpty()) {
      CloudSolrClient solrClient = this.resultSet.getStatementImpl().getConnectionImpl().getClient();
      String collection = this.resultSet.getStatementImpl().getConnectionImpl().getCollection();
      String path = "/schema/fields/" + columnName;
      ModifiableSolrParams solrParams = new ModifiableSolrParams();
      solrParams.add("includeDynamic", "true");
      GenericSolrRequest request = new GenericSolrRequest(SolrRequest.METHOD.GET, path, solrParams);
      try {
        NamedList<Object> namedList = solrClient.request(request, collection);
        columnType = String.valueOf(((SimpleOrderedMap) namedList.get("field")).get("type"));
      } catch (SolrServerException | IOException ignore) {
        // Does it matter if we can't get the mapping?
      }
    }
    return columnType;
  }

  private String getFunctionType(String columnName) {
    for(String function : functionTypes.keySet()) {
      if(columnName.startsWith(function + "(") && columnName.endsWith(")")) {
        return functionTypes.get(function);
      }
    }
    return "";
  }

  @Override
  public boolean isReadOnly(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isWritable(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    return false;
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {
    return null;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }
}
