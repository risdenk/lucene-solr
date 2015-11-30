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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.impl.CloudSolrClient;

public class DatabaseMetaDataImpl implements DatabaseMetaData {
  private final ConnectionImpl connection;

  public DatabaseMetaDataImpl(ConnectionImpl connection) {
    this.connection = connection;
  }

  @Override
  public boolean allProceduresAreCallable() throws SQLException {
    return false;
  }

  @Override
  public boolean allTablesAreSelectable() throws SQLException {
    return false;
  }

  @Override
  public String getURL() throws SQLException {
    return "jdbc:solr://" + this.connection.getClient().getZkHost() + "?collection=" + this.connection.getCollection();
  }

  @Override
  public String getUserName() throws SQLException {
    // TODO can this be used? with basic auth???
    return null;
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return true;
  }

  @Override
  public boolean nullsAreSortedHigh() throws SQLException {
    return false;
  }

  @Override
  public boolean nullsAreSortedLow() throws SQLException {
    return false;
  }

  @Override
  public boolean nullsAreSortedAtStart() throws SQLException {
    return false;
  }

  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException {
    return false;
  }

  @Override
  public String getDatabaseProductName() throws SQLException {
    // TODO Get name from POM?
    return "Apache Solr";
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    // TODO Replace with POM variable or from Solr Server?
    return "6.0.0-SNAPSHOT";
  }

  @Override
  public String getDriverName() throws SQLException {
    // TODO Get name from POM?
    return "Apache Solr JDBC";
  }

  @Override
  public String getDriverVersion() throws SQLException {
    // TODO Replace with POM variable?
    return "6.0.0-SNAPSHOT";
  }

  @Override
  public int getDriverMajorVersion() {
    // TODO Replace with POM variable?
    return 6;
  }

  @Override
  public int getDriverMinorVersion() {
    // TODO Replace with POM variable?
    return 0;
  }

  @Override
  public boolean usesLocalFiles() throws SQLException {
    return false;
  }

  @Override
  public boolean usesLocalFilePerTable() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public String getIdentifierQuoteString() throws SQLException {
    return null;
  }

  @Override
  public String getSQLKeywords() throws SQLException {
    // TODO determine if there are special keywords
    return null;
  }

  @Override
  public String getNumericFunctions() throws SQLException {
    // TODO determine if there are special numeric functions
    return null;
  }

  @Override
  public String getStringFunctions() throws SQLException {
    // TODO determine if there are special string functions
    return null;
  }

  @Override
  public String getSystemFunctions() throws SQLException {
    // TODO determine if there are special system functions
    return null;
  }

  @Override
  public String getTimeDateFunctions() throws SQLException {
    // TODO determine if there are special time/date functions
    return null;
  }

  @Override
  public String getSearchStringEscape() throws SQLException {
    return null;
  }

  @Override
  public String getExtraNameCharacters() throws SQLException {
    return null;
  }

  @Override
  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsColumnAliasing() throws SQLException {
    return false;
  }

  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsConvert() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsConvert(int fromType, int toType) throws SQLException {
    return false;
  }

  @Override
  public boolean supportsTableCorrelationNames() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsOrderByUnrelated() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsGroupBy() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsGroupByUnrelated() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsLikeEscapeClause() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMultipleResultSets() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMultipleTransactions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsNonNullableColumns() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCoreSQLGrammar() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsExtendedSQLGrammar() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsANSI92FullSQL() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsOuterJoins() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsFullOuterJoins() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException {
    return false;
  }

  @Override
  public String getSchemaTerm() throws SQLException {
    return null;
  }

  @Override
  public String getProcedureTerm() throws SQLException {
    return null;
  }

  @Override
  public String getCatalogTerm() throws SQLException {
    return null;
  }

  @Override
  public boolean isCatalogAtStart() throws SQLException {
    return false;
  }

  @Override
  public String getCatalogSeparator() throws SQLException {
    return null;
  }

  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsPositionedDelete() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsPositionedUpdate() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSelectForUpdate() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsStoredProcedures() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInComparisons() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInExists() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInIns() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsUnion() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsUnionAll() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    return false;
  }

  @Override
  public int getMaxBinaryLiteralLength() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxCharLiteralLength() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxColumnNameLength() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxColumnsInGroupBy() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxColumnsInIndex() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxColumnsInOrderBy() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxColumnsInSelect() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxColumnsInTable() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxConnections() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxCursorNameLength() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxIndexLength() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxSchemaNameLength() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxProcedureNameLength() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxCatalogNameLength() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxRowSize() throws SQLException {
    return 0;
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    return false;
  }

  @Override
  public int getMaxStatementLength() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxStatements() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxTableNameLength() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxTablesInSelect() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxUserNameLength() throws SQLException {
    return 0;
  }

  @Override
  public int getDefaultTransactionIsolation() throws SQLException {
    return 0;
  }

  @Override
  public boolean supportsTransactions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
    return false;
  }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    return false;
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    return false;
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    return false;
  }

  @Override
  public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
    return null;
  }

  @Override
  public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
    return null;
  }

  @Override
  public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
    return null;
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    Set<String> collections = this.connection.getClient().getZkStateReader().getClusterState().getCollections();
    LinkedList<LinkedList<Object>> rows = new LinkedList<>();
    for(String collection : collections) {
      LinkedList<Object> row = new LinkedList<>();
      row.add(collection);
      row.add("default");
      rows.add(row);
    }
    Map<String, Integer> schema = new HashMap<>();
    schema.put("TABLE_SCHEM", 0);
    schema.put("TABLE_CATALOG", 1);
    return new TestResultSetImpl(schema, rows);
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    LinkedList<LinkedList<Object>> rows = new LinkedList<>();
    LinkedList<Object> row = new LinkedList<>();
    row.add("default");
    rows.add(row);
    Map<String, Integer> schema = new HashMap<>();
    schema.put("TABLE_CAT", 0);
    return new TestResultSetImpl(schema, rows);
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    return null;
  }

  @Override
  public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
    return null;
  }

  @Override
  public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
    return null;
  }

  @Override
  public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
    return null;
  }

  @Override
  public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
    return null;
  }

  @Override
  public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
    return null;
  }

  @Override
  public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
    return null;
  }

  @Override
  public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
    return null;
  }

  @Override
  public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
    return null;
  }

  @Override
  public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
    return null;
  }

  @Override
  public ResultSet getTypeInfo() throws SQLException {
    LinkedList<LinkedList<Object>> rows = new LinkedList<>();
    LinkedList<Object> row = new LinkedList<>();
    row.add("string");
    row.add("string");
    rows.add(row);
    Map<String, Integer> schema = new HashMap<>();
    schema.put("TYPE_NAME", 0);
    schema.put("DATA_TYPE", 1);
    TestResultSetImpl test = new TestResultSetImpl(schema, rows);
    return test;
  }

  @Override
  public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
    return null;
  }

  @Override
  public boolean supportsResultSetType(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
    return false;
  }

  @Override
  public boolean ownUpdatesAreVisible(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean ownDeletesAreVisible(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean ownInsertsAreVisible(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean othersUpdatesAreVisible(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean othersDeletesAreVisible(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean othersInsertsAreVisible(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean updatesAreDetected(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean deletesAreDetected(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean insertsAreDetected(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean supportsBatchUpdates() throws SQLException {
    return false;
  }

  @Override
  public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
    return null;
  }

  @Override
  public Connection getConnection() throws SQLException {
    return this.connection;
  }

  @Override
  public boolean supportsSavepoints() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsNamedParameters() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMultipleOpenResults() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsGetGeneratedKeys() throws SQLException {
    return false;
  }

  @Override
  public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
    return null;
  }

  @Override
  public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
    return null;
  }

  @Override
  public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
    return null;
  }

  @Override
  public boolean supportsResultSetHoldability(int holdability) throws SQLException {
    return false;
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    return 0;
  }

  @Override
  public int getDatabaseMajorVersion() throws SQLException {
    // TODO Get version from Solr server?
    return 6;
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException {
    // TODO Get version from Solr server?
    return 0;
  }

  @Override
  public int getJDBCMajorVersion() throws SQLException {
    // TODO Get version from POM?
    return 6;
  }

  @Override
  public int getJDBCMinorVersion() throws SQLException {
    // TODO Get version from POM?
    return 0;
  }

  @Override
  public int getSQLStateType() throws SQLException {
    return 0;
  }

  @Override
  public boolean locatorsUpdateCopy() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsStatementPooling() throws SQLException {
    return false;
  }

  @Override
  public RowIdLifetime getRowIdLifetime() throws SQLException {
    return null;
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    return null;
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    return false;
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    return false;
  }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException {
    return null;
  }

  @Override
  public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
    return null;
  }

  @Override
  public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
    return null;
  }

  @Override
  public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
    return null;
  }

  @Override
  public boolean generatedKeyAlwaysReturned() throws SQLException {
    return false;
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
