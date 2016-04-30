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
package org.apache.solr.handler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Locale;
import java.util.Properties;

import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressSSL
public class TestAvaticaHandler extends SolrCloudTestCase {
  private static final String COLLECTION = TestAvaticaHandler.class.getSimpleName().toLowerCase(Locale.ROOT);

  @BeforeClass
  public static void setupCluster() throws Exception {
    int numShards = random().nextInt(2) + 1;
    int numReplicas = random().nextInt(2) + 1;
    int maxShardsPerNode = random().nextInt(2) + 1;
    int nodeCount = (numShards*numReplicas + (maxShardsPerNode-1))/maxShardsPerNode;

    // create and configure cluster
    configureCluster(nodeCount)
        .addConfig("sql", configset("_default"))
        .configure();

    // create an empty collection
    CollectionAdminRequest.createCollection(COLLECTION, "sql", numShards, numReplicas)
        .setMaxShardsPerNode(maxShardsPerNode)
        .process(cluster.getSolrClient());

    new UpdateRequest()
        .add(sdoc("id", "1", "text", "XXXX XXXX", "str_s", "a", "field_i", "7"))
        .add(sdoc("id", "2", "text", "XXXX XXXX", "str_s", "b", "field_i", "8"))
        .add(sdoc("id", "3", "text", "XXXX XXXX", "str_s", "a", "field_i", "20"))
        .add(sdoc("id", "4", "text", "XXXX XXXX", "str_s", "b", "field_i", "11"))
        .add(sdoc("id", "5", "text", "XXXX XXXX", "str_s", "c", "field_i", "30"))
        .add(sdoc("id", "6", "text", "XXXX XXXX", "str_s", "c", "field_i", "40"))
        .add(sdoc("id", "7", "text", "XXXX XXXX", "str_s", "c", "field_i", "50"))
        .add(sdoc("id", "8", "text", "XXXX XXXX", "str_s", "c", "field_i", "60"))
        .commit(cluster.getSolrClient(), COLLECTION);
  }

  @Test
  public void doTest() throws Exception {
    String url = "jdbc:avatica:remote:";
    String sql = "select id, str_s, field_i as myField from " + COLLECTION + " order by id desc limit 2";

    Properties properties = new Properties();
    properties.setProperty("url", cluster.getRandomJetty(random()).getBaseUrl() + "/" + COLLECTION + "/avatica");

    try (Connection connection = DriverManager.getConnection(url, properties)) {
      try (Statement statement = connection.createStatement()) {
        try (ResultSet resultSet = statement.executeQuery(sql)) {
          ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

          assertNotNull(resultSetMetaData);
          assertEquals(3, resultSetMetaData.getColumnCount());

          assertEquals("id", resultSetMetaData.getColumnName(1));
          assertEquals("id", resultSetMetaData.getColumnLabel(1));
          assertEquals("str_s", resultSetMetaData.getColumnName(2));
          assertEquals("str_s", resultSetMetaData.getColumnLabel(2));
          assertEquals("field_i", resultSetMetaData.getColumnName(3));
          assertEquals("myField", resultSetMetaData.getColumnLabel(3));

          assertTrue(resultSet.next());

          assertEquals("8", resultSet.getString(1));
          assertEquals("c", resultSet.getString(2));
          assertEquals(60, resultSet.getInt(3));

          assertTrue(resultSet.next());

          assertEquals("7", resultSet.getString(1));
          assertEquals("c", resultSet.getString(2));
          assertEquals(50, resultSet.getInt(3));

          assertFalse(resultSet.next());
        }
      }
    }
  }
}
