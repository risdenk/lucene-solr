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

import java.lang.invoke.MethodHandles;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.metrics.noop.NoopMetricsSystem;
import org.apache.calcite.avatica.remote.Handler;
import org.apache.calcite.avatica.remote.JsonHandler;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.config.Lex;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.sql.CalciteSolrDriver;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.RawResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvaticaHandler extends RequestHandlerBase implements SolrCoreAware, PermissionNameProvider {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static JsonHandler jsonHandler;

  public void inform(SolrCore core) {
    CoreContainer coreContainer = core.getCoreContainer();
    if(coreContainer.isZooKeeperAware()) {
      try {
        Class.forName(CalciteSolrDriver.class.getCanonicalName());
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }

      String zkHost = coreContainer.getZkController().getZkServerAddress();
      try {
        setupJsonHandler(zkHost);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private synchronized void setupJsonHandler(String zkHost) throws SQLException {
    if(jsonHandler == null) {
      Properties properties = new Properties();
      properties.setProperty("lex", Lex.MYSQL.toString());
      properties.setProperty("zk", zkHost);

      JdbcMeta jdbcMeta = new JdbcMeta(CalciteSolrDriver.CONNECT_STRING_PREFIX, properties);
      jsonHandler = new JsonHandler(new LocalService(jdbcMeta), NoopMetricsSystem.getInstance());
    }
  }

  @Override
  public PermissionNameProvider.Name getPermissionName(AuthorizationContext request) {
    return PermissionNameProvider.Name.READ_PERM;
  }

  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    String jsonRequest = AvaticaUtils.readFully(req.getContentStreams().iterator().next().getStream());
    Handler.HandlerResponse<String> jsonResponse = jsonHandler.apply(jsonRequest);
    rsp.add(RawResponseWriter.CONTENT, new ContentStreamBase.StringStream(jsonResponse.getResponse()));
  }

  public String getDescription() {
    return this.getClass().getName();
  }

  public String getSource() {
    return null;
  }
}
