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


import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.solr.common.util.SuppressForbidden;

/**
 * Get a Connection with with a url and properties.
 *
 * jdbc:solr://zkhost:port?collection=collection&amp;aggregationMode=map_reduce
 **/

public class DriverImpl implements Driver {

  static {
    try {
      DriverManager.registerDriver(new DriverImpl());
    } catch (SQLException e) {
      throw new RuntimeException("Can't register driver!", e);
    }
  }

  public Connection connect(String url, Properties props) throws SQLException {
    if(!acceptsURL(url)) {
      return null;
    }

    URI uri = processUrl(url);

    loadParams(uri, props);

    if (!props.containsKey("collection")) {
      throw new SQLException("The connection url has no connection properties. At a mininum the collection must be specified.");
    }
    String collection = (String) props.remove("collection");

    if (!props.containsKey("aggregationMode")) {
      props.setProperty("aggregationMode", "facet");
    }

    String zkHost = uri.getAuthority() + uri.getPath();

    return new ConnectionImpl(zkHost, collection, props);
  }

  public int getMajorVersion() {
    // TODO get from POM?
    return 6;
  }

  public int getMinorVersion() {
    // TODO get from POM?
    return 0;
  }

  public boolean acceptsURL(String url) {
    return url != null && url.startsWith("jdbc:solr");
  }

  public boolean jdbcCompliant() {
    return false;
  }

  @SuppressForbidden(reason = "Required for JDBC java.sql.Driver interface")
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException();
  }

  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
    return null;
  }

  protected URI processUrl(String url) throws SQLException {
    URI uri;
    try {
      uri = new URI(url.replaceFirst("jdbc:", ""));
    } catch (URISyntaxException e) {
      throw new SQLException(e);
    }

    if (uri.getAuthority() == null) {
      throw new SQLException("The zkHost must not be null");
    }

    return uri;
  }

  private void loadParams(URI uri, Properties props) throws SQLException {
    List<NameValuePair> parsedParams = URLEncodedUtils.parse(uri, "UTF-8");
    for (NameValuePair pair : parsedParams) {
      if (pair.getValue() != null) {
        props.put(pair.getName(), pair.getValue());
      } else {
        props.put(pair.getName(), "");
      }
    }
  }
}