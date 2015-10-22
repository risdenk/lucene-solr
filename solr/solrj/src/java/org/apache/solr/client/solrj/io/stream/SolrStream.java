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

package org.apache.solr.client.solrj.io.stream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.List;

import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
/**
*  Queries a single Solr instance and maps SolrDocs to a Stream of Tuples.
**/

public class SolrStream extends TupleStream {

  private static final long serialVersionUID = 1;

  private String baseUrl;
  private Map<String, Object> params;
  private int numWorkers;
  private int workerID;
  private boolean trace;
  private Map<String, String> fieldMappings;
  private transient JSONTupleStream jsonTupleStream;
  private transient HttpSolrClient client;
  private transient SolrClientCache cache;
  private StreamContext context;

  public SolrStream(String baseUrl, Map<String, Object> params) {
    this.baseUrl = baseUrl;
    this.params = params;
    this.context = new StreamContext();
  }

  public void setFieldMappings(Map<String, String> fieldMappings) {
    this.fieldMappings = fieldMappings;
  }

  public List<TupleStream> children() {
    return new ArrayList<>();
  }

  public String getBaseUrl() {
    return baseUrl;
  }

  public void setStreamContext(StreamContext context) {
    this.context = context;
    this.numWorkers = context.numWorkers;
    this.workerID = context.workerID;
    this.cache = context.getSolrClientCache();
  }

  @Override
  public StreamContext getStreamContext() {
    return this.context;
  }

  /**
  * Opens the stream to a single Solr instance.
  **/

  public void open() throws IOException {

    if(cache == null) {
      client = new HttpSolrClient(baseUrl);
    } else {
      client = cache.getHttpSolrClient(baseUrl);
    }

    try {
      jsonTupleStream = JSONTupleStream.create(client, loadParams(params));

      // Fill StreamContext entries with entries from JSON TupleStream
      this.context.getEntries().putAll(jsonTupleStream.getStreamContext().getEntries());
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   *  Setting trace to true will include the "_CORE_" field in each Tuple emitted by the stream.
   **/

  public void setTrace(boolean trace) {
    this.trace = trace;
  }

  private SolrParams loadParams(Map<String, Object> params) throws IOException {
    ModifiableSolrParams solrParams = new ModifiableSolrParams();
    if(params.containsKey("partitionKeys")) {
      if(!params.get("partitionKeys").equals("none")) {
        String partitionFilter = getPartitionFilter();
        solrParams.add("fq", partitionFilter);
      }
    } else {
      if(numWorkers > 1) {
        throw new IOException("When numWorkers > 1 partitionKeys must be set. Set partitionKeys=none to send the entire stream to each worker.");
      }
    }

    for (Map.Entry<String, Object> entry : params.entrySet()) {
      solrParams.add(entry.getKey(), String.valueOf(entry.getValue()));
    }

    return solrParams;
  }

  private String getPartitionFilter() {
    return "{!hash workers=" + this.numWorkers + " worker=" + this.workerID + "}";
  }

  /**
  *  Closes the Stream to a single Solr Instance
  * */

  public void close() throws IOException {

    if(jsonTupleStream != null) {
      jsonTupleStream.close();
    }

    if(cache == null) {
      client.close();
    }
  }

  /**
  * Reads a Tuple from the stream. The Stream is completed when Tuple.EOF == true.
  **/

  public Tuple read() throws IOException {
    try {
      Map<Object, Object> fields = jsonTupleStream.next();

      if (fields == null) {
        //Return the EOF tuple.
        Map<Object, Object> m = new HashMap<>();
        m.put("EOF", true);
        return new Tuple(m);
      } else {

        String msg = (String) fields.get("EXCEPTION");
        if (msg != null) {
          throw new HandledException(msg);
        }

        if (trace) {
          fields.put("_CORE_", this.baseUrl);
        }

        if (fieldMappings != null) {
          fields = mapFields(fields, fieldMappings);
        }
        return new Tuple(fields);
      }
    } catch (HandledException e) {
      throw new IOException("--> "+this.baseUrl+":"+e.getMessage());
    } catch (Exception e) {
      //The Stream source did not provide an exception in a format that the SolrStream could propagate.
      throw new IOException("--> "+this.baseUrl+": An exception has occurred on the server, refer to server log for details.");
    }
  }

  public static class HandledException extends IOException {
    public HandledException(String msg) {
      super(msg);
    }
  }
  
  /** There is no known sort applied to a SolrStream */
  public StreamComparator getStreamSort(){
    return null;
  }

  private Map<Object, Object> mapFields(Map<Object, Object> fields, Map<String,String> mappings) {

    for (Map.Entry<String, String> entry : mappings.entrySet()) {
      String mapFrom = entry.getKey();
      String mapTo = entry.getValue();
      Object o = fields.get(mapFrom);
      fields.remove(mapFrom);
      fields.put(mapTo, o);
    }

    return fields;
  }
}