/*
 *
 *  *  Copyright 2010-2017 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.http;

import com.orientechnologies.orient.connection.ONeo4jConnectionManager;
import com.orientechnologies.orient.connection.OSourceNeo4jInfo;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by gabriele on 27/02/17.
 */
public class ONeo4jImporterHandler {

  private ExecutorService pool = Executors.newFixedThreadPool(1);
  private ONeo4jImporterJob currentJob = null;

  public ONeo4jImporterHandler() {}

  /**
   * Executes import with configuration;
   *
   * @param cfg
   */
  public void executeImport(ODocument cfg, OServer server) {

    ONeo4jImporterJob job = new ONeo4jImporterJob(cfg, server, new ONeo4ImporterListener() {
      @Override
      public void onEnd(ONeo4jImporterJob oTeleporterJob) {
        currentJob = null;
      }
    });

    job.validate();

    currentJob = job;
    pool.execute(job);

  }

  /**
   * Checks If the connection with given parameters is alive
   *
   * @param args
   *
   * @throws Exception
   */
  public void checkConnection(ODocument args) throws Exception {

    final String neo4jUrl = args.field("neo4jUrl");
    final String neo4jUsername = args.field("neo4jUsername");
    final String neo4jPassword = args.field("neo4jPassword");
    OSourceNeo4jInfo sourceNeo4jInfo = new OSourceNeo4jInfo(neo4jUrl, neo4jUsername, neo4jPassword);
    ONeo4jConnectionManager connectionManager = new ONeo4jConnectionManager(sourceNeo4jInfo);
    connectionManager.checkConnection();
  }


  /**
   * Status of the Running Jobs
   *
   * @return ODocument
   */
  public ODocument status() {

    ODocument status = new ODocument();

    Collection<ODocument> jobs = new ArrayList<ODocument>();
    if (currentJob != null) {
      jobs.add(currentJob.status());
    }
    status.field("jobs", jobs);
    return status;
  }
}
