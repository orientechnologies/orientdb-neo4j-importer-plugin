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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.context.ONeo4jImporterMessageHandler;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.neo4jimporter.*;
import com.orientechnologies.orient.output.OPluginMessageHandler;
import com.orientechnologies.orient.server.OServer;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

/**
 * Created by gabriele on 27/02/17.
 */
public class ONeo4jImporterJob  implements Runnable {

  private final ODocument cfg;
  private       ONeo4ImporterListener listener;
  public Status      status;

  public    PrintStream           stream;
  private   ByteArrayOutputStream baos;
  private OPluginMessageHandler messageHandler;

  private OServer currentServerInstance;

  public ONeo4jImporterJob(ODocument cfg, OServer currentServerInstance, ONeo4ImporterListener listener) {
    this.cfg = cfg;
    this.listener = listener;

    this.baos = new ByteArrayOutputStream();
    this.stream = new PrintStream(baos);

    this.currentServerInstance = currentServerInstance;
  }


  @Override
  public void run() {

    String neo4jUrl = cfg.field("neo4jUrl");
    String neo4jUsername = cfg.field("neo4jUsername");
    String neo4jPassword = cfg.field("neo4jPassword");
    String odbName = cfg.field("odbName");
    String odbProtocol = cfg.field("odbProtocol");
    boolean overrideDB = cfg.field("overwriteDB");
    boolean indexesOnRelationships = cfg.field("indexesOnRelationships");
    int logLevel = Integer.parseInt((String)cfg.field("logLevel"));

    // disabling debug level
    if(logLevel > 0) {
      logLevel++;
    }

    status = Status.RUNNING;
    this.messageHandler = new ONeo4jImporterMessageHandler(this.stream, logLevel);


    ONeo4jImporterSettings settings = new ONeo4jImporterSettings(neo4jUrl, neo4jUsername, neo4jPassword, odbName, odbProtocol, overrideDB, indexesOnRelationships);
    final ONeo4jImporterPlugin neo4jImporterPlugin = new ONeo4jImporterPlugin();

    try {
      String databaseDirectory = null;
      OrientDB orientDBInstance = null;
      if (this.currentServerInstance != null) {
        databaseDirectory = this.currentServerInstance.getDatabaseDirectory();
        orientDBInstance = currentServerInstance.getContext();
      }
      neo4jImporterPlugin.executeJob(settings, this.messageHandler, databaseDirectory, orientDBInstance);
    } catch (Exception e) {
      e.printStackTrace();
    }

    synchronized (listener) {
      status = Status.FINISHED;
      try {
        listener.wait(5000);
        listener.onEnd(this);
      } catch (InterruptedException e) {
      }
    }
  }

  public void validate() {

  }

  /**
   * Single Job Status
   *
   * @return ODocument
   */
  public ODocument status() {

    synchronized (listener) {
      ODocument status = new ODocument();
      status.field("cfg", cfg);
      status.field("status", this.status);

      String lastBatchLog = "";
      if(this.messageHandler != null) {
        lastBatchLog = extractBatchLog();
      }
      status.field("log", lastBatchLog);

      if (this.status == Status.FINISHED) {
        listener.notifyAll();
      }
      return status;
    }

  }

  private String extractBatchLog() {

    String lastBatchLog = "Current status not correctly loaded.";

    synchronized (this.messageHandler) {

      // filling the last log batch
      int baosInitSize = baos.size();
      try {
        lastBatchLog = baos.toString("UTF-8");
      } catch (Exception e) {
        e.printStackTrace();
      }
      int baosFinalSize = baos.size();
      if (baosFinalSize - baosInitSize > 0) {
        OLogManager.instance().info(this, "Losing some buffer info.");
      } else {
        baos.reset();
      }
    }
    return lastBatchLog;
  }

  public enum Status {
    STARTED, RUNNING, FINISHED
  }
}
