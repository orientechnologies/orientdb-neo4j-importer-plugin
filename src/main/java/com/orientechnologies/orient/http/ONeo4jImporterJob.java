package com.orientechnologies.orient.http;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.neo4jimporter.*;
import com.orientechnologies.orient.outputmanager.OOutputStreamManager;
import com.orientechnologies.orient.server.OServer;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by gabriele on 27/02/17.
 */
public class ONeo4jImporterJob  implements Runnable {

  private final ODocument cfg;
  private       ONeo4ImporterListener listener;
  public Status      status;

  public  PrintStream           stream;
  private ByteArrayOutputStream baos;
  private OOutputStreamManager outputMgr;

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
    this.outputMgr = new OOutputStreamManager(this.stream, logLevel);


    ONeo4jImporterSettings settings = new ONeo4jImporterSettings(neo4jUrl, neo4jUsername, neo4jPassword, odbName, odbProtocol, overrideDB, indexesOnRelationships);
    final ONeo4jImporterPlugin neo4jImporterPlugin = new ONeo4jImporterPlugin();

    try {
      String databaseDirectory = null;
      OrientDB orientDBInstance = null;
      if (this.currentServerInstance != null) {
        databaseDirectory = this.currentServerInstance.getDatabaseDirectory();
        orientDBInstance = currentServerInstance.getContext();
      }
      neo4jImporterPlugin.executeJob(settings, this.outputMgr, databaseDirectory, orientDBInstance);
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
      if(this.outputMgr != null) {
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

    synchronized (this.outputMgr) {

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
