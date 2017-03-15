package com.orientechnologies.orient.http;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.neo4jimporter.ONeo4jImporter;
import com.orientechnologies.orient.neo4jimporter.ONeo4jImporterCommandLineParser;
import com.orientechnologies.orient.neo4jimporter.ONeo4jImporterPlugin;
import com.orientechnologies.orient.neo4jimporter.ONeo4jImporterSettings;

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

  public ONeo4jImporterJob(ODocument cfg, ONeo4ImporterListener listener) {
    this.cfg = cfg;
    this.listener = listener;

    this.baos = new ByteArrayOutputStream();
    this.stream = new PrintStream(baos);
  }


  @Override
  public void run() {

    String neo4jUrl = cfg.field("neo4jUrl");
    String neo4jUsername = cfg.field("neo4jUsername");
    String neo4jPassword = cfg.field("neo4jPassword");
    String odbDir = cfg.field("outDbUrl");
    String odbProtocol = cfg.field("odbProtocol");
    boolean overrideDB = cfg.field("overwriteDB");
    boolean indexesOnRelationships = cfg.field("indexesOnRelationships");

    status = Status.RUNNING;

    // TODO: change default values ov booleans
    ONeo4jImporterSettings settings = new ONeo4jImporterSettings(neo4jUrl, neo4jUsername, neo4jPassword, odbDir, odbProtocol, overrideDB, indexesOnRelationships);
    final ONeo4jImporterPlugin neo4jImporterPlugin = new ONeo4jImporterPlugin();

    try {
      neo4jImporterPlugin.executeJob(settings);
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
      status.field("log", baos.toString());
      if (this.status == Status.FINISHED) {
        listener.notifyAll();
      }
      return status;
    }

  }

  public enum Status {
    STARTED, RUNNING, FINISHED
  }
}
