package com.orientechnologies.orient.http;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.neo4jimporter.ONeo4jImporter;
import com.orientechnologies.orient.neo4jimporter.ONeo4jImporterCommandLineParser;
import com.orientechnologies.orient.neo4jimporter.ONeo4jImporterPlugin;
import com.orientechnologies.orient.neo4jimporter.ONeo4jImporterSettings;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by gabriele on 27/02/17.
 */
public class ONeo4jImporterJob  implements Runnable {

  private final ODocument cfg;
  private       ONeo4ImporterListener listener;
  public Status      status;

  public ONeo4jImporterJob(ODocument cfg, ONeo4ImporterListener listener) {
    this.cfg = cfg;
    this.listener = listener;
  }


  @Override
  public void run() {

    List<String> argsList = new ArrayList<String>();


      final String neo4jDbDir = cfg.field("neo4jDbDir");
      final String odbDir = cfg.field("outDbUrl");
      final String override = cfg.field("override");
      final String options = cfg.field("options");


      argsList.add("-neo4jdbdir");
      argsList.add(neo4jDbDir);
      if(odbDir != null) {
        argsList.add("-odbdir");
        argsList.add(odbDir);
      }
      if(options != null) {
        argsList.add("");
        argsList.add(options);
      }
      status = Status.RUNNING;

    // TODO: change default values ov booleans
    ONeo4jImporterSettings settings = new ONeo4jImporterSettings(neo4jDbDir, odbDir, true, true);
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

  public enum Status {
    STARTED, RUNNING, FINISHED
  }
}
