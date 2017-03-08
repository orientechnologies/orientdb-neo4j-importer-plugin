package com.orientechnologies.orient.http;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.depsloader.OChildFirstURLClassLoader;
import com.orientechnologies.orient.depsloader.OPluginDependencyManager;
import com.orientechnologies.orient.neo4jimporter.ONeo4jImporter;
import com.orientechnologies.orient.neo4jimporter.ONeo4jImporterCommandLineParser;
import com.orientechnologies.orient.neo4jimporter.ONeo4jImporterPlugin;

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

    System.out.println("Thread: id:" + Thread.currentThread().getId() + ", name:" + Thread.currentThread().getId() + ", state:" + Thread.currentThread().getState() +
        ", thread-group:" + Thread.currentThread().getThreadGroup().getName());

    ClassLoader currentThreadClassLoader = Thread.currentThread().getContextClassLoader();
    if(!(currentThreadClassLoader instanceof OChildFirstURLClassLoader)) {

      try {
        // defining child class loader to load neo4j dependencies
        OPluginDependencyManager.setNewChildClassLoaderFromJarDir("/Users/gabriele/neo4j-community-3.1.1/lib");
      } catch(Exception e) {
        e.printStackTrace();
      }
    }

    List<String> argsList = new ArrayList<String>();
    final String neo4jLibDir = cfg.field("neo4jLibDir");
    final String neo4jDbDir = cfg.field("neo4jDbDir");
    final String odbDir = cfg.field("outDbUrl");
    final String override = cfg.field("override");
    final String options = cfg.field("options");

    argsList.add("-neo4jlibdir");
    argsList.add(neo4jLibDir);
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

    String[] args = argsList.toArray(new String[argsList.size()]);

    try {
      final ONeo4jImporterPlugin neo4jImporterPlugin = new ONeo4jImporterPlugin();
      neo4jImporterPlugin.executeJob(args);
    } catch (Exception e) {
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
