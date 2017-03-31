package com.orientechnologies.orient.context;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.outputmanager.OOutputStreamManager;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;

/**
 * Created by gabriele on 15/03/17.
 */
public class ONeo4jImporterContext {

  private OrientDB orient;
  private static ONeo4jImporterContext instance = null;

  private OOutputStreamManager     outputManager;
  private ONeo4jImporterStatistics statistics;

  public ONeo4jImporterContext(OrientDB orientDBInstance) {
    this.statistics = new ONeo4jImporterStatistics();
    this.orient = orientDBInstance;
  }

  public ONeo4jImporterContext(String url) {
    this.statistics = new ONeo4jImporterStatistics();
    this.initOrientDBInstance(url);
  }

  public static ONeo4jImporterContext getInstance() {
    return instance;
  }

  public static ONeo4jImporterContext newInstance(OrientDB orientDBInstance) {
    instance = new ONeo4jImporterContext(orientDBInstance);
    return instance;
  }

  public static ONeo4jImporterContext newInstance(String url) {
    instance = new ONeo4jImporterContext(url);
    return instance;
  }

  /**
   * Prints the error message for a caught exception according to a level passed as argument. It's composed of:
   * - defined error message
   * - exception message
   *
   * @param e
   * @param message
   * @param level
   *
   * @return printedMessage
   */
  public String printExceptionMessage(Exception e, String message, String level) {

    if (e.getMessage() != null)
      message += "\n" + e.getClass().getName() + " - " + e.getMessage();
    else
      message += "\n" + e.getClass().getName();

    switch (level) {
    case "debug":
      this.outputManager.debug(message);
      break;
    case "info":
      this.outputManager.info(message);
      break;
    case "warn":
      this.outputManager.warn(message);
      break;
    case "error":
      this.outputManager.error(message);
      break;
    }

    return message;
  }

  /**
   * Builds the exception stack trace and prints it according to a level passed as argument.
   *
   * @param e
   * @param level
   *
   * @return printedMessage
   */
  public String printExceptionStackTrace(Exception e, String level) {

    // copying the exception stack trace in the string
    Writer writer = new StringWriter();
    e.printStackTrace(new PrintWriter(writer));
    String s = writer.toString();

    switch (level) {
    case "debug":
      this.outputManager.debug("\n" + s + "\n");
      break;
    case "info":
      this.outputManager.info("\n" + s + "\n");
      break;
    case "warn":
      this.outputManager.warn("\n" + s + "\n");
      break;
    case "error":
      this.outputManager.error("\n" + s + "\n");
      break;
    }

    return s;
  }

  public OrientDB getOrientDBInstance() {
    return orient;
  }

  public void initOrientDBInstance(String url) {
    this.orient = new OrientDB(url, OrientDBConfig.defaultConfig());
  }

  public void initOrientDBInstance(String url, OrientDBConfig config) {
    this.orient = new OrientDB(url, config);
  }

  public void closeOrientDBInstance() {
    this.orient.close();
  }

  public OOutputStreamManager getOutputManager() {
    return this.outputManager;
  }

  public void setOutputManager(OOutputStreamManager outputManager) {
    this.outputManager = outputManager;
  }

  public ONeo4jImporterStatistics getStatistics() {
    return this.statistics;
  }

  public void setStatistics(ONeo4jImporterStatistics statistics) {
    this.statistics = statistics;
  }
}
