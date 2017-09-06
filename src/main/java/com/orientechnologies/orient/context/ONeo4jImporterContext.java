package com.orientechnologies.orient.context;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.output.OOutputStreamManager;
import com.orientechnologies.orient.output.OPluginMessageHandler;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;

/**
 * Created by gabriele on 15/03/17.
 */
public class ONeo4jImporterContext {

  private OrientDB orient;
  private static ONeo4jImporterContext instance = null;

  private OPluginMessageHandler    messageHandler;
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
      this.messageHandler.debug(message);
      break;
    case "info":
      this.messageHandler.info(message);
      break;
    case "warn":
      this.messageHandler.warn(message);
      break;
    case "error":
      this.messageHandler.error(message);
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
      this.messageHandler.debug("\n" + s + "\n");
      break;
    case "info":
      this.messageHandler.info("\n" + s + "\n");
      break;
    case "warn":
      this.messageHandler.warn("\n" + s + "\n");
      break;
    case "error":
      this.messageHandler.error("\n" + s + "\n");
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

  public OPluginMessageHandler getMessageHandler() {
    return this.messageHandler;
  }

  public void setMessageHandler(OPluginMessageHandler messageHandler) {
    this.messageHandler = messageHandler;
  }

  public ONeo4jImporterStatistics getStatistics() {
    return this.statistics;
  }

  public void setStatistics(ONeo4jImporterStatistics statistics) {
    this.statistics = statistics;
  }
}
