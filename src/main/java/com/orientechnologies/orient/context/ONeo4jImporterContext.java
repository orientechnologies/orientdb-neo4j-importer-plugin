package com.orientechnologies.orient.context;

import com.orientechnologies.orient.output.OPluginMessageHandler;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;

/**
 * Created by gabriele on 15/03/17.
 */
public class ONeo4jImporterContext {

  private static ONeo4jImporterContext instance = null;

  private OPluginMessageHandler    messageHandler;
  private ONeo4jImporterStatistics statistics;

  public ONeo4jImporterContext() {
    this.statistics = new ONeo4jImporterStatistics();
  }

  public static ONeo4jImporterContext getInstance() {
    if(instance == null) {
      instance = new ONeo4jImporterContext();
    }
    return instance;
  }

  public static ONeo4jImporterContext newInstance() {
    instance = new ONeo4jImporterContext();
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
