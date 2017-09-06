package com.orientechnologies.orient.neo4jimporter;

import com.orientechnologies.orient.context.ONeo4jImporterContext;
import com.orientechnologies.orient.context.ONeo4jImporterMessageHandler;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.listener.OProgressMonitor;
import com.orientechnologies.orient.output.OOutputStreamManager;
import com.orientechnologies.orient.output.OPluginMessageHandler;

import java.util.Timer;
import java.util.TimerTask;

import static com.orientechnologies.orient.neo4jimporter.ONeo4jImporter.PROGRAM_NAME;

/**
 * Created by frank on 13/11/2016.
 */
public class ONeo4jImporterMain {

  private static OPluginMessageHandler messageHandler = new ONeo4jImporterMessageHandler(2);


  public static void main(String[] args) throws Exception {

    int returnValue = 1;

//    ONeo4jImporterCommandLineParser commandParser = new ONeo4jImporterCommandLineParser();
//    ONeo4jImporterSettings settings = commandParser.getNeo4jImporterSettings(args);

    // TO DELETE
    ONeo4jImporterSettings settings = new ONeo4jImporterSettings();

    returnValue = executeJob(settings);

    System.exit(returnValue);
  }

  public static int executeJob(ONeo4jImporterSettings settings) {

    ONeo4jImporterContext.getInstance().setMessageHandler(messageHandler);
    ONeo4jImporterContext.getInstance().getMessageHandler().info("\n");
    ONeo4jImporterContext.getInstance().getMessageHandler().info(String.format(PROGRAM_NAME + " v.%s - %s\n\n", OConstants.ORIENT_VERSION, OConstants.COPYRIGHT));
    ONeo4jImporterContext.getInstance().getMessageHandler().info("\n");

    //parses the command line parameters, and starts the import (.execute). Then exits
    int returnValue = 1;

    try {

      // Progress Monitor initialization
      OProgressMonitor progressMonitor = new OProgressMonitor();
      progressMonitor.initialize();

      // Timer for statistics notifying
      Timer timer = new Timer();
      try {
        timer.scheduleAtFixedRate(new TimerTask() {

          @Override
          public void run() {
            ONeo4jImporterContext.getInstance().getStatistics().notifyListeners();
          }
        }, 0, 1000);

        final ONeo4jImporter neo4jImporter = new ONeo4jImporter(settings);
        returnValue = neo4jImporter.execute();

      } finally {
        timer.cancel();
      }

    } catch (Exception e) {
      String mess = "";
      ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
      ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
      throw new RuntimeException(e);
    }
    return returnValue;
  }

}