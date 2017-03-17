package com.orientechnologies.orient.neo4jimporter;

import com.orientechnologies.orient.context.ONeo4jImporterContext;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.listener.OProgressMonitor;
import com.orientechnologies.orient.outputmanager.OOutputStreamManager;

import java.util.Timer;
import java.util.TimerTask;

import static com.orientechnologies.orient.neo4jimporter.ONeo4jImporter.PROGRAM_NAME;

/**
 * Created by frank on 13/11/2016.
 */
public class ONeo4jImporterMain {

  private static OOutputStreamManager outputManager = new OOutputStreamManager(2);

  public static void main(String[] args) throws Exception {

    int returnValue = 1;

    ONeo4jImporterCommandLineParser commandParser = new ONeo4jImporterCommandLineParser();
    ONeo4jImporterSettings settings = commandParser.getNeo4jImporter(args);
    returnValue = executeJob(settings);

    System.exit(returnValue);
  }

  public static int executeJob(ONeo4jImporterSettings settings) {

    ONeo4jImporterContext.getInstance().setOutputManager(outputManager);
    ONeo4jImporterContext.getInstance().getOutputManager().info("\n");
    ONeo4jImporterContext.getInstance().getOutputManager().info(String.format(PROGRAM_NAME + " v.%s - %s\n\n", OConstants.getVersion(), OConstants.COPYRIGHT));
    ONeo4jImporterContext.getInstance().getOutputManager().info("\n");

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
    }
    return returnValue;
  }

}