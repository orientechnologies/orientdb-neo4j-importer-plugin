package com.orientechnologies.orient.neo4jimporter;

import com.orientechnologies.orient.context.ONeo4jImporterContext;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.http.OServerCommandNeo4jImporter;
import com.orientechnologies.orient.listener.OProgressMonitor;
import com.orientechnologies.orient.output.OPluginMessageHandler;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpAbstract;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;

import java.util.Timer;
import java.util.TimerTask;

import static com.orientechnologies.orient.neo4jimporter.ONeo4jImporter.PROGRAM_NAME;

/**
 * Created by gabriele on 01/03/17.
 */
public class ONeo4jImporterPlugin extends OServerPluginAbstract {

  private OServer server;

  public ONeo4jImporterPlugin() {}

  public void executeJob(ONeo4jImporterSettings settings, OPluginMessageHandler messageHandler, String orientdbDatabasesAbsolutePath) throws Exception {

    final ONeo4jImporter neo4jImporter = new ONeo4jImporter(settings, orientdbDatabasesAbsolutePath);
    ONeo4jImporterContext.newInstance().setMessageHandler(messageHandler);
    ONeo4jImporterContext.getInstance().getMessageHandler().info("\n");
    ONeo4jImporterContext.getInstance().getMessageHandler().info(String.format(PROGRAM_NAME + " v.%s - %s\n\n", OConstants.ORIENT_VERSION, OConstants.COPYRIGHT));
    ONeo4jImporterContext.getInstance().getMessageHandler().info("\n");

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

        neo4jImporter.execute();

      } finally {
        timer.cancel();
      }

    } catch(Exception e) {
      String mess = "";
      ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
      ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
    }
  }

  @Override
  public String getName() {
    return "neo4j-importer";
  }

  @Override
  public void startup() {

    final OServerNetworkListener listener = server.getListenerByProtocol(ONetworkProtocolHttpAbstract.class);
    if (listener == null)
      throw new OConfigurationException("HTTP listener not found");

    listener.registerStatelessCommand(new OServerCommandNeo4jImporter());
  }

  @Override
  public void config(OServer oServer, OServerParameterConfiguration[] iParams) {
    server = oServer;
  }

  @Override
  public void shutdown() {
    super.shutdown();
  }
}