package com.orientechnologies.orient.neo4jimporter;

import com.orientechnologies.orient.context.ONeo4jImporterContext;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.http.OServerCommandNeo4jImporter;
import com.orientechnologies.orient.outputmanager.OOutputStreamManager;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpAbstract;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;

/**
 * Created by gabriele on 01/03/17.
 */
public class ONeo4jImporterPlugin extends OServerPluginAbstract {

  private OServer server;
  private String  orientdbDatabasesAbsolutePath;

  public ONeo4jImporterPlugin() {}

  public void executeJob(ONeo4jImporterSettings settings, OOutputStreamManager outputManager) throws Exception {

    final ONeo4jImporter neo4jImporter = new ONeo4jImporter(settings, this.orientdbDatabasesAbsolutePath);
    ONeo4jImporterContext.getInstance().setOutputManager(outputManager);

    try {
      neo4jImporter.execute();
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

    try {
      Thread.sleep(5000L);
    } catch(Exception e) {
      e.printStackTrace();
    }
    final OServerNetworkListener listener = server.getListenerByProtocol(ONetworkProtocolHttpAbstract.class);
    if (listener == null)
      throw new OConfigurationException("HTTP listener not found");

    listener.registerStatelessCommand(new OServerCommandNeo4jImporter());

    this.orientdbDatabasesAbsolutePath = server.getDatabaseDirectory();
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