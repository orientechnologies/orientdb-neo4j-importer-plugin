package com.orientechnologies.orient.neo4jimporter;

import com.orientechnologies.orient.connection.ONeo4jConnectionManager;
import com.orientechnologies.orient.connection.OSourceNeo4jInfo;
import com.orientechnologies.orient.context.ONeo4jImporterContext;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import org.neo4j.driver.v1.Session;

import java.io.File;
import java.util.logging.Level;

import static com.orientechnologies.orient.neo4jimporter.ONeo4jImporter.PROGRAM_NAME;

/**
 * Created by frank on 08/11/2016.
 */
class ONeo4jImporterInitializer {

  private final String               orientDbFolder;
  private final String               orientDbProtocol;
  private       double               initializationStartTime;
  private       OrientGraphFactory   oFactory;
  private       OrientGraphNoTx      oDb;
  private       String               orientVertexClass;
  private       double               initializationStopTime;

  private OSourceNeo4jInfo sourceNeo4jInfo;
  private Session neo4jSession;

  public ONeo4jImporterInitializer(OSourceNeo4jInfo sourceNeo4jInfo, String orientDbProtocol, String orientDbFolder) {
    this.orientDbFolder = orientDbFolder;
    this.orientDbProtocol = orientDbProtocol;
    this.sourceNeo4jInfo = sourceNeo4jInfo;
  }

  public OSourceNeo4jInfo getSourceNeo4jInfo() {
    return sourceNeo4jInfo;
  }

  public void setSourceNeo4jInfo(OSourceNeo4jInfo sourceNeo4jInfo) {
    this.sourceNeo4jInfo = sourceNeo4jInfo;
  }

  public double getInitializationStartTime() {
    return initializationStartTime;
  }

  public OrientGraphFactory getOFactory() {
    return oFactory;
  }

  public OrientGraphNoTx getoDb() {
    return oDb;
  }

  public String getOrientVertexClass() {
    return orientVertexClass;
  }

  public double getInitializationStopTime() {
    return initializationStopTime;
  }

  public Session getNeo4jSession() {
    return neo4jSession;
  }

  public void setNeo4jSession(Session neo4jSession) {
    this.neo4jSession = neo4jSession;
  }

  public Session initConnections() throws Exception {
    String logString;

    initializationStartTime = System.currentTimeMillis();

    ONeo4jImporterContext.getInstance().getOutputManager().info("\n\n");
    ONeo4jImporterContext.getInstance().getOutputManager().info("Trying connection to Neo4j...");

    ONeo4jConnectionManager connectionManager = new ONeo4jConnectionManager(sourceNeo4jInfo);
    Session neo4jSession = connectionManager.getSession();

    logString = "Trying connection to Neo4j...Neo4j server is alive and connection succeeded.";

    ONeo4jImporterContext.getInstance().getOutputManager().info("\r" + logString);

    ONeo4jImporterContext.getInstance().getOutputManager().info("\n");
    ONeo4jImporterContext.getInstance().getOutputManager().info("Initializing OrientDB...");

    String dbUrl = this.orientDbProtocol + ":" + orientDbFolder;

    oFactory = new OrientGraphFactory(dbUrl, "admin", "admin");
    oFactory.declareIntent(new OIntentMassiveInsert());
    oDb = oFactory.getNoTx();

    oDb.setStandardElementConstraints(false);

    orientVertexClass = "";

    logString = "Initializing OrientDB...Done\n";

    ONeo4jImporterContext.getInstance().getOutputManager().info("\r" + logString + "\n");

    ONeo4jImporterContext.getInstance().getOutputManager().info("Importing Neo4j database from server:");
    ONeo4jImporterContext.getInstance().getOutputManager().info("  '" + sourceNeo4jInfo.getNeo4jUrl() + "'");
    ONeo4jImporterContext.getInstance().getOutputManager().info("into OrientDB database:");
    ONeo4jImporterContext.getInstance().getOutputManager().info("  '" + orientDbFolder + "'\n");

    logString = PROGRAM_NAME + " - v." + OConstants.getVersion() + " - PHASE 1 completed!\n";
    ONeo4jImporterContext.getInstance().getOutputManager().info(logString);

    initializationStopTime = System.currentTimeMillis();
    return neo4jSession;
  }
}
