package com.orientechnologies.orient.neo4jimporter;

import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.File;
import java.util.logging.Level;

import static com.orientechnologies.orient.neo4jimporter.ONeo4jImporter.*;

/**
 * Created by frank on 08/11/2016.
 */
class ONeo4jImporterInitializer {
  private final String               neo4jDBPath;
  private final String               orientDbFolder;
  private       double               initializationStartTime;
  private       GraphDatabaseService neo4jGraphDb;
  private       OrientGraphFactory   oFactory;
  private       OrientGraphNoTx      oDb;
  private       String               orientVertexClass;
  private       double               initializationStopTime;

  public ONeo4jImporterInitializer(String neo4jDBPath, String orientDbFolder) {
    this.neo4jDBPath = neo4jDBPath;
    this.orientDbFolder = orientDbFolder;
  }

  public double getInitializationStartTime() {
    return initializationStartTime;
  }

  public GraphDatabaseService getNeo4jGraphDb() {
    return neo4jGraphDb;
  }

  public OrientGraphFactory getoFactory() {
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

  public ONeo4jImporterInitializer invoke() {
    String logString;//

    initializationStartTime = System.currentTimeMillis();

    //
    System.out.println("Please make sure that there are no running servers on:");
    System.out.println("  '" + neo4jDBPath + "' (Neo4j)");
    System.out.println("and:");
    System.out.println("  '" + orientDbFolder + "' (OrientDB)");
    //

    //
    System.out.println();
    System.out.print("Initializing Neo4j...");

    File DB_PATH = new File(neo4jDBPath);

    neo4jGraphDb = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
    ONeo4jImporterUtils.registerNeo4jShutdownHook(neo4jGraphDb);

    logString = "Initializing Neo4j...Done";

    System.out.print("\r" + logString + "\n");
    ONeo4jImporter.importLogger.log(Level.INFO, logString);
    //

    //
    System.out.println();
    System.out.print("Initializing OrientDB...");

    String dbUrl = "plocal:" + orientDbFolder;

    OGlobalConfiguration.USE_WAL.setValue(false);
    OGlobalConfiguration.WAL_SYNC_ON_PAGE_FLUSH.setValue(false);

    oFactory = new OrientGraphFactory(dbUrl, "admin", "admin");
    oFactory.declareIntent(new OIntentMassiveInsert());
    oDb = oFactory.getNoTx();

    oDb.setStandardElementConstraints(false);

    orientVertexClass = "";

    logString = "Initializing OrientDB...Done";

    System.out.print("\r" + logString + "\n");
    ONeo4jImporter.importLogger.log(Level.INFO, logString);
    //

    //
    System.out.println();
    System.out.println("Importing Neo4j database:");
    System.out.println("  '" + neo4jDBPath + "'");
    System.out.println("into OrientDB database:");
    System.out.println("  '" + orientDbFolder + "'");
    //

    logString = PROGRAM_NAME + " - v." + OConstants.getVersion() + " - PHASE 1 completed!\n";
    ONeo4jImporter.importLogger.log(Level.INFO, logString);

    initializationStopTime = System.currentTimeMillis();
    return this;
  }
}
