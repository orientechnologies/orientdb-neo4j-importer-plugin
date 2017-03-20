/*
 *
 *  * Copyright 2014 Orient Technologies.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *  
 */

package com.orientechnologies.orient.neo4jimporter;

import com.orientechnologies.orient.connection.OSourceNeo4jInfo;
import com.orientechnologies.orient.context.ONeo4jImporterContext;
import com.orientechnologies.orient.context.ONeo4jImporterStatistics;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import org.neo4j.driver.v1.Session;

import java.io.File;
import java.text.DecimalFormat;

/**
 * The main class of the ONeo4jImporter. It is instantiated from the ONeo4jImporterCommandLineParser
 *
 * @author Santo Leto
 */

public class ONeo4jImporter {

  public static final String PROGRAM_NAME = "Neo4j to OrientDB Importer";
  private final ONeo4jImporterSettings settings;
  private String orientdbDatabasesAbsolutePath;


  /**
   * Used byt the script work flow
   * @param settings
   * @throws Exception
   */
  public ONeo4jImporter(final ONeo4jImporterSettings settings) throws Exception {
    this.settings = settings;
  }


  /**
   * Used by the plugin work flow
   * @param settings
   * @param orientdbDatabasesAbsolutePath
   * @throws Exception
   */
  public ONeo4jImporter(final ONeo4jImporterSettings settings, String orientdbDatabasesAbsolutePath) throws Exception {
    this.settings = settings;
    this.orientdbDatabasesAbsolutePath = orientdbDatabasesAbsolutePath;
  }

  public int execute() throws Exception {

    int returnCode = 1;
    String logString = "";
    String keepLogString = "";
    double startTime = System.currentTimeMillis();
    double value;

    boolean migrateRels = true;  //set to false only during debug
    boolean migrateNodes = true; //set to false only during debug
    boolean relSampleOnly = false; //set to true only during debug

    DecimalFormat df = new DecimalFormat("#");
    DecimalFormat dfd = new DecimalFormat("#.##");

    logString = PROGRAM_NAME + " - v." + OConstants.getVersion() + " started!\n\n";
    ONeo4jImporterContext.getInstance().getOutputManager().info(logString);

    // parameters
    String neo4jUrl = settings.getNeo4jUrl();
    String neo4jUsername = settings.getNeo4jUsername();
    String neo4jPassword = settings.getNeo4jPassword();
    String orientDbPath = settings.getOrientDbPath();
    String orientDbProtocol = settings.getOrientDbProtocol();
    boolean overwriteOrientDBDir = settings.getOverwriteOrientDbDir();
    boolean neo4jRelIdIndex = settings.getCreateIndexOnNeo4jRelID();

    if(this.orientdbDatabasesAbsolutePath != null && orientDbProtocol.equals("plocal")) {
      // entered in the work flow through the plugin, so orientDbPath contains just the name, and we must prepend orientdbDatabasesAbsolutePath if we want to connect in plocal
      orientDbPath = this.orientdbDatabasesAbsolutePath + orientDbPath;
    }

    boolean dbExist = true;
    String dbUrl = orientDbProtocol + ":" + orientDbPath;
    OrientGraphFactory oFactory = new OrientGraphFactory(dbUrl, "admin", "admin", false);
    ODatabaseDocumentTx db = null;
    try {
      db = oFactory.getDatabase(false, true);
    } catch(ODatabaseException e) {
      dbExist = false;
    }

    if (dbExist) {
      if (overwriteOrientDBDir) {
        logString = "Directory '" + orientDbPath + "' exists already and the overwrite option '-o' is 'true'. Directory will be erased";
        ONeo4jImporterContext.getInstance().getOutputManager().warn(logString);
        db.drop();
      } else {

        //we exit the program
        logString = "ERROR: The directory '" + orientDbPath
            + "' exists and the overwrite option '-o' is 'false' (default). Please delete the directory or run the program with the '-o true' option. Exiting.\n\n";

        ONeo4jImporterContext.getInstance().getOutputManager().error(logString);
      }
    }

    //
    // PHASE 1 : INITIALIZATION
    //

    OSourceNeo4jInfo sourceNeo4jInfo = new OSourceNeo4jInfo(neo4jUrl, neo4jUsername, neo4jPassword);
    ONeo4jImporterInitializer initializer = new ONeo4jImporterInitializer(sourceNeo4jInfo, orientDbProtocol, orientDbPath);
    Session neo4jSession = initializer.initConnections();
    String orientVertexClass = initializer.getOrientVertexClass();
    OrientGraphNoTx oDb = initializer.getoDb();
    oFactory = initializer.getOFactory();
    ONeo4jImporterStatistics statistics = ONeo4jImporterContext.getInstance().getStatistics();


    //
    // PHASE 2 : MIGRATION OF VERTICES AND EDGES
    //

    ONeo4jImporterVerticesAndEdgesMigrator verticesAndEdgesImporter = new ONeo4jImporterVerticesAndEdgesMigrator(keepLogString,
        migrateRels, migrateNodes, df, orientVertexClass, oDb, statistics, relSampleOnly, neo4jRelIdIndex);
    verticesAndEdgesImporter.invoke(neo4jSession);
    keepLogString = verticesAndEdgesImporter.getKeepLogString();

    //
    // PHASE 3 : SCHEMA MIGRATION
    //

    ONeo4jImporterSchemaMigrator schemaMigrator = new ONeo4jImporterSchemaMigrator(keepLogString, df, oDb, statistics);
    schemaMigrator.invoke(neo4jSession);

    //
    // PHASE 4 : SHUTDOWN OF THE SERVERS AND SUMMARY INFO
    //

    stopServers(neo4jSession, oDb, oFactory);
    printSummary(startTime, df, dfd, statistics, initializer, verticesAndEdgesImporter, schemaMigrator, neo4jRelIdIndex);

    returnCode = 0;
    return returnCode;
  }

  private void stopServers(Session neo4jSession, OrientGraphNoTx oDb, OrientGraphFactory oFactory) throws Exception {

    String logString;
    logString = "\nShutting down OrientDB...";

    ONeo4jImporterContext.getInstance().getOutputManager().info(logString);

    oDb.shutdown();
    oFactory.close();

    ONeo4jImporterContext.getInstance().getOutputManager().info("\rShutting down OrientDB...Done\n");

    logString = "Shutting down Neo4j...";

    try {
      if(neo4jSession != null) {
        neo4jSession.close();
      }
    } catch(Exception e) {
      String mess = "";
      ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
      ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
      throw new Exception(e.getMessage());
    }

    ONeo4jImporterContext.getInstance().getOutputManager().info(logString);

    ONeo4jImporterContext.getInstance().getOutputManager().info("\rShutting down Neo4j...Done\n");
  }

  private void printSummary(double startTime, DecimalFormat df, DecimalFormat dfd, ONeo4jImporterStatistics counters,
      ONeo4jImporterInitializer initializer, ONeo4jImporterVerticesAndEdgesMigrator migrator,
      ONeo4jImporterSchemaMigrator schemaMigrator, boolean neo4jRelIdIndex) {

    double value;
    String logString;

    double stopTime = System.currentTimeMillis();
    double elapsedTime = (stopTime - startTime);
    double elapsedTimeSeconds = elapsedTime / (1000);

    double initializationElapsedTime = (initializer.getInitializationStopTime() - initializer.getInitializationStartTime());
    double initializationElapsedTimeSeconds = initializationElapsedTime / (1000);

    double importingNodesElapsedTime = counters.importingNodesStopTime - counters.importingNodesStartTime;
    double importingNodesElapsedTimeSeconds = importingNodesElapsedTime / (1000);

    double importingRelsElapsedTime = migrator.getImportingRelsStopTime() - migrator.getImportingRelsStartTime();
    double importingRelsElapsedTimeSeconds = importingRelsElapsedTime / (1000);

    double importingSchemaElapsedTime = schemaMigrator.getImportingSchemaStopTime() - schemaMigrator.getImportingSchemaStartTime();
    double importingSchemaElapsedTimeSeconds = importingSchemaElapsedTime / (1000);

    double internalVertexIndicesElapsedTime = counters.internalVertexIndicesStopTime - counters.internalVertexIndicesStartTime;
    double internalVertexIndicesElapsedTimeSeconds = internalVertexIndicesElapsedTime / (1000);

    double internalEdgeIndicesElapsedTime = counters.internalEdgeIndicesStopTime - counters.internalEdgeIndicesStartTime;
    double internalEdgeIndicesElapsedTimeSeconds = internalEdgeIndicesElapsedTime / (1000);

    double neo4jTotalInternalIndicesCounter = counters.neo4jInternalVertexIndicesCounter + counters.neo4jInternalEdgeIndicesCounter;

    String format = "%-100s %s";
    ONeo4jImporterContext.getInstance().getOutputManager().info("\n\n");
    ONeo4jImporterContext.getInstance().getOutputManager().info("===============");
    ONeo4jImporterContext.getInstance().getOutputManager().info("Import Summary:");
    ONeo4jImporterContext.getInstance().getOutputManager().info("===============");
    ONeo4jImporterContext.getInstance().getOutputManager().info("\n\n");
    ONeo4jImporterContext.getInstance().getOutputManager().info(format, "- Found Neo4j Nodes", ": " + df.format(counters.neo4jNodeCounter) + "\n");
    ONeo4jImporterContext.getInstance().getOutputManager().info(format, "-- With at least one Label", ": " + df.format(counters.neo4jNodeMultipleLabelsCounter) + "\n");
    ONeo4jImporterContext.getInstance().getOutputManager().info(format, "-- With multiple Labels", ": " + df.format(counters.neo4jNodeMultipleLabelsCounter) + "\n");
    ONeo4jImporterContext.getInstance().getOutputManager().info(format, "-- Without Labels", ": " + df.format(counters.neo4jNodeNoLabelCounter) + "\n");
    ONeo4jImporterContext.getInstance().getOutputManager().info(format, "- Imported OrientDB Vertices", ": " + df.format(counters.orientDBImportedVerticesCounter));
    if (counters.neo4jNodeCounter > 0) {
      value = (counters.orientDBImportedVerticesCounter / counters.neo4jNodeCounter) * 100;
      ONeo4jImporterContext.getInstance().getOutputManager().info(" (" + df.format(value) + "%)");
    }

    ONeo4jImporterContext.getInstance().getOutputManager().info("\n\n");
    ONeo4jImporterContext.getInstance().getOutputManager().info(format, "- Found Neo4j Relationships", ": " + df.format(counters.neo4jRelCounter) + "\n");
    ONeo4jImporterContext.getInstance().getOutputManager().info(format, "- Imported OrientDB Edges", ": " + df.format(counters.orientDBImportedEdgesCounter));
    if (counters.neo4jRelCounter > 0) {
      value = (counters.orientDBImportedEdgesCounter / counters.neo4jRelCounter) * 100;
      ONeo4jImporterContext.getInstance().getOutputManager().info(" (" + df.format(value) + "%)");
    }

    ONeo4jImporterContext.getInstance().getOutputManager().info("\n\n");
    ONeo4jImporterContext.getInstance().getOutputManager().info(format, "- Found Neo4j Constraints", ": " + df.format(counters.neo4jConstraintsCounter) +"\n");
    ONeo4jImporterContext.getInstance().getOutputManager().info(format, "- Imported OrientDB Constraints (UNIQUE Indices created)", ": " + df.format(counters.orientDBImportedConstraintsCounter));
    if (counters.neo4jConstraintsCounter > 0) {
      value = (counters.orientDBImportedConstraintsCounter / counters.neo4jConstraintsCounter) * 100;
      ONeo4jImporterContext.getInstance().getOutputManager().info(" (" + df.format(value) + "%)");
    }
    ONeo4jImporterContext.getInstance().getOutputManager().info("\n\n");
    ONeo4jImporterContext.getInstance().getOutputManager().info(format, "- NOT UNIQUE Indices created due to failure in creating UNIQUE Indices", ": " + df.format(counters.orientDBImportedNotUniqueWorkaroundCounter));
    if (counters.neo4jConstraintsCounter > 0) {
      value = (counters.orientDBImportedNotUniqueWorkaroundCounter / counters.neo4jConstraintsCounter) * 100;
      ONeo4jImporterContext.getInstance().getOutputManager().info(" (" + df.format(value) + "%)");
    }

    ONeo4jImporterContext.getInstance().getOutputManager().info("\n\n");
    ONeo4jImporterContext.getInstance().getOutputManager().info(format, "- Found Neo4j (non-constraint) Indices", ": " + df.format(counters.neo4jNonConstraintsIndicesCounter) +"\n");
    ONeo4jImporterContext.getInstance().getOutputManager().info(format, "- Imported OrientDB Indices", ": " + df.format(counters.orientDBImportedIndicesCounter));
    if (counters.neo4jNonConstraintsIndicesCounter > 0) {
      value = (counters.orientDBImportedIndicesCounter / counters.neo4jNonConstraintsIndicesCounter) * 100;
      ONeo4jImporterContext.getInstance().getOutputManager().info(" (" + df.format(value) + "%)");
    }

    ONeo4jImporterContext.getInstance().getOutputManager().info("\n\n");

    //ONeo4jImporterContext.getInstance().getOutputManager().info("- Additional created Indices (on vertex properties 'neo4jNodeID' & 'neo4jLabelList')          : " + df.format(counters.neo4jInternalIndicesCounter));

    ONeo4jImporterContext.getInstance().getOutputManager().info(format, "- Additional internal Indices created", ": " + df.format(neo4jTotalInternalIndicesCounter) + "\n");

    ONeo4jImporterContext.getInstance().getOutputManager().info("\n");
    ONeo4jImporterContext.getInstance().getOutputManager().info(format, "- Total Import time:", ": " + df.format(elapsedTimeSeconds) + " seconds\n");

    ONeo4jImporterContext.getInstance().getOutputManager().info(format, "-- Initialization time", ": " + df.format(initializationElapsedTimeSeconds) + " seconds\n");
    ONeo4jImporterContext.getInstance().getOutputManager().info(format, "-- Time to Import Nodes", ": " + df.format(importingNodesElapsedTimeSeconds) + " seconds");
    if (importingNodesElapsedTimeSeconds > 0) {
      value = (counters.orientDBImportedVerticesCounter / importingNodesElapsedTimeSeconds);
      ONeo4jImporterContext.getInstance().getOutputManager().info(" (" + dfd.format(value) + " nodes/sec)");
    }

    ONeo4jImporterContext.getInstance().getOutputManager().info("\n");
    ONeo4jImporterContext.getInstance().getOutputManager().info(format, "-- Time to Import Relationships", ": " + df.format(importingRelsElapsedTimeSeconds) + " seconds");
    if (importingRelsElapsedTimeSeconds > 0) {
      value = (counters.orientDBImportedEdgesCounter / importingRelsElapsedTimeSeconds);
      ONeo4jImporterContext.getInstance().getOutputManager().info(" (" + dfd.format(value) + " rels/sec)");
    }

    ONeo4jImporterContext.getInstance().getOutputManager().info("\n");
    ONeo4jImporterContext.getInstance().getOutputManager().info(format, "-- Time to Import Constraints and Indices", ": " + df.format(importingSchemaElapsedTimeSeconds) + " seconds");
    if (importingSchemaElapsedTimeSeconds > 0) {
      value = ((counters.orientDBImportedConstraintsCounter + counters.orientDBImportedIndicesCounter)
          / importingSchemaElapsedTimeSeconds);
      ONeo4jImporterContext.getInstance().getOutputManager().info(" (" + dfd.format(value) + " indices/sec)");
    }

    ONeo4jImporterContext.getInstance().getOutputManager().info("\n");
    ONeo4jImporterContext.getInstance().getOutputManager().info(format, "-- Time to Create Internal Indices (on vertex properties 'neo4jNodeID' & 'neo4jLabelList')", ": " + df.format(internalVertexIndicesElapsedTimeSeconds) + " seconds");
    if (internalVertexIndicesElapsedTimeSeconds > 0) {
      value = (counters.neo4jInternalVertexIndicesCounter / internalVertexIndicesElapsedTimeSeconds);
      ONeo4jImporterContext.getInstance().getOutputManager().info(" (" + dfd.format(value) + " indices/sec)");
    }

    if (neo4jRelIdIndex) {
      ONeo4jImporterContext.getInstance().getOutputManager().info("\n");
      ONeo4jImporterContext.getInstance().getOutputManager().info(format, "-- Time to Create Internal Indices (on edge property 'neo4jRelID')", ": " + df.format(internalEdgeIndicesElapsedTimeSeconds) + " seconds");
      if (internalEdgeIndicesElapsedTimeSeconds > 0) {
        value = (counters.neo4jInternalEdgeIndicesCounter / internalEdgeIndicesElapsedTimeSeconds);
        ONeo4jImporterContext.getInstance().getOutputManager().info(" (" + dfd.format(value) + " indices/sec)");
      }
      ONeo4jImporterContext.getInstance().getOutputManager().info("\n");
    }

    ONeo4jImporterContext.getInstance().getOutputManager().info("\n");
    logString = PROGRAM_NAME + " - v." + OConstants.getVersion() + " - PHASE 4 completed!\n\n";
    ONeo4jImporterContext.getInstance().getOutputManager().info(logString);
  }

}

	