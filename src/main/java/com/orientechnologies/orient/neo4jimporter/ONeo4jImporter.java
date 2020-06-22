/*
 *
 *  *  Copyright 2010-2017 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.neo4jimporter;

import com.orientechnologies.orient.connection.OSourceNeo4jInfo;
import com.orientechnologies.orient.context.ONeo4jImporterContext;
import com.orientechnologies.orient.context.ONeo4jImporterStatistics;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.util.OFunctionsHandler;
import java.text.DecimalFormat;
import org.neo4j.driver.v1.Session;

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
   *
   * @param settings
   * @throws Exception
   */
  public ONeo4jImporter(final ONeo4jImporterSettings settings) throws Exception {
    this.settings = settings;
  }

  /**
   * Used by the plugin work flow
   *
   * @param settings
   * @param orientdbDatabasesAbsolutePath
   * @throws Exception
   */
  public ONeo4jImporter(final ONeo4jImporterSettings settings, String orientdbDatabasesAbsolutePath)
      throws Exception {
    this.settings = settings;
    this.orientdbDatabasesAbsolutePath = orientdbDatabasesAbsolutePath;
  }

  public int execute() throws Exception {

    int returnCode = 1;
    String logString = "";
    String keepLogString = "";
    long startTime = System.currentTimeMillis();
    double value;

    boolean migrateRels = true; // set to false only during debug
    boolean migrateNodes = true; // set to false only during debug
    boolean relSampleOnly = false; // set to true only during debug

    DecimalFormat df = new DecimalFormat("#");
    DecimalFormat dfd = new DecimalFormat("#.##");

    logString = PROGRAM_NAME + " - v." + OConstants.getVersion() + " started!\n\n";
    ONeo4jImporterContext.getInstance().getMessageHandler().info(this, logString);

    // parameters
    String neo4jUrl = settings.getNeo4jUrl();
    String neo4jUsername = settings.getNeo4jUsername();
    String neo4jPassword = settings.getNeo4jPassword();
    String outDbUrl = settings.getOrientDbPath();
    String orientDbProtocol = settings.getOrientDbProtocol();
    boolean overwriteOrientDBDir = settings.getOverwriteOrientDbDir();
    boolean neo4jRelIdIndex = settings.getCreateIndexOnNeo4jRelID();

    String dbName;

    if (outDbUrl.contains("embedded") || outDbUrl.contains("plocal")) {
      outDbUrl = outDbUrl.replace("plocal", "embedded");
      dbName = outDbUrl.substring(outDbUrl.lastIndexOf('/') + 1);
    } else if (outDbUrl.contains("remote")) {
      dbName = outDbUrl.substring(outDbUrl.lastIndexOf('/') + 1);
    } else {
      // memory protocol
      dbName = outDbUrl.substring(outDbUrl.lastIndexOf('/') + 1);
    }

    if (ONeo4jImporterContext.getInstance().getOrientDBInstance().exists(dbName)) {
      if (overwriteOrientDBDir) {
        logString =
            "The '"
                + dbName
                + "' database already exists and the overwrite option is 'true'. The database will be erased before the new migration.";
        ONeo4jImporterContext.getInstance().getMessageHandler().warn(this, logString);
        ONeo4jImporterContext.getInstance().getOrientDBInstance().drop(dbName);
      } else {

        // we exit the program
        logString =
            "The '"
                + dbName
                + "' database already exists and the overwrite option is 'false' (default). "
                + "Please delete the database or run the migration with the 'overwrite' option set to true. Terminating the migration.\n\n";
        ONeo4jImporterContext.getInstance().getMessageHandler().error(this, logString);
        throw new RuntimeException(logString);
      }
    }

    try {

      //
      // PHASE 1 : INITIALIZATION
      //

      OSourceNeo4jInfo sourceNeo4jInfo =
          new OSourceNeo4jInfo(neo4jUrl, neo4jUsername, neo4jPassword);
      ONeo4jImporterInitializer initializer =
          new ONeo4jImporterInitializer(sourceNeo4jInfo, orientDbProtocol, dbName);
      Session neo4jSession = initializer.initConnections();
      String orientVertexClass = initializer.getOrientVertexClass();
      ODatabaseDocument oDb = initializer.getoDb();
      ONeo4jImporterStatistics statistics = ONeo4jImporterContext.getInstance().getStatistics();

      //
      // PHASE 2 : MIGRATION OF VERTICES AND EDGES
      //

      ONeo4jImporterVerticesAndEdgesMigrator verticesAndEdgesImporter =
          new ONeo4jImporterVerticesAndEdgesMigrator(
              keepLogString,
              migrateRels,
              migrateNodes,
              df,
              orientVertexClass,
              oDb,
              statistics,
              relSampleOnly,
              neo4jRelIdIndex);
      verticesAndEdgesImporter.invoke(neo4jSession);
      keepLogString = verticesAndEdgesImporter.getKeepLogString();

      //
      // PHASE 3 : SCHEMA MIGRATION
      //

      ONeo4jImporterSchemaMigrator schemaMigrator =
          new ONeo4jImporterSchemaMigrator(keepLogString, df, oDb, statistics);
      schemaMigrator.invoke(neo4jSession);

      //
      // PHASE 4 : SHUTDOWN OF THE SERVERS AND SUMMARY INFO
      //

      stopServers(neo4jSession, oDb);
      printSummary(
          startTime,
          df,
          dfd,
          statistics,
          initializer,
          verticesAndEdgesImporter,
          schemaMigrator,
          neo4jRelIdIndex);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    returnCode = 0;
    return returnCode;
  }

  private void stopServers(Session neo4jSession, ODatabaseDocument oDb) throws Exception {

    String logString;
    logString = "\nShutting down OrientDB connection...";
    ONeo4jImporterContext.getInstance().getMessageHandler().info(this, logString);
    oDb.close();
    ONeo4jImporterContext.getInstance()
        .getMessageHandler()
        .info(this, "\rShutting down OrientDB connection...Done\n");

    logString = "Shutting down Neo4j connection...";

    try {
      if (neo4jSession != null) {
        neo4jSession.close();
      }
    } catch (Exception e) {
      String mess = "";
      ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
      ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
      throw new RuntimeException(e.getMessage());
    }
    ONeo4jImporterContext.getInstance().getMessageHandler().info(this, logString);
    ONeo4jImporterContext.getInstance()
        .getMessageHandler()
        .info(this, "\rShutting down Neo4j connection...Done\n");
  }

  private void printSummary(
      long startTime,
      DecimalFormat df,
      DecimalFormat dfd,
      ONeo4jImporterStatistics counters,
      ONeo4jImporterInitializer initializer,
      ONeo4jImporterVerticesAndEdgesMigrator migrator,
      ONeo4jImporterSchemaMigrator schemaMigrator,
      boolean neo4jRelIdIndex) {

    double value;
    String logString;

    long stopTime = System.currentTimeMillis();
    long elapsedTime = (stopTime - startTime);
    long elapsedTimeSeconds = elapsedTime / (1000);

    long initializationElapsedTime =
        (initializer.getInitializationStopTime() - initializer.getInitializationStartTime());
    long initializationElapsedTimeSeconds = initializationElapsedTime / (1000);

    long importingNodesElapsedTime =
        counters.importingNodesStopTime - counters.importingNodesStartTime;
    long importingNodesElapsedTimeSeconds = importingNodesElapsedTime / (1000);

    long importingRelsElapsedTime =
        migrator.getImportingRelsStopTime() - migrator.getImportingRelsStartTime();
    long importingRelsElapsedTimeSeconds = importingRelsElapsedTime / (1000);

    long importingSchemaElapsedTime =
        schemaMigrator.getImportingSchemaStopTime() - schemaMigrator.getImportingSchemaStartTime();
    long importingSchemaElapsedTimeSeconds = importingSchemaElapsedTime / (1000);

    long internalVertexIndicesElapsedTime =
        counters.internalVertexIndicesStopTime - counters.internalVertexIndicesStartTime;
    long internalVertexIndicesElapsedTimeSeconds = internalVertexIndicesElapsedTime / (1000);

    long internalEdgeIndicesElapsedTime =
        counters.internalEdgeIndicesStopTime - counters.internalEdgeIndicesStartTime;
    long internalEdgeIndicesElapsedTimeSeconds = internalEdgeIndicesElapsedTime / (1000);

    double neo4jTotalInternalIndicesCounter =
        counters.neo4jInternalVertexIndicesCounter + counters.neo4jInternalEdgeIndicesCounter;

    String format = "%-100s %s";
    ONeo4jImporterContext.getInstance().getMessageHandler().info(this, "\n\n");
    ONeo4jImporterContext.getInstance().getMessageHandler().info(this, "===============");
    ONeo4jImporterContext.getInstance().getMessageHandler().info(this, "Import Summary:");
    ONeo4jImporterContext.getInstance().getMessageHandler().info(this, "===============");
    ONeo4jImporterContext.getInstance().getMessageHandler().info(this, "\n\n");
    ONeo4jImporterContext.getInstance()
        .getMessageHandler()
        .info(
            this,
            format,
            "- Found Neo4j Nodes",
            ": " + df.format(counters.neo4jNodeCounter) + "\n");
    ONeo4jImporterContext.getInstance()
        .getMessageHandler()
        .info(
            this,
            format,
            "-- With at least one Label",
            ": " + df.format(counters.neo4jNodeMultipleLabelsCounter) + "\n");
    ONeo4jImporterContext.getInstance()
        .getMessageHandler()
        .info(
            this,
            format,
            "-- With multiple Labels",
            ": " + df.format(counters.neo4jNodeMultipleLabelsCounter) + "\n");
    ONeo4jImporterContext.getInstance()
        .getMessageHandler()
        .info(
            this,
            format,
            "-- Without Labels",
            ": " + df.format(counters.neo4jNodeNoLabelCounter) + "\n");
    ONeo4jImporterContext.getInstance()
        .getMessageHandler()
        .info(
            this,
            format,
            "- Imported OrientDB Vertices",
            ": " + df.format(counters.orientDBImportedVerticesCounter));
    if (counters.neo4jNodeCounter > 0) {
      value = (counters.orientDBImportedVerticesCounter / counters.neo4jNodeCounter) * 100;
      ONeo4jImporterContext.getInstance()
          .getMessageHandler()
          .info(this, " (" + df.format(value) + "%)");
    }

    ONeo4jImporterContext.getInstance().getMessageHandler().info(this, "\n\n");
    ONeo4jImporterContext.getInstance()
        .getMessageHandler()
        .info(
            this,
            format,
            "- Found Neo4j Relationships",
            ": " + df.format(counters.neo4jRelCounter) + "\n");
    ONeo4jImporterContext.getInstance()
        .getMessageHandler()
        .info(
            this,
            format,
            "- Imported OrientDB Edges",
            ": " + df.format(counters.orientDBImportedEdgesCounter));
    if (counters.neo4jRelCounter > 0) {
      value = (counters.orientDBImportedEdgesCounter / counters.neo4jRelCounter) * 100;
      ONeo4jImporterContext.getInstance()
          .getMessageHandler()
          .info(this, " (" + df.format(value) + "%)");
    }

    ONeo4jImporterContext.getInstance().getMessageHandler().info(this, "\n\n");
    ONeo4jImporterContext.getInstance()
        .getMessageHandler()
        .info(
            this,
            format,
            "- Found Neo4j Constraints",
            ": " + df.format(counters.neo4jConstraintsCounter) + "\n");
    ONeo4jImporterContext.getInstance()
        .getMessageHandler()
        .info(
            this,
            format,
            "- Imported OrientDB Constraints (UNIQUE Indices created)",
            ": " + df.format(counters.orientDBImportedConstraintsCounter));
    if (counters.neo4jConstraintsCounter > 0) {
      value =
          (counters.orientDBImportedConstraintsCounter / counters.neo4jConstraintsCounter) * 100;
      ONeo4jImporterContext.getInstance()
          .getMessageHandler()
          .info(this, " (" + df.format(value) + "%)");
    }
    ONeo4jImporterContext.getInstance().getMessageHandler().info(this, "\n\n");
    ONeo4jImporterContext.getInstance()
        .getMessageHandler()
        .info(
            this,
            format,
            "- NOT UNIQUE Indices created due to failure in creating UNIQUE Indices",
            ": " + df.format(counters.orientDBImportedNotUniqueWorkaroundCounter));
    if (counters.neo4jConstraintsCounter > 0) {
      value =
          (counters.orientDBImportedNotUniqueWorkaroundCounter / counters.neo4jConstraintsCounter)
              * 100;
      ONeo4jImporterContext.getInstance()
          .getMessageHandler()
          .info(this, " (" + df.format(value) + "%)");
    }

    ONeo4jImporterContext.getInstance().getMessageHandler().info(this, "\n\n");
    ONeo4jImporterContext.getInstance()
        .getMessageHandler()
        .info(
            this,
            format,
            "- Found Neo4j (non-constraint) Indices",
            ": " + df.format(counters.neo4jNonConstraintsIndicesCounter) + "\n");
    ONeo4jImporterContext.getInstance()
        .getMessageHandler()
        .info(
            this,
            format,
            "- Imported OrientDB Indices",
            ": " + df.format(counters.orientDBImportedIndicesCounter));
    if (counters.neo4jNonConstraintsIndicesCounter > 0) {
      value =
          (counters.orientDBImportedIndicesCounter / counters.neo4jNonConstraintsIndicesCounter)
              * 100;
      ONeo4jImporterContext.getInstance()
          .getMessageHandler()
          .info(this, " (" + df.format(value) + "%)");
    }

    ONeo4jImporterContext.getInstance().getMessageHandler().info(this, "\n\n");

    // ONeo4jImporterContext.getInstance().getMessageHandler().info(this, "- Additional created
    // Indices (on vertex properties 'neo4jNodeID' & 'neo4jLabelList')          : "
    // + df.format(counters.neo4jInternalIndicesCounter));

    ONeo4jImporterContext.getInstance()
        .getMessageHandler()
        .info(
            this,
            format,
            "- Additional internal Indices created",
            ": " + df.format(neo4jTotalInternalIndicesCounter) + "\n");

    ONeo4jImporterContext.getInstance().getMessageHandler().info(this, "\n");
    ONeo4jImporterContext.getInstance()
        .getMessageHandler()
        .info(
            this,
            format,
            "- Total Import time:",
            ": " + OFunctionsHandler.getHMSFormat(elapsedTime) + "\n");

    ONeo4jImporterContext.getInstance()
        .getMessageHandler()
        .info(
            this,
            format,
            "-- Initialization time",
            ": " + OFunctionsHandler.getHMSFormat(initializationElapsedTime) + "\n");
    ONeo4jImporterContext.getInstance()
        .getMessageHandler()
        .info(
            this,
            format,
            "-- Time to Import Nodes",
            ": " + OFunctionsHandler.getHMSFormat(importingNodesElapsedTime));
    if (importingNodesElapsedTimeSeconds > 0) {
      value = (counters.orientDBImportedVerticesCounter / importingNodesElapsedTimeSeconds);
      ONeo4jImporterContext.getInstance()
          .getMessageHandler()
          .info(this, " (" + dfd.format(value) + " nodes/sec)");
    }

    ONeo4jImporterContext.getInstance().getMessageHandler().info(this, "\n");
    ONeo4jImporterContext.getInstance()
        .getMessageHandler()
        .info(
            this,
            format,
            "-- Time to Import Relationships",
            ": " + OFunctionsHandler.getHMSFormat(importingRelsElapsedTime));
    if (importingRelsElapsedTimeSeconds > 0) {
      value = (counters.orientDBImportedEdgesCounter / importingRelsElapsedTimeSeconds);
      ONeo4jImporterContext.getInstance()
          .getMessageHandler()
          .info(this, " (" + dfd.format(value) + " rels/sec)");
    }

    ONeo4jImporterContext.getInstance().getMessageHandler().info(this, "\n");
    ONeo4jImporterContext.getInstance()
        .getMessageHandler()
        .info(
            this,
            format,
            "-- Time to Import Constraints and Indices",
            ": " + OFunctionsHandler.getHMSFormat(importingSchemaElapsedTime));
    if (importingSchemaElapsedTimeSeconds > 0) {
      value =
          ((counters.orientDBImportedConstraintsCounter + counters.orientDBImportedIndicesCounter)
              / importingSchemaElapsedTimeSeconds);
      ONeo4jImporterContext.getInstance()
          .getMessageHandler()
          .info(this, " (" + dfd.format(value) + " indices/sec)");
    }

    ONeo4jImporterContext.getInstance().getMessageHandler().info(this, "\n");
    ONeo4jImporterContext.getInstance()
        .getMessageHandler()
        .info(
            this,
            format,
            "-- Time to Create Internal Indices (on vertex properties 'neo4jNodeID' & 'neo4jLabelList')",
            ": " + OFunctionsHandler.getHMSFormat(internalVertexIndicesElapsedTime));
    if (internalVertexIndicesElapsedTimeSeconds > 0) {
      value =
          (counters.neo4jInternalVertexIndicesCounter / internalVertexIndicesElapsedTimeSeconds);
      ONeo4jImporterContext.getInstance()
          .getMessageHandler()
          .info(this, " (" + dfd.format(value) + " indices/sec)");
    }

    if (neo4jRelIdIndex) {
      ONeo4jImporterContext.getInstance().getMessageHandler().info(this, "\n");
      ONeo4jImporterContext.getInstance()
          .getMessageHandler()
          .info(
              this,
              format,
              "-- Time to Create Internal Indices (on edge property 'neo4jRelID')",
              ": " + OFunctionsHandler.getHMSFormat(internalEdgeIndicesElapsedTime));
      if (internalEdgeIndicesElapsedTimeSeconds > 0) {
        value = (counters.neo4jInternalEdgeIndicesCounter / internalEdgeIndicesElapsedTimeSeconds);
        ONeo4jImporterContext.getInstance()
            .getMessageHandler()
            .info(this, " (" + dfd.format(value) + " indices/sec)");
      }
      ONeo4jImporterContext.getInstance().getMessageHandler().info(this, "\n");
    }

    ONeo4jImporterContext.getInstance().getMessageHandler().info(this, "\n");
    logString = PROGRAM_NAME + " - v." + OConstants.getVersion() + " - PHASE 4 completed!\n\n";
    ONeo4jImporterContext.getInstance().getMessageHandler().info(this, logString);
  }
}
