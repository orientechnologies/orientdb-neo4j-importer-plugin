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

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.OConstants;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import org.neo4j.graphdb.GraphDatabaseService;

import java.io.File;
import java.text.DecimalFormat;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The main class of the ONeo4jImporter. It is instantiated from the ONeo4jImporterCommandLineParser
 *
 * @author Santo Leto
 */

public class ONeo4jImporter {

  public static final Logger importLogger = Logger.getLogger("OrientDB.Neo4j.Importer");
  public static final String PROGRAM_NAME = "Neo4j to OrientDB Importer";
  private final ONeo4jImporterSettings settings;

  public ONeo4jImporter(final ONeo4jImporterSettings settings) throws Exception {
    this.settings = settings;
  }

  public int execute() throws Exception {

    //
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
    //

    logString = PROGRAM_NAME + " - v." + OConstants.getVersion() + " started!\n";
    importLogger.log(Level.INFO, logString);

    // parameters (from command line)
    boolean overwriteOrientDBDir = settings.overwriteOrientDbDir;
    String neo4jLibPath = settings.neo4jLibPath; //actually unused right now - but important to start the program from the command line
    String neo4jDBPath = settings.neo4jDbPath;
    String orientDbFolder = settings.orientDbDir;
    //

    // check existance of orientDbFolder and takes action accordingly to option overwriteOrientDBDir
    final File f = new File(orientDbFolder);
    if (f.exists()) {
      if (overwriteOrientDBDir) {

        logString =
            "Directory '" + orientDbFolder + "' exists already and the overwrite option '-o' is 'true'. Directory will be erased";
        importLogger.log(Level.WARNING, logString);

        OFileUtils.deleteRecursively(f);

      } else {

        //we exit the program
        logString = "ERROR: The directory '" + orientDbFolder
            + "' exists and the overwrite option '-o' is 'false' (default). Please delete the directory or run the program with the '-o true' option. Exiting";

        System.out.print(logString);
        System.out.print("\n\n");

        importLogger.log(Level.SEVERE, logString);

        System.exit(1);

      }
    }
    //

    //
    // PHASE 1 : INITIALIZATION
    //

    ONeo4jImporterInitializer initializer = new ONeo4jImporterInitializer(neo4jDBPath, orientDbFolder).invoke();
    GraphDatabaseService neo4jGraphDb = initializer.getNeo4jGraphDb();
    String orientVertexClass = initializer.getOrientVertexClass();
    OrientGraphNoTx oDb = initializer.getoDb();
    OrientGraphFactory oFactory = initializer.getoFactory();
    ONeo4jImporterCounters counters = new ONeo4jImporterCounters();

    //
    // PHASE 2 : MIGRATION OF VERTICES AND EDGES
    //

    ONeo4jImporterVerticesAndEdgesMigrator verticesAndEngesImporter = new ONeo4jImporterVerticesAndEdgesMigrator(keepLogString,
        migrateRels, migrateNodes, df, neo4jGraphDb, orientVertexClass, oDb, counters, relSampleOnly).invoke();
    keepLogString = verticesAndEngesImporter.getKeepLogString();

    //
    // PHASE 3 : SCHEMA MIGRATION
    //

    ONeo4jImporterSchemaMigrator schemaMigrator = new ONeo4jImporterSchemaMigrator(keepLogString, df, neo4jGraphDb, oDb, counters)
        .invoke();

    //
    // PHASE 4 : SHUTDOWN OF THE SERVERS AND SUMMARY INFO
    //

    stopServers(neo4jGraphDb, oDb, oFactory);

    printSummary(startTime, df, dfd, counters, initializer, verticesAndEngesImporter, schemaMigrator);

    returnCode = 0;
    return returnCode;

  }

  private void stopServers(GraphDatabaseService neo4jGraphDb, OrientGraphNoTx oDb, OrientGraphFactory oFactory) {
    String logString;
    logString = "Shutting down OrientDB...";

    System.out.println();
    System.out.print(logString);

    importLogger.log(Level.INFO, logString);

    oDb.shutdown();
    oFactory.close();

    System.out.print("\rShutting down OrientDB...Done");

    //
    logString = "Shutting down Neo4j...";

    System.out.println();
    System.out.print(logString);

    importLogger.log(Level.INFO, logString);

    neo4jGraphDb.shutdown();

    System.out.print("\rShutting down Neo4j...Done");
    System.out.println();
    //
  }

  private void printSummary(double startTime, DecimalFormat df, DecimalFormat dfd, ONeo4jImporterCounters counters,
      ONeo4jImporterInitializer initializer, ONeo4jImporterVerticesAndEdgesMigrator migrator,
      ONeo4jImporterSchemaMigrator schemaMigrator) {
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

    double internalIndicesElapsedTime = counters.internalIndicesStopTime - counters.internalIndicesStartTime;
    double internalIndicesElapsedTimeSeconds = internalIndicesElapsedTime / (1000);

    //
    System.out.println();
    System.out.println("===============");
    System.out.println("Import Summary:");
    System.out.println("===============");
    System.out.println();
    System.out.println("- Found Neo4j Nodes                                                                           : " + df
        .format(counters.neo4jNodeCounter));
    System.out.println("-- With at least one Label                                                                    :  " + df
        .format((counters.neo4jNodeCounter - counters.neo4jNodeNoLabelCounter)));
    System.out.println("--- With multiple Labels                                                                      :   " + df
        .format(counters.neo4jNodeMultipleLabelsCounter));
    System.out.println("-- Without Labels                                                                             :  " + df
        .format(counters.neo4jNodeNoLabelCounter));
    System.out.print("- Imported OrientDB Vertices                                                                  : " + df
        .format(counters.orientDBImportedVerticesCounter));
    if (counters.neo4jNodeCounter > 0) {
      value = (counters.orientDBImportedVerticesCounter / counters.neo4jNodeCounter) * 100;
      System.out.print(" (" + df.format(value) + "%)");
      value = 0;
    }

    System.out.println();
    System.out.println();
    System.out.println("- Found Neo4j Relationships                                                                   : " + df
        .format(counters.neo4jRelCounter));
    System.out.print("- Imported OrientDB Edges                                                                     : " + df
        .format(counters.orientDBImportedEdgesCounter));
    if (counters.neo4jRelCounter > 0) {
      value = (counters.orientDBImportedEdgesCounter / counters.neo4jRelCounter) * 100;
      System.out.print(" (" + df.format(value) + "%)");
      value = 0;
    }

    System.out.println();
    System.out.println();
    System.out.println("- Found Neo4j Constraints                                                                     : " + df
        .format(counters.neo4jConstraintsCounter));
    System.out.print("- Imported OrientDB Constraints (UNIQUE Indices created)                                      : " + df
        .format(counters.orientDBImportedConstraintsCounter));
    if (counters.neo4jConstraintsCounter > 0) {
      value = (counters.orientDBImportedConstraintsCounter / counters.neo4jConstraintsCounter) * 100;
      System.out.print(" (" + df.format(value) + "%)");
      value = 0;
    }
    System.out.println();
    System.out.print("- NOT UNIQUE Indices created due to failure in creating UNIQUE Indices                        : " + df
        .format(counters.orientDBImportedNotUniqueWorkaroundCounter));
    if (counters.neo4jConstraintsCounter > 0) {
      value = (counters.orientDBImportedNotUniqueWorkaroundCounter / counters.neo4jConstraintsCounter) * 100;
      System.out.print(" (" + df.format(value) + "%)");
      value = 0;
    }

    System.out.println();
    System.out.println();
    System.out.println("- Found Neo4j (non-constraint) Indices                                                        : " + df
        .format(counters.neo4jNonConstraintsIndicesCounter));
    System.out.print("- Imported OrientDB Indices                                                                   : " + df
        .format(counters.orientDBImportedIndicesCounter));
    if (counters.neo4jNonConstraintsIndicesCounter > 0) {
      value = (counters.orientDBImportedIndicesCounter / counters.neo4jNonConstraintsIndicesCounter) * 100;
      System.out.print(" (" + df.format(value) + "%)");
      value = 0;
    }

    System.out.println();
    System.out.println();
    System.out.println("- Additional created Indices (on vertex properties 'Neo4jNodeID' & 'Neo4jLabelList')          : " + df
        .format(counters.neo4jInternalIndicesCounter));

    System.out.println();
    System.out.println("- Total Import time:                                                                          : " + df
        .format(elapsedTimeSeconds) + " seconds");

    System.out.println("-- Initialization time                                                                        :  " + df
        .format(initializationElapsedTimeSeconds) + " seconds");
    System.out.print("-- Time to Import Nodes                                                                       :  " + df
        .format(importingNodesElapsedTimeSeconds) + " seconds");
    if (importingNodesElapsedTimeSeconds > 0) {
      value = (counters.orientDBImportedVerticesCounter / importingNodesElapsedTimeSeconds);
      System.out.print(" (" + dfd.format(value) + " nodes/sec)");
      value = 0;
    }

    System.out.println();
    System.out.print("-- Time to Import Relationships                                                               :  " + df
        .format(importingRelsElapsedTimeSeconds) + " seconds");
    if (importingRelsElapsedTimeSeconds > 0) {
      value = (counters.orientDBImportedEdgesCounter / importingRelsElapsedTimeSeconds);
      System.out.print(" (" + dfd.format(value) + " rels/sec)");
      value = 0;
    }

    System.out.println();
    System.out.print("-- Time to Import Constraints and Indices                                                     :  " + df
        .format(importingSchemaElapsedTimeSeconds) + " seconds");
    if (importingSchemaElapsedTimeSeconds > 0) {
      value = ((counters.orientDBImportedConstraintsCounter + counters.orientDBImportedIndicesCounter)
          / importingSchemaElapsedTimeSeconds);
      System.out.print(" (" + dfd.format(value) + " indices/sec)");
      value = 0;
    }

    System.out.println();
    System.out.print("-- Time to create internal Indices (on vertex properties 'Neo4jNodeID' & 'Neo4jLabelList')    :  " + df
        .format(internalIndicesElapsedTimeSeconds) + " seconds");
    if (internalIndicesElapsedTimeSeconds > 0) {
      value = (counters.neo4jInternalIndicesCounter / internalIndicesElapsedTimeSeconds);
      System.out.print(" (" + dfd.format(value) + " indices/sec)");
      value = 0;
    }

    System.out.println("\n");
    //

    //
    logString = PROGRAM_NAME + " - v." + OConstants.getVersion() + " - PHASE 4 completed!\n";
    importLogger.log(Level.INFO, logString);

    logString = PROGRAM_NAME + " - v." + OConstants.getVersion() + " - Import completed!\n";
    importLogger.log(Level.INFO, logString);
    //
  }

}

	