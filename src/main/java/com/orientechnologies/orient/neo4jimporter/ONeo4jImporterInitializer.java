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

import static com.orientechnologies.orient.neo4jimporter.ONeo4jImporter.PROGRAM_NAME;

import com.orientechnologies.orient.connection.ONeo4jConnectionManager;
import com.orientechnologies.orient.connection.OSourceNeo4jInfo;
import com.orientechnologies.orient.context.ONeo4jImporterContext;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import org.neo4j.driver.v1.Session;

/** Created by frank on 08/11/2016. */
class ONeo4jImporterInitializer {

  private final String orientDbName;
  private final String orientDbProtocol;
  private long initializationStartTime;
  private ODatabaseDocument oDb;
  private String orientVertexClass;
  private long initializationStopTime;

  private OSourceNeo4jInfo sourceNeo4jInfo;
  private Session neo4jSession;

  public ONeo4jImporterInitializer(
      OSourceNeo4jInfo sourceNeo4jInfo, String orientDbProtocol, String orientDbName) {
    this.orientDbName = orientDbName;
    this.orientDbProtocol = orientDbProtocol;
    this.sourceNeo4jInfo = sourceNeo4jInfo;
  }

  public OSourceNeo4jInfo getSourceNeo4jInfo() {
    return sourceNeo4jInfo;
  }

  public void setSourceNeo4jInfo(OSourceNeo4jInfo sourceNeo4jInfo) {
    this.sourceNeo4jInfo = sourceNeo4jInfo;
  }

  public long getInitializationStartTime() {
    return initializationStartTime;
  }

  public ODatabaseDocument getoDb() {
    return this.oDb;
  }

  public String getOrientVertexClass() {
    return orientVertexClass;
  }

  public long getInitializationStopTime() {
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

    this.initializationStartTime = System.currentTimeMillis();

    ONeo4jImporterContext.getInstance().getMessageHandler().info(this, "\n\n");
    ONeo4jImporterContext.getInstance()
        .getMessageHandler()
        .info(this, "Trying connection to Neo4j...");

    ONeo4jConnectionManager connectionManager = new ONeo4jConnectionManager(this.sourceNeo4jInfo);
    Session neo4jSession = connectionManager.getSession();

    logString = "Trying connection to Neo4j...Neo4j server is alive and connection succeeded.";

    ONeo4jImporterContext.getInstance().getMessageHandler().info(this, "\r" + logString);

    ONeo4jImporterContext.getInstance().getMessageHandler().info(this, "\n");
    ONeo4jImporterContext.getInstance().getMessageHandler().info(this, "Initializing OrientDB...");

    // creating orientdb graph database
    switch (this.orientDbProtocol) {
      case "embedded":
        ONeo4jImporterContext.getInstance()
            .getOrientDBInstance()
            .execute(
                "create database "
                    + this.orientDbName
                    + " plocal users ( admin identified by 'admin' role admin)");
        break;
      case "plocal":
        ONeo4jImporterContext.getInstance()
            .getOrientDBInstance()
            .execute(
                "create database "
                    + this.orientDbName
                    + " plocal users ( admin identified by 'admin' role admin)");
        break;
      case "memory":
        ONeo4jImporterContext.getInstance()
            .getOrientDBInstance()
            .execute(
                "create database "
                    + this.orientDbName
                    + " memory users ( admin identified by 'admin' role admin)");
        break;
      case "remote":
        String message =
            "Cannot create a new database in remote. Try to create a new empty database and restart the migration.\nThe current job will be aborted.";
        throw new RuntimeException(message);
      default:
        message = "Protocol not correct. Migration will be aborted.";
        throw new RuntimeException(message);
    }

    // acquiring connection to the just created database
    oDb =
        ONeo4jImporterContext.getInstance()
            .getOrientDBInstance()
            .open(this.orientDbName, "admin", "admin");

    this.orientVertexClass = "";

    logString = "Initializing OrientDB...Done\n";

    ONeo4jImporterContext.getInstance().getMessageHandler().info(this, "\r" + logString + "\n");

    ONeo4jImporterContext.getInstance()
        .getMessageHandler()
        .info(this, "Importing Neo4j database from server: ");
    ONeo4jImporterContext.getInstance()
        .getMessageHandler()
        .info(this, "  '" + this.sourceNeo4jInfo.getNeo4jUrl() + "' ");
    ONeo4jImporterContext.getInstance().getMessageHandler().info(this, "into OrientDB database:");
    ONeo4jImporterContext.getInstance()
        .getMessageHandler()
        .info(this, "  '" + orientDbName + "'\n");

    logString = PROGRAM_NAME + " - v." + OConstants.getVersion() + " - PHASE 1 completed!\n\n";
    ONeo4jImporterContext.getInstance().getMessageHandler().info(this, logString);

    this.initializationStopTime = System.currentTimeMillis();
    return neo4jSession;
  }
}
