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

import com.orientechnologies.orient.connection.ONeo4jConnectionManager;
import com.orientechnologies.orient.connection.OSourceNeo4jInfo;
import com.orientechnologies.orient.context.ONeo4jImporterContext;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import org.neo4j.driver.v1.Session;

import static com.orientechnologies.orient.neo4jimporter.ONeo4jImporter.PROGRAM_NAME;

/**
 * Created by frank on 08/11/2016.
 */
class ONeo4jImporterInitializer {

  private final String             orientDbFolder;
  private       String             dbName;
  private final String             orientDbProtocol;
  private       long               initializationStartTime;
  private       OrientGraphFactory oFactory;
  private       OrientBaseGraph    oDb;
  private       String             orientVertexClass;
  private       long               initializationStopTime;

  private OSourceNeo4jInfo sourceNeo4jInfo;
  private Session neo4jSession;

  public ONeo4jImporterInitializer(OSourceNeo4jInfo sourceNeo4jInfo, String orientDbProtocol, String orientDbFolder, String dbName) {
    this.orientDbFolder = orientDbFolder;
    this.orientDbProtocol = orientDbProtocol;
    this.sourceNeo4jInfo = sourceNeo4jInfo;
    this.dbName = dbName;
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

  public OrientGraphFactory getOFactory() {
    return oFactory;
  }

  public OrientBaseGraph getoDb() {
    return oDb;
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

  public Session initConnections(OrientTransactionality rule) throws Exception {
    String logString;

    this.initializationStartTime = System.currentTimeMillis();

    ONeo4jImporterContext.getInstance().getMessageHandler().info(this, "\n\n");
    ONeo4jImporterContext.getInstance().getMessageHandler().info(this, "Trying connection to Neo4j...");

    ONeo4jConnectionManager connectionManager = new ONeo4jConnectionManager(this.sourceNeo4jInfo);
    Session neo4jSession = connectionManager.getSession();

    logString = "Trying connection to Neo4j...Neo4j server is alive and connection succeeded.";

    ONeo4jImporterContext.getInstance().getMessageHandler().info(this, "\r" + logString);

    ONeo4jImporterContext.getInstance().getMessageHandler().info(this, "\n");
    ONeo4jImporterContext.getInstance().getMessageHandler().info(this, "Initializing OrientDB...");

    String dbUrl = this.orientDbProtocol + ":" + this.orientDbFolder;

    this.oFactory = new OrientGraphFactory(dbUrl, "admin", "admin");
    this.oFactory.declareIntent(new OIntentMassiveInsert());

    if(rule.equals(OrientTransactionality.TX)) {
      this.oDb = oFactory.getTx();
    }
    else if(rule.equals(OrientTransactionality.NoTX)) {
      this.oDb = oFactory.getNoTx();
    }

    this.oDb.setStandardElementConstraints(false);
    this.oDb.setAutoStartTx(false);

    this.orientVertexClass = "";

    logString = "Initializing OrientDB...Done\n";

    ONeo4jImporterContext.getInstance().getMessageHandler().info(this, "\r" + logString + "\n");

    ONeo4jImporterContext.getInstance().getMessageHandler().info(this, "Importing Neo4j database from server: ");
    ONeo4jImporterContext.getInstance().getMessageHandler().info(this, "  '" + this.sourceNeo4jInfo.getNeo4jUrl() + "' ");
    ONeo4jImporterContext.getInstance().getMessageHandler().info(this, "into OrientDB database:");
    ONeo4jImporterContext.getInstance().getMessageHandler().info(this, "  '" + this.dbName + "' (Path: " + this.orientDbFolder + ")\n");

    logString = PROGRAM_NAME + " - v." + OConstants.ORIENT_VERSION + " - PHASE 1 completed!\n\n";
    ONeo4jImporterContext.getInstance().getMessageHandler().info(this, logString);

    this.initializationStopTime = System.currentTimeMillis();
    return neo4jSession;
  }

}
