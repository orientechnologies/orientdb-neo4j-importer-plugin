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

package com.orientechnologies.orient.connection;

import com.orientechnologies.orient.context.ONeo4jImporterContext;
import java.sql.SQLException;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

/** Created by gabriele on 14/03/17. */
public class ONeo4jConnectionManager {

  private OSourceNeo4jInfo sourceNeo4jInfo;

  public ONeo4jConnectionManager(OSourceNeo4jInfo sourceNeo4jInfo) {
    this.sourceNeo4jInfo = sourceNeo4jInfo;
  }

  /**
   * It returns a new connection to neo4j server.
   *
   * @throws SQLException
   */
  public Session getSession() throws Exception {

    org.neo4j.driver.v1.Driver driver = null;
    Session session = null;

    try {
      Config noSSL = Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig();
      driver =
          GraphDatabase.driver(
              this.sourceNeo4jInfo.getNeo4jUrl(),
              AuthTokens.basic(
                  this.sourceNeo4jInfo.getNeo4jUsername(), this.sourceNeo4jInfo.getNeo4jPassword()),
              noSSL);
      session = driver.session();
    } catch (Exception e) {
      String mess = "";
      ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
      ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
      throw new RuntimeException(e.getMessage());
    }
    return session;
  }

  /**
   * It checks the connection to neo4j server.
   *
   * @throws SQLException
   */
  public void checkConnection() throws Exception {

    org.neo4j.driver.v1.Driver driver = null;
    Session session = null;

    try {
      driver =
          GraphDatabase.driver(
              this.sourceNeo4jInfo.getNeo4jUrl(),
              AuthTokens.basic(
                  this.sourceNeo4jInfo.getNeo4jUsername(),
                  this.sourceNeo4jInfo.getNeo4jPassword()));
      session = driver.session();
    } finally {
      if (session != null) {
        session.close();
      }
    }
  }
}
