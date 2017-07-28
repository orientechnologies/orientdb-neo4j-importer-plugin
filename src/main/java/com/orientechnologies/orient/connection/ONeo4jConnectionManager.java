package com.orientechnologies.orient.connection;

import com.orientechnologies.orient.context.ONeo4jImporterContext;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

import java.sql.SQLException;

/**
 * Created by gabriele on 14/03/17.
 */
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
      driver = GraphDatabase.driver(this.sourceNeo4jInfo.getNeo4jUrl(), AuthTokens.basic( this.sourceNeo4jInfo.getNeo4jUsername(), this.sourceNeo4jInfo.getNeo4jPassword()), noSSL);
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
      driver = GraphDatabase.driver(this.sourceNeo4jInfo.getNeo4jUrl(), AuthTokens.basic( this.sourceNeo4jInfo.getNeo4jUsername(), this.sourceNeo4jInfo.getNeo4jPassword()) );
      session = driver.session();
    } finally {
      if (session != null) {
        session.close();
      }
    }
  }

}
