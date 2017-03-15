package com.orientechnologies.orient.connection;

/**
 * Created by gabriele on 14/03/17.
 */
public class OSourceNeo4jInfo {

  private String neo4jUrl;
  private String neo4jUsername;
  private String neo4jPassword;

  public OSourceNeo4jInfo(String neo4jUrl, String neo4jUsername, String neo4jPassword) {
    this.neo4jUrl = neo4jUrl;
    this.neo4jUsername = neo4jUsername;
    this.neo4jPassword = neo4jPassword;
  }

  public String getNeo4jUrl() {
    return this.neo4jUrl;
  }

  public void setNeo4jUrl(String neo4jUrl) {
    this.neo4jUrl = neo4jUrl;
  }

  public String getNeo4jUsername() {
    return this.neo4jUsername;
  }

  public void setNeo4jUsername(String neo4jUsername) {
    this.neo4jUsername = neo4jUsername;
  }

  public String getNeo4jPassword() {
    return this.neo4jPassword;
  }

  public void setNeo4jPassword(String neo4jPassword) {
    this.neo4jPassword = neo4jPassword;
  }
}
