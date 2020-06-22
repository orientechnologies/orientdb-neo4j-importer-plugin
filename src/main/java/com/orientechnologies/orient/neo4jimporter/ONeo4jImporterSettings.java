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

/**
 * OrientDB's Neo4j Importer - Settings Class
 *
 * @author Santo Leto
 */
public class ONeo4jImporterSettings {

  private String neo4jUrl;
  private String neo4jUsername;
  private String neo4jPassword;
  private String orientDbPath;
  private String orientDbProtocol;
  private boolean overwriteOrientDbDir = false;
  private boolean createIndexOnNeo4jRelID = false;

  private int logLevel;

  public ONeo4jImporterSettings() {}

  public ONeo4jImporterSettings(
      String neo4jUrl,
      String neo4jUsername,
      String neo4jPassword,
      String orientDbPath,
      String orientDbProtocol,
      boolean overwriteOrientDbDir,
      boolean createIndexOnNeo4jRelID) {

    this.neo4jUrl = neo4jUrl;
    this.neo4jUsername = neo4jUsername;
    this.neo4jPassword = neo4jPassword;
    this.orientDbPath = orientDbPath;
    this.orientDbProtocol = orientDbProtocol;
    this.overwriteOrientDbDir = overwriteOrientDbDir;
    this.createIndexOnNeo4jRelID = createIndexOnNeo4jRelID;
  }

  public String getNeo4jUrl() {
    return neo4jUrl;
  }

  public void setNeo4jUrl(String neo4jUrl) {
    this.neo4jUrl = neo4jUrl;
  }

  public String getNeo4jUsername() {
    return neo4jUsername;
  }

  public void setNeo4jUsername(String neo4jUsername) {
    this.neo4jUsername = neo4jUsername;
  }

  public String getNeo4jPassword() {
    return neo4jPassword;
  }

  public void setNeo4jPassword(String neo4jPassword) {
    this.neo4jPassword = neo4jPassword;
  }

  public String getOrientDbPath() {
    return orientDbPath;
  }

  public void setOrientDbPath(String orientDbPath) {
    this.orientDbPath = orientDbPath;
  }

  public String getOrientDbProtocol() {
    return orientDbProtocol;
  }

  public void setOrientDbProtocol(String orientDbProtocol) {
    this.orientDbProtocol = orientDbProtocol;
  }

  public boolean getOverwriteOrientDbDir() {
    return overwriteOrientDbDir;
  }

  public void setOverwriteOrientDbDir(boolean overwriteOrientDbDir) {
    this.overwriteOrientDbDir = overwriteOrientDbDir;
  }

  public boolean getCreateIndexOnNeo4jRelID() {
    return createIndexOnNeo4jRelID;
  }

  public void setCreateIndexOnNeo4jRelID(boolean createIndexOnNeo4jRelID) {
    this.createIndexOnNeo4jRelID = createIndexOnNeo4jRelID;
  }

  public int getLogLevel() {
    return this.logLevel;
  }

  public void setLogLevel(int logLevel) {
    this.logLevel = logLevel;
  }
}
