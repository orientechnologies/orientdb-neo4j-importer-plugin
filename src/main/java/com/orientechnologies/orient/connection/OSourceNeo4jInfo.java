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
