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

/**
 * OrientDB's Neo4j Importer.
 *
 * @author Santo Leto
 */
 
public class ONeo4jImporterSettings {
  public String                             Neo4jLibPath;
  public String                             Neo4jDbPath;  
  public String                             OrientDbDir;
  public boolean                            overwriteOrientDbDir = false;  
}
