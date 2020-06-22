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

package com.orientechnologies.orient.context;

import com.orientechnologies.orient.listener.OStatisticsListener;
import java.util.ArrayList;
import java.util.List;

/** Created by frank on 08/11/2016. */
public class ONeo4jImporterStatistics {

  public volatile String importingElements = "nothing";

  public volatile double neo4jNodeCounter = 0;
  public volatile double neo4jNodeNoLabelCounter = 0;
  public volatile double neo4jNodeMultipleLabelsCounter = 0;
  public volatile double orientDBImportedVerticesCounter = 0;
  public volatile double neo4jRelCounter = 0;
  public volatile double orientDBImportedEdgesCounter = 0;
  public volatile double neo4jConstraintsCounter = 0;
  public volatile double neo4jUniqueConstraintsCounter = 0;
  public volatile double orientDBImportedConstraintsCounter = 0;
  public volatile double orientDBImportedUniqueConstraintsCounter = 0;
  public volatile double orientDBImportedNotUniqueWorkaroundCounter = 0;
  public volatile double neo4jIndicesCounter = 0;
  public volatile double neo4jNonConstraintsIndicesCounter = 0;
  public volatile double neo4jInternalVertexIndicesCounter = 0;
  public volatile double neo4jInternalEdgeIndicesCounter = 0;
  public volatile double orientDBImportedIndicesCounter = 0;
  public volatile double neo4jTotalNodes = 0;
  public volatile double neo4jTotalRels = 0;
  public volatile double neo4jTotalUniqueConstraints = 0;
  public volatile double neo4jTotalConstraints = 0;
  public volatile double neo4jTotalNodePropertyExistenceConstraints = 0;
  public volatile double neo4jTotalRelPropertyExistenceConstraints = 0;
  public volatile double neo4jTotalIndices = 0;

  public volatile long importingNodesStartTime = 0;
  public volatile long importingNodesStopTime = 0;
  public volatile long internalVertexIndicesStartTime = 0;
  public volatile double orientDBVerticesClassCount = 0;
  public volatile long internalVertexIndicesStopTime = 0;
  public volatile long internalEdgeIndicesStartTime = 0;
  public volatile double orientDBEdgeClassesCount = 0;
  public volatile long internalEdgeIndicesStopTime = 0;

  // Listeners
  private volatile List<OStatisticsListener> listeners;

  public ONeo4jImporterStatistics() {
    this.listeners = new ArrayList<OStatisticsListener>();
  }

  /*
   * Publisher-Subscribers
   */

  public void registerListener(OStatisticsListener listener) {
    this.listeners.add(listener);
  }

  public void notifyListeners() {
    for (OStatisticsListener listener : this.listeners) {
      listener.updateOnEvent(this);
    }
  }
}
