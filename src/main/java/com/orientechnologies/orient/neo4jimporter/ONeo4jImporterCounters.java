package com.orientechnologies.orient.neo4jimporter;

/**
 * Created by frank on 08/11/2016.
 */
public class ONeo4jImporterCounters {

  double neo4jNodeCounter                           = 0;
  double neo4jNodeNoLabelCounter                    = 0;
  double neo4jNodeMultipleLabelsCounter             = 0;
  double orientDBImportedVerticesCounter            = 0;
  double neo4jRelCounter                            = 0;
  double orientDBImportedEdgesCounter               = 0;
  double neo4jConstraintsCounter                    = 0;
  double neo4jUniqueConstraintsCounter              = 0;
  double orientDBImportedConstraintsCounter         = 0;
  double orientDBImportedUniqueConstraintsCounter   = 0;
  double orientDBImportedNotUniqueWorkaroundCounter = 0;
  double neo4jIndicesCounter                        = 0;
  double neo4jNonConstraintsIndicesCounter          = 0;
  double neo4jInternalVertexIndicesCounter          = 0;
  double neo4jInternalEdgeIndicesCounter            = 0;
  double orientDBImportedIndicesCounter             = 0;
  double neo4jTotalNodes                            = 0;
  double neo4jTotalRels                             = 0;
  double neo4jTotalUniqueConstraints                = 0;
  double neo4jTotalConstraints                      = 0;
  double neo4jTotalNodePropertyExistenceConstraints = 0;
  double neo4jTotalRelPropertyExistenceConstraints  = 0;
  double neo4jTotalIndices                          = 0;

  double importingNodesStartTime        = 0;
  double importingNodesStopTime         = 0;
  double internalVertexIndicesStartTime = 0;
  double orientDBVerticesClassCount     = 0;
  double internalVertexIndicesStopTime  = 0;
  double internalEdgeIndicesStartTime   = 0;
  double orientDBEdgeClassesCount       = 0;
  double internalEdgeIndicesStopTime    = 0;
}
