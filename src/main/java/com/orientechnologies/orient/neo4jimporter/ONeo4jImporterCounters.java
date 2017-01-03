package com.orientechnologies.orient.neo4jimporter;

/**
 * Created by frank on 08/11/2016.
 */
public class ONeo4jImporterCounters {

  double neo4jNodeCounter                           = 0L;
  double neo4jNodeNoLabelCounter                    = 0L;
  double neo4jNodeMultipleLabelsCounter             = 0L;
  double orientDBImportedVerticesCounter            = 0L;
  double neo4jRelCounter                            = 0L;
  double orientDBImportedEdgesCounter               = 0L;
  double neo4jConstraintsCounter                    = 0L;
  double neo4jUniqueConstraintsCounter              = 0L;
  double orientDBImportedConstraintsCounter         = 0L;
  double orientDBImportedUniqueConstraintsCounter   = 0L;
  double orientDBImportedNotUniqueWorkaroundCounter = 0L;
  double neo4jIndicesCounter                        = 0L;
  double neo4jNonConstraintsIndicesCounter          = 0L;
  double neo4jInternalIndicesCounter                = 0L;
  double orientDBImportedIndicesCounter             = 0L;
  double neo4jTotalNodes                            = 0L;
  double neo4jTotalRels                             = 0L;
  double neo4jTotalUniqueConstraints                = 0L;
  double neo4jTotalConstraints                      = 0L;
  double neo4jTotalNodePropertyExistenceConstraints = 0L;
  double neo4jTotalRelPropertyExistenceConstraints  = 0L;
  double neo4jTotalIndices                          = 0L;

  double importingNodesStartTime    = 0L;
  double importingNodesStopTime     = 0L;
  double internalIndicesStartTime   = 0L;
  double orientDBVerticesClassCount = 0L;
  double internalIndicesStopTime    = 0L;

}
