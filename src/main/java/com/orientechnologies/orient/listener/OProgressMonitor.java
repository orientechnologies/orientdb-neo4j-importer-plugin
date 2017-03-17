package com.orientechnologies.orient.listener;

import com.orientechnologies.orient.context.ONeo4jImporterContext;
import com.orientechnologies.orient.context.ONeo4jImporterStatistics;

import java.text.DecimalFormat;

/**
 * Created by gabriele on 16/03/17.
 */
public class OProgressMonitor implements OStatisticsListener {


  private DecimalFormat df;

  public OProgressMonitor() {
    this.df = new DecimalFormat("#");
  }


  /**
   * Called whenever the progress monitor needs to be updated.
   * that is whenever progress OTeleporterStatistics publishes an event.
   *
   * @param statistics
   */

  @Override
  public String updateOnEvent(ONeo4jImporterStatistics statistics) {

    String message = null;

    switch (statistics.importingElements) {
    case "vertices":
      message = this.updateImportedVerticesLog(statistics);
      break;
    case "edges":
      message = this.updateImportedEdgesLog(statistics);
      break;
    case "indices-on-vertices":
      message = this.updateImportedIndicesOnVertices(statistics);
      break;
    case "indices-on-edges":
      message = this.updateImportedIndicesOnEdges(statistics);
      break;
    case "constraints":
      message = this.updateImportedConstraints(statistics);
      break;
    case "indices":
      message = this.updateImportedIndices(statistics);
      break;
    default:
      break;
    }
    return message;
  }


  private String updateImportedVerticesLog(ONeo4jImporterStatistics statistics) {

    String keepLogString;

    if (statistics.orientDBImportedVerticesCounter == 0) {
      keepLogString = df.format(statistics.orientDBImportedVerticesCounter) + " OrientDB Vertices have been created";
      ONeo4jImporterContext.getInstance().getOutputManager().info("\r  " + keepLogString);
    }
    else {
      double value = 100.0 * (statistics.orientDBImportedVerticesCounter / statistics.neo4jTotalNodes);
      keepLogString = df.format(statistics.orientDBImportedVerticesCounter) + " OrientDB Vertices have been created (" + df.format(value) + "% done)";
      ONeo4jImporterContext.getInstance().getOutputManager().info("\r  " + keepLogString);
    }
    return keepLogString;
  }

  private String updateImportedIndicesOnVertices(ONeo4jImporterStatistics statistics) {

    String keepLogString;

    if (statistics.neo4jInternalVertexIndicesCounter == 0) {
      keepLogString = df.format(statistics.neo4jInternalVertexIndicesCounter) + " OrientDB Indices have been created";
      ONeo4jImporterContext.getInstance().getOutputManager().info("\r  " + keepLogString);
    }
    else {
      double value = 100.0 * (statistics.neo4jInternalVertexIndicesCounter / (statistics.orientDBVerticesClassCount * 2));
      keepLogString = df.format(statistics.neo4jInternalVertexIndicesCounter) + " OrientDB Indices have been created (" + df.format(value) + "% done)";
      ONeo4jImporterContext.getInstance().getOutputManager().info("\r  " + keepLogString);
    }
    return keepLogString;
  }


  private String updateImportedEdgesLog(ONeo4jImporterStatistics statistics) {

    String keepLogString;

    if (statistics.orientDBImportedEdgesCounter == 0) {
      keepLogString = df.format(statistics.orientDBImportedEdgesCounter) + " OrientDB Edges have been created";
      ONeo4jImporterContext.getInstance().getOutputManager().debug("\r  " + keepLogString);
    }
    else {
      double value = 100 * (statistics.orientDBImportedEdgesCounter / statistics.neo4jTotalRels);
      keepLogString =
          df.format(statistics.orientDBImportedEdgesCounter) + " OrientDB Edges have been created (" + df.format(value) + "% done)";
      ONeo4jImporterContext.getInstance().getOutputManager().info("\r  " + keepLogString);
    }
    return keepLogString;
  }


  private String updateImportedIndicesOnEdges(ONeo4jImporterStatistics statistics) {

    String keepLogString;

    if (statistics.neo4jInternalEdgeIndicesCounter == 0) {
      keepLogString = df.format(statistics.neo4jInternalEdgeIndicesCounter) + " OrientDB Indices have been created";
      ONeo4jImporterContext.getInstance().getOutputManager().info("\r  " + keepLogString);
    }
    else {
      double value = 100.0 * (statistics.neo4jInternalEdgeIndicesCounter / (statistics.orientDBEdgeClassesCount));
      keepLogString =
          df.format(statistics.neo4jInternalEdgeIndicesCounter) + " OrientDB Indices have been created (" + df.format(value) + "% done)";
      ONeo4jImporterContext.getInstance().getOutputManager().info("\r  " + keepLogString);
    }
    return keepLogString;
  }

  private String updateImportedConstraints(ONeo4jImporterStatistics statistics) {

    String keepLogString;

    double value = 100 * (statistics.neo4jConstraintsCounter / statistics.neo4jTotalConstraints);
    keepLogString = df.format(statistics.orientDBImportedUniqueConstraintsCounter) + " OrientDB UNIQUE Indices have been created";

    // TODO: discerning behaviour according to constraints' type (not present in old version too)
    keepLogString = keepLogString + " (" + df.format(value) + "% done)";
    ONeo4jImporterContext.getInstance().getOutputManager().info("\r  " + keepLogString);

    return keepLogString;
  }

  private String updateImportedIndices(ONeo4jImporterStatistics statistics) {

    String keepLogString;

    if (statistics.orientDBImportedIndicesCounter == 0) {
      keepLogString = df.format(statistics.orientDBImportedIndicesCounter) + " OrientDB Indices have been created";
      ONeo4jImporterContext.getInstance().getOutputManager().info("\r  " + keepLogString);
    }
    else {
      double value = 100 * (statistics.neo4jNonConstraintsIndicesCounter / (statistics.neo4jTotalIndices - statistics.neo4jTotalUniqueConstraints));
      keepLogString = df.format(statistics.orientDBImportedIndicesCounter) + " OrientDB Indices have been created (" + df.format(value) + "% done)";
      ONeo4jImporterContext.getInstance().getOutputManager().info("\r" + keepLogString);
    }
    return keepLogString;
  }


  public void initialize() {
    ONeo4jImporterContext.getInstance().getStatistics().registerListener(this);
  }

}
