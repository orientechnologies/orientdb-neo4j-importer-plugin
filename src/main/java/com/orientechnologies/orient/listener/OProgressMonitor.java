package com.orientechnologies.orient.listener;

import com.orientechnologies.orient.context.ONeo4jImporterContext;
import com.orientechnologies.orient.context.ONeo4jImporterStatistics;

import java.text.DecimalFormat;

/**
 * Created by gabriele on 16/03/17.
 */
public class OProgressMonitor implements OStatisticsListener {


  private DecimalFormat df;
  private String format;

  public OProgressMonitor() {
    this.df = new DecimalFormat("#");
    this.format =  "\r%s";
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
      keepLogString = "Added OrientDB Vertices: " + df.format(statistics.orientDBImportedVerticesCounter) + " (0% done)";
      String message = String.format(format, keepLogString);
      ONeo4jImporterContext.getInstance().getMessageHandler().info(message);
    }
    else {
      double value = 100.0 * (statistics.orientDBImportedVerticesCounter / statistics.neo4jTotalNodes);
      keepLogString = "Added OrientDB Vertices: " + df.format(statistics.orientDBImportedVerticesCounter) + " (" + df.format(value) + "% done)";
      String message = String.format(format, keepLogString);
      ONeo4jImporterContext.getInstance().getMessageHandler().info(message);
    }
    return keepLogString;
  }

  private String updateImportedIndicesOnVertices(ONeo4jImporterStatistics statistics) {

    String keepLogString;

    if (statistics.neo4jInternalVertexIndicesCounter == 0) {
      keepLogString = "Built OrientDB Indices: " + df.format(statistics.neo4jInternalVertexIndicesCounter) + " (0% done)";
      String message = String.format(format, keepLogString);
      ONeo4jImporterContext.getInstance().getMessageHandler().info(message);
    }
    else {
      double value = 100.0 * (statistics.neo4jInternalVertexIndicesCounter / (statistics.orientDBVerticesClassCount * 2));
      keepLogString = "Built OrientDB Indices: " + df.format(statistics.neo4jInternalVertexIndicesCounter) + " (" + df.format(value) + "% done)";
      String message = String.format(format, keepLogString);
      ONeo4jImporterContext.getInstance().getMessageHandler().info(message);
    }
    return keepLogString;
  }


  private String updateImportedEdgesLog(ONeo4jImporterStatistics statistics) {

    String keepLogString;

    if (statistics.orientDBImportedEdgesCounter == 0) {
      keepLogString = "Added OrientDB Edges: " + df.format(statistics.orientDBImportedEdgesCounter) + " (0% done)";
      String message = String.format(format, keepLogString);
      ONeo4jImporterContext.getInstance().getMessageHandler().info(message);
    }
    else {
      double value = 100 * (statistics.orientDBImportedEdgesCounter / statistics.neo4jTotalRels);
      keepLogString = "Added OrientDB Edges: " + df.format(statistics.orientDBImportedEdgesCounter) + " (" + df.format(value) + "% done)";
      String message = String.format(format, keepLogString);
      ONeo4jImporterContext.getInstance().getMessageHandler().info(message);
    }
    return keepLogString;
  }


  private String updateImportedIndicesOnEdges(ONeo4jImporterStatistics statistics) {

    String keepLogString;

    if (statistics.neo4jInternalEdgeIndicesCounter == 0) {
      keepLogString =  "Built OrientDB Indices: " + df.format(statistics.neo4jInternalEdgeIndicesCounter) + " (0% done)";
      String message = String.format(format, keepLogString);
      ONeo4jImporterContext.getInstance().getMessageHandler().info(message);
    }
    else {
      double value = 100.0 * (statistics.neo4jInternalEdgeIndicesCounter / (statistics.orientDBEdgeClassesCount));
      keepLogString = "Built OrientDB Indices: " + df.format(statistics.neo4jInternalEdgeIndicesCounter) + " (" + df.format(value) + "% done)";
      String message = String.format(format, keepLogString);
      ONeo4jImporterContext.getInstance().getMessageHandler().info(message);
    }
    return keepLogString;
  }

  private String updateImportedConstraints(ONeo4jImporterStatistics statistics) {

    String keepLogString;

    if(statistics.neo4jConstraintsCounter == 0) {
      keepLogString = "Built OrientDB UNIQUE Indices: " + df.format(statistics.orientDBImportedUniqueConstraintsCounter);
      if(statistics.neo4jTotalConstraints == 0) {
        keepLogString += " (100% done)";
      }
      else {
        keepLogString += " (0% done)";
      }
      String message = String.format(format, keepLogString);
      ONeo4jImporterContext.getInstance().getMessageHandler().info(message);
    }
    else {
      double value = 100 * (statistics.neo4jConstraintsCounter / statistics.neo4jTotalConstraints);
      keepLogString = "Built OrientDB UNIQUE Indices: " + df.format(statistics.orientDBImportedUniqueConstraintsCounter) + " (" + df.format(value) + "% done)";

      // TODO: discerning behaviour according to constraints' type (not present in old version too)

      String message = String.format(format, keepLogString);
      ONeo4jImporterContext.getInstance().getMessageHandler().info(message);
    }

    return keepLogString;
  }

  private String updateImportedIndices(ONeo4jImporterStatistics statistics) {

    String keepLogString;

    if (statistics.orientDBImportedIndicesCounter == 0) {
      keepLogString = "Built OrientDB Indices: " + df.format(statistics.orientDBImportedIndicesCounter);
      if((statistics.neo4jTotalIndices - statistics.neo4jTotalUniqueConstraints) == 0) {
        keepLogString += " (100% done)";
      }
      else {
        keepLogString += " (0% done)";

      }
      String message = String.format(format, keepLogString);
      ONeo4jImporterContext.getInstance().getMessageHandler().info(message);
    }
    else {
      double value = 100 * (statistics.neo4jNonConstraintsIndicesCounter / (statistics.neo4jTotalIndices - statistics.neo4jTotalUniqueConstraints));
      keepLogString = "Built OrientDB Indices: " + df.format(statistics.orientDBImportedIndicesCounter) + " (" + df.format(value) + "% done)";
      String message = String.format(format, keepLogString);
      ONeo4jImporterContext.getInstance().getMessageHandler().info(message);
    }
    return keepLogString;
  }


  public void initialize() {
    ONeo4jImporterContext.getInstance().getStatistics().registerListener(this);
  }

}
