package com.orientechnologies.orient.neo4jimporter;

import com.orientechnologies.orient.context.ONeo4jImporterContext;
import com.orientechnologies.orient.context.ONeo4jImporterCounters;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.neo4j.driver.v1.*;

import java.text.DecimalFormat;
import java.util.*;
import java.util.logging.Level;

import static com.orientechnologies.orient.neo4jimporter.ONeo4jImporter.PROGRAM_NAME;

/**
 * Created by frank on 08/11/2016.
 */
class ONeo4jImporterVerticesAndEdgesMigrator {

  private final boolean                migrateRels;
  private final boolean                migrateNodes;
  private final boolean                relSampleOnly;
  private final boolean                neo4jRelIdIndex;
  private final DecimalFormat          df;
  private       String                 keepLogString;
  private       String                 orientVertexClass;
  private       OrientGraphNoTx        oDb;
  private       ONeo4jImporterCounters counters;
  private       double                 importingRelsStartTime;
  private       double                 importingRelsStopTime;

  public ONeo4jImporterVerticesAndEdgesMigrator(String keepLogString, boolean migrateRels, boolean migrateNodes, DecimalFormat df,
      String orientVertexClass, OrientGraphNoTx oDb, ONeo4jImporterCounters counters,
      boolean relSampleOnly, boolean neo4jRelIdIndex) {

    this.keepLogString = keepLogString;
    this.migrateRels = migrateRels;
    this.migrateNodes = migrateNodes;
    this.relSampleOnly = relSampleOnly;
    this.neo4jRelIdIndex = neo4jRelIdIndex;
    this.df = df;
    this.orientVertexClass = orientVertexClass;
    this.oDb = oDb;
    this.counters = counters;
  }

  public String getKeepLogString() {
    return keepLogString;
  }

  public double getImportingRelsStartTime() {
    return importingRelsStartTime;
  }

  public double getImportingRelsStopTime() {
    return importingRelsStopTime;
  }


  /**
   * Executes all the importing phases.
   * @return
   */

  public void invoke(Session neo4jSession) {

    String logString;

    try {


      /**
       * Importing nodes with all properties and labels into OrientDB
       */

      this.importNodesAndBuildIndices(neo4jSession);


      /**
       * Importing all relationships from Neo4j and creates the corresponding Edges in OrientDB
       */

      this.importRelationships(neo4jSession);


      /**
       * Building Indices on Edge classes:
       *      - an index on each OrientDB Edge class on property Neo4jRelID (it will help querying by original Neo4j Rel IDs)
       */

      this.buildIndicesOnRelationships();


    } catch (Exception e) {
      String mess = "";
      ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
      ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
    }

    logString = PROGRAM_NAME + " - v." + OConstants.getVersion() + " - PHASE 2 completed!\n";
    ONeo4jImporterContext.getInstance().getOutputManager().info(logString);
  }


  /**
   * Imports nodes and builds indices.
   * @param session
   */

  private void importNodesAndBuildIndices(Session session) {

    String logString;
    boolean hasMultipleLabels;
    double value;

    if (migrateNodes) {

      logString = "Getting all Nodes from Neo4j and creating corresponding Vertices in OrientDB...";

      ONeo4jImporterContext.getInstance().getOutputManager().info("\n");
      ONeo4jImporterContext.getInstance().getOutputManager().info(logString);

      ONeo4jImporterContext.getInstance().getOutputManager().info(logString);

      /**
       * Counting Neo4j Nodes so that we can show a % on OrientDB vertices creation
       */
      try {
        String query = "MATCH (node) RETURN count(node) as count";
        StatementResult result = session.run(query);
        Record record = result.next();
        counters.neo4jTotalNodes = record.get("count").asDouble();
      } catch (Exception e) {
        String mess = "";
        ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
        ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
      }

      counters.importingNodesStartTime = System.currentTimeMillis();

      try {
        String query = "MATCH (node) RETURN properties(node) as properties, ID(node) as id, labels(node) as labels";
        StatementResult result = session.run(query);

        while(result.hasNext()) {

          Record currentRecord = result.next();
          counters.neo4jNodeCounter++;
          List<Object> nodeLabels = currentRecord.get("labels").asList();

          int i = 0;
          for (Object myLabel : nodeLabels) {
            i++;
          }
          String[] multipleLabelsArray;
          multipleLabelsArray = new String[i];

          //determines the class to use in OrientDB, starting from the original Neo4j label. First thing we check if this node has multiple labels
          int numberOfLabels = 0;
          String multipleLabelClass = "MultipleLabelNeo4jConversion";
          for (Object myLabel : nodeLabels) {
            numberOfLabels++;
            if (numberOfLabels == 1) {
              orientVertexClass = (String) myLabel;
            }
            multipleLabelsArray[numberOfLabels - 1] = (String) myLabel;
          }

          if (numberOfLabels >= 2) {
            hasMultipleLabels = true;
            orientVertexClass = multipleLabelClass;
            counters.neo4jNodeMultipleLabelsCounter++;
            logString = "Found node ('" + currentRecord + "') with multiple labels. Only the first (" + orientVertexClass
                + ") will be used as Class when importing this node in OrientDB";
            ONeo4jImporterContext.getInstance().getOutputManager().info(logString);
          }

          // if numberOfLabels=0 the neo4j node has no label
          if (numberOfLabels == 0) {
            counters.neo4jNodeNoLabelCounter++;
            // set generic class for OrientDB
            orientVertexClass = "GenericClassNeo4jConversion";
            logString = "Found node ('" + currentRecord
                + "') with no labels. Class 'GenericClassNeo4jConversion' will be used when importing this node in OrientDB";
            ONeo4jImporterContext.getInstance().getOutputManager().info(logString);
          }

          //gets the node properties
          Map<String, Object> resultMap = currentRecord.get("properties").asMap();
          Map<String, Object> nodeProperties = new LinkedHashMap<String,Object>();
          nodeProperties.putAll(resultMap);

          //stores also the original neo4j nodeId in the property map - we will use it when creating the corresponding OrientDB vertex
          nodeProperties.put("Neo4jNodeID", currentRecord.get("id").asObject());

          //store also the original labels
          nodeProperties.put("Neo4jLabelList", multipleLabelsArray);

          try {
            // store the vertex on OrientDB
            Vertex myVertex = oDb.addVertex("class:" + orientVertexClass, nodeProperties);
            ONeo4jImporterContext.getInstance().getOutputManager().debug(myVertex.toString());

            counters.orientDBImportedVerticesCounter++;
            value = 100.0 * (counters.orientDBImportedVerticesCounter / counters.neo4jTotalNodes);
            keepLogString =
                df.format(counters.orientDBImportedVerticesCounter) + " OrientDB Vertices have been created (" + df.format(value)
                    + "% done)";
            ONeo4jImporterContext.getInstance().getOutputManager().info("\r  " + keepLogString);
            value = 0;
          } catch (Exception e) {
            String mess = "Found an error when trying to store node ('" + currentRecord + "') to OrientDB: " + e.getMessage();
            ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
            ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
          }
        }

        if (counters.orientDBImportedVerticesCounter == 0) {
          keepLogString = df.format(counters.orientDBImportedVerticesCounter) + " OrientDB Vertices have been created";
          ONeo4jImporterContext.getInstance().getOutputManager().info("\r  " + keepLogString);
        }

        ONeo4jImporterContext.getInstance().getOutputManager().info("\nDone\n");
      } catch (Exception e) {
        String mess = "";
        ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
        ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
      }

      counters.importingNodesStopTime = System.currentTimeMillis();

      /**
       * Building Indices on Vertex classes:
       *      - two indices on each OrientDB vertex class on properties Neo4jNodeID & Neo4jLabelList
       *      - index on Neo4jNodeID: it will help in speeding up vertices lookup during relationships creation
       *      - index on Neo4jLabelList: it will help querying by original Neo4j Label
       */

      counters.internalVertexIndicesStartTime = System.currentTimeMillis();
      logString = "Creating internal Indices on properties 'Neo4jNodeID' & 'Neo4jLabelList' on all OrientDB Vertices Classes...";

      ONeo4jImporterContext.getInstance().getOutputManager().info("\n");
      ONeo4jImporterContext.getInstance().getOutputManager().info(logString);

      ONeo4jImporterContext.getInstance().getOutputManager().info(logString);
      Collection<OClass> vertexClasses = oDb.getRawGraph().getMetadata().getSchema().getClass("V").getAllSubclasses();
      counters.orientDBVerticesClassCount = (double) vertexClasses.size();

      for (OClass currentClass : vertexClasses) {

        // index on property Neo4jNodeID
        try {

          //first create the property
          currentClass.createProperty("Neo4jNodeID", OType.LONG);

          //creates the index if the property creation was successful
          try {

            currentClass.getProperty("Neo4jNodeID").createIndex(OClass.INDEX_TYPE.UNIQUE_HASH_INDEX);
            counters.neo4jInternalVertexIndicesCounter++;

            value = 100.0 * (counters.neo4jInternalVertexIndicesCounter / (counters.orientDBVerticesClassCount * 2));
            keepLogString =
                df.format(counters.neo4jInternalVertexIndicesCounter) + " OrientDB Indices have been created (" + df.format(value)
                    + "% done)";
            ONeo4jImporterContext.getInstance().getOutputManager().info("\r  " + keepLogString);
            value = 0;
          } catch (Exception e) {
            String mess =
                "Found an error when trying to create a UNIQUE Index in OrientDB on the 'Neo4jNodeID' Property of the vertex Class '"
                    + currentClass.getName() + "': " + e.getMessage();
            ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
            ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
          }
        } catch (Exception e) {
          String mess = "Found an error when trying to create the 'Neo4jNodeID' Property in OrientDB on the vertex Class '"
              + currentClass.getName() + "': " + e.getMessage();
          ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
          ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
        }

        // index on property Neo4jLabelList
        try {

          //first create the property
          currentClass.createProperty("Neo4jLabelList", OType.EMBEDDEDLIST, OType.STRING);

          //creates the index if the property creation was successful
          try {

            currentClass.getProperty("Neo4jLabelList").createIndex(OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX);
            counters.neo4jInternalVertexIndicesCounter++;

            value = 100.0 * (counters.neo4jInternalVertexIndicesCounter / (counters.orientDBVerticesClassCount * 2));
            keepLogString =
                df.format(counters.neo4jInternalVertexIndicesCounter) + " OrientDB Indices have been created (" + df.format(value)
                    + "% done)";
            ONeo4jImporterContext.getInstance().getOutputManager().info("\r  " + keepLogString);
            value = 0;
          } catch (Exception e) {
            String mess =
                "Found an error when trying to create a NOT UNIQUE Index in OrientDB on the 'Neo4jLabelList' Property of the vertex Class '"
                    + currentClass.getName() + "': " + e.getMessage();
            ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
            ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");          }
        } catch (Exception e) {
          String mess = "Found an error when trying to create the 'Neo4jLabelList' Property in OrientDB on the vertex Class '"
              + currentClass.getName() + "': " + e.getMessage();
          ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
          ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
        }
      }

      counters.internalVertexIndicesStopTime = System.currentTimeMillis();

      if (counters.neo4jInternalVertexIndicesCounter == 0) {
        keepLogString = df.format(counters.neo4jInternalVertexIndicesCounter) + " OrientDB Indices have been created";
        ONeo4jImporterContext.getInstance().getOutputManager().info("\r  " + keepLogString);

      }

      ONeo4jImporterContext.getInstance().getOutputManager().info("\nDone\n");
    }
  }

  /**
   * Imports all the relationships present in the source neo4j graph database.
   * @param session
   */

  private void importRelationships(Session session) {

    String logString;
    double value;
    importingRelsStartTime = 0L;
    importingRelsStopTime = 0L;

    if (migrateRels) {

      logString = "Getting all Relationships from Neo4j and creating corresponding Edges in OrientDB...";

      ONeo4jImporterContext.getInstance().getOutputManager().info("\n");
      ONeo4jImporterContext.getInstance().getOutputManager().info(logString);

      //counting Neo4j Relationships so that we can show a % on OrientDB Edges creation
      try {
        String query = "MATCH ()-[r]->() RETURN count(r) as count";
        StatementResult result = session.run(query);
        Record record = result.next();
        counters.neo4jTotalRels = record.get("count").asDouble();
      } catch (Exception e) {
        String mess = "";
        ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
        ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
      }

      importingRelsStartTime = System.currentTimeMillis();

      try {

        String query = "MATCH (a)-[r]->(b) RETURN ID(a) as outVertexID, r as relationship, ID(b) as inVertexID, ID(r) as relationshipId, properties(r) as relationshipProps, type(r) as relationshipType";
        StatementResult result = session.run(query);

        while(result.hasNext()) {

          Record currentRecord = result.next();
          counters.neo4jRelCounter++;

          String currentRelationshipType = currentRecord.get("relationshipType").asString();
          ONeo4jImporterContext.getInstance().getOutputManager().debug("Current relationship type: " + currentRelationshipType);

          //get the relationship properties
          Map<String, Object> resultMap = currentRecord.get("relationshipProps").asMap();
          Map<String, Object> relationshipProperties = new LinkedHashMap<String,Object>();
          relationshipProperties.putAll(resultMap);

          //store also the original neo4j relationship id
          relationshipProperties.put("Neo4jRelID", currentRecord.get("relationshipId").asObject());

          ONeo4jImporterContext.getInstance().getOutputManager().debug("Neo:" + currentRecord.get("outVertexID") +"-"+ currentRelationshipType  +"->"+ currentRecord.get("inVertexID"));

          //lookup the corresponding outVertex in OrientDB
          Iterator<Vertex> it = oDb.getVertices("Neo4jNodeID", currentRecord.get("outVertexID")).iterator();
          Vertex outVertex = it.next();   // id in unique, thus the query contains just a vertex
          if(it.hasNext()) {
            throw new Exception("Out vertex lookup for the current relationship returned more than one vertex.");
          }

          //lookup the corresponding inVertex in OrientDB
          it = oDb.getVertices("Neo4jNodeID", currentRecord.get("inVertexID")).iterator();
          Vertex inVertex = it.next();
          if(it.hasNext()) {
            throw new Exception("In vertex lookup for the current relationship returned more than one vertex.");
          }

          //cast from Vertex to OrientVertex so that we can make use of more functionalities
          OrientVertex outOrientVertex = (OrientVertex) outVertex;
          OrientVertex inOrientVertex = (OrientVertex) inVertex;

          String orientEdgeClassName = currentRelationshipType;

          ONeo4jImporterContext.getInstance().getOutputManager().debug("\nOrientDb Edge class name: " + orientEdgeClassName);

          /*
           * In neo4j we can have labels on nodes and relationship with the same name, but in OrientDB we cannot have vertex and edges classes with the same name.
           * To handle this case, we append an E_ to the relationship name in case the relationship name is the same of a vertex class.
           */

          Collection<OClass> vertexClasses = oDb.getRawGraph().getMetadata().getSchema().getClass("V").getAllSubclasses();
          for (OClass currentClass: vertexClasses) {

            if (orientEdgeClassName.equalsIgnoreCase(currentClass.getName())) {
              //we have already a label on a vertex with the same name, changes the edge class by adding an "E_" prefix

              //prints just one warning per relationship type (fix for github issue #1)
              if (!oDb.getRawGraph().getMetadata().getSchema().existsClass("E_" + orientEdgeClassName)) {

                logString =
                    "Found a Neo4j Relationship Type ('" + orientEdgeClassName + "') with same name of a Neo4j node Label ('"
                        + currentClass.getName() + "'). Importing in OrientDB relationships of this type as 'E_"
                        + orientEdgeClassName;
                ONeo4jImporterContext.getInstance().getOutputManager().warn(logString);
              }
              orientEdgeClassName = "E_" + orientEdgeClassName;
            }
          }

          /**
           * Converting map relationshipProperties to an Object[], so that it can be passed to addEdge method below.
           * This will allow to create edges with a single create operation, instead of a create and update operation similar to the following:
           * OrientEdge myOrientEdge = oDb.addEdge("class:" + orientEdgeClass, myOutVertex, myInVertex, orientEdgeClass);
           * myOrientEdge.setProperties(myRelationshipProperties);
           */

          Object[] edgeProps = new Object[relationshipProperties.size() * 2];
          int i = 0;
          for (Map.Entry entry : relationshipProperties.entrySet()) {
            edgeProps[i++] = entry.getKey();
            edgeProps[i++] = entry.getValue();
          }

          try {
            OrientEdge currentEdge = outOrientVertex.addEdge(orientEdgeClassName, inOrientVertex, edgeProps);
            counters.orientDBImportedEdgesCounter++;

            ONeo4jImporterContext.getInstance().getOutputManager().debug("Orient:" + outOrientVertex.getProperty("@rid") +"-"+ currentRelationshipType  +"->"+ inOrientVertex.getProperty("@rid"));

            value = 100 * (counters.orientDBImportedEdgesCounter / counters.neo4jTotalRels);
            keepLogString =
                df.format(counters.orientDBImportedEdgesCounter) + " OrientDB Edges have been created (" + df.format(value)
                    + "% done)";
            ONeo4jImporterContext.getInstance().getOutputManager().info("\r  " + keepLogString);
            value = 0;
          } catch (Exception e) {
            String mess = "Found an error when trying to create an Edge in OrientDB. Corresponding Relationship in Neo4j is '"
                + currentRecord + "': " + e.getMessage();
            ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
            ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
          }
        }
      } catch (Exception e) {
        String mess = "";
        ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
        ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
      }

      importingRelsStopTime = System.currentTimeMillis();

      if (counters.orientDBImportedEdgesCounter == 0) {
        keepLogString = df.format(counters.orientDBImportedEdgesCounter) + " OrientDB Edges have been created";
        ONeo4jImporterContext.getInstance().getOutputManager().debug("\r  " + keepLogString);
      }

      ONeo4jImporterContext.getInstance().getOutputManager().info("\nDone\n");
    }
  }

  /**
   *
   */

  private void buildIndicesOnRelationships() {

    String logString;
    double value;

    if (neo4jRelIdIndex) {

      counters.internalEdgeIndicesStartTime = System.currentTimeMillis();

      if (migrateRels) {

        logString = "Creating internal Indices on properties 'Neo4jRelID' on all OrientDB Edge Classes...";
        ONeo4jImporterContext.getInstance().getOutputManager().info("\n");
        ONeo4jImporterContext.getInstance().getOutputManager().info(logString);

        Collection<OClass> edgeClasses = oDb.getRawGraph().getMetadata().getSchema().getClass("E").getAllSubclasses();
        counters.orientDBEdgeClassesCount = (double) edgeClasses.size();

        for (OClass currentEdgeClass : edgeClasses) {

          // index on property Neo4jRelID
          try {

            //first create the property
            currentEdgeClass.createProperty("Neo4jRelID", OType.LONG);

            //creates the index if the property creation was successful
            try {

              currentEdgeClass.getProperty("Neo4jRelID").createIndex(OClass.INDEX_TYPE.UNIQUE_HASH_INDEX);
              counters.neo4jInternalEdgeIndicesCounter++;

              value = 100.0 * (counters.neo4jInternalEdgeIndicesCounter / (counters.orientDBEdgeClassesCount));
              keepLogString =
                  df.format(counters.neo4jInternalEdgeIndicesCounter) + " OrientDB Indices have been created (" + df.format(value)
                      + "% done)";
              ONeo4jImporterContext.getInstance().getOutputManager().info("\r  " + keepLogString);
              value = 0;
            } catch (Exception e) {
              String mess =
                  "Found an error when trying to create a UNIQUE Index in OrientDB on the 'Neo4jRelID' Property of the edge Class '"
                      + currentEdgeClass.getName() + "': " + e.getMessage();
              ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
              ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
            }
          } catch (Exception e) {
            String mess = "Found an error when trying to create the 'Neo4jRelID' Property in OrientDB on the edge Class '" + currentEdgeClass.getName() + "': " + e.getMessage();
            ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
            ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
          }
        }

        counters.internalEdgeIndicesStopTime = System.currentTimeMillis();

        if (counters.neo4jInternalEdgeIndicesCounter == 0) {
          keepLogString = df.format(counters.neo4jInternalEdgeIndicesCounter) + " OrientDB Indices have been created";
          ONeo4jImporterContext.getInstance().getOutputManager().info("\r  " + keepLogString);
        }

        ONeo4jImporterContext.getInstance().getOutputManager().info("\nDone\n");
      }
    }
  }

}
