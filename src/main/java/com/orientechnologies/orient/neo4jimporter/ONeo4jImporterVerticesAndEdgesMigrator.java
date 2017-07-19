package com.orientechnologies.orient.neo4jimporter;

import com.orientechnologies.orient.context.ONeo4jImporterContext;
import com.orientechnologies.orient.context.ONeo4jImporterStatistics;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.sun.jdi.*;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.neo4j.driver.internal.value.BooleanValue;
import org.neo4j.driver.internal.value.FloatValue;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.Neo4jException;

import java.text.DecimalFormat;
import java.util.*;

import static com.orientechnologies.orient.neo4jimporter.ONeo4jImporter.PROGRAM_NAME;

/**
 * Created by frank on 08/11/2016.
 */
class ONeo4jImporterVerticesAndEdgesMigrator {

  private final boolean                  migrateRels;
  private final boolean                  migrateNodes;
  private final boolean                  relSampleOnly;
  private final boolean                  neo4jRelIdIndex;
  private final DecimalFormat            df;
  private       String                   keepLogString;
  private       String                   orientVertexClass;
  private       OrientGraphNoTx          oDb;
  private       ONeo4jImporterStatistics statistics;
  private       long                     importingRelsStartTime;
  private       long                     importingRelsStopTime;

  public ONeo4jImporterVerticesAndEdgesMigrator(String keepLogString, boolean migrateRels, boolean migrateNodes, DecimalFormat df,
      String orientVertexClass, OrientGraphNoTx oDb, ONeo4jImporterStatistics statistics,
      boolean relSampleOnly, boolean neo4jRelIdIndex) {

    this.keepLogString = keepLogString;
    this.migrateRels = migrateRels;
    this.migrateNodes = migrateNodes;
    this.relSampleOnly = relSampleOnly;
    this.neo4jRelIdIndex = neo4jRelIdIndex;
    this.df = df;
    this.orientVertexClass = orientVertexClass;
    this.oDb = oDb;
    this.statistics = statistics;
  }

  public String getKeepLogString() {
    return keepLogString;
  }

  public long getImportingRelsStartTime() {
    return importingRelsStartTime;
  }

  public long getImportingRelsStopTime() {
    return importingRelsStopTime;
  }


  /**
   * Executes all the importing phases.
   * @return
   */

  public void invoke(Session neo4jSession) throws Exception {

    String logString;

    try {


      /**
       * Importing nodes with all properties and labels into OrientDB
       */
      this.statistics.importingElements = "vertices";
      this.importVertices(neo4jSession);
      ONeo4jImporterContext.getInstance().getStatistics().notifyListeners();
      ONeo4jImporterContext.getInstance().getOutputManager().info("\nDone\n\n");
      this.statistics.importingElements = "nothing";


      /**
       * Building Indices on Vertex classes:
       *      - an index on each OrientDB Vertex class on property neo4jNodeID and neo4jLabelList
       */
      this.statistics.importingElements = "indices-on-vertices";
      this.importIndicesOnVertices();
      ONeo4jImporterContext.getInstance().getStatistics().notifyListeners();
      ONeo4jImporterContext.getInstance().getOutputManager().info("\nDone\n\n");
      this.statistics.importingElements = "nothing";


      /**
       * Importing all relationships from Neo4j and creates the corresponding Edges in OrientDB
       */
      this.statistics.importingElements = "edges";
      this.importEdges(neo4jSession);
      ONeo4jImporterContext.getInstance().getStatistics().notifyListeners();
      ONeo4jImporterContext.getInstance().getOutputManager().info("\nDone\n\n");
      this.statistics.importingElements = "nothing";


      /**
       * Building Indices on Edge classes:
       *      - an index on each OrientDB Edge class on property neo4jRelID (it will help querying by original Neo4j Rel IDs)
       */
      this.statistics.importingElements = "indices-on-edges";
      this.buildIndicesOnEdges();
      ONeo4jImporterContext.getInstance().getStatistics().notifyListeners();
      ONeo4jImporterContext.getInstance().getOutputManager().info("\nDone\n\n");
      this.statistics.importingElements = "nothing";

    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    logString = PROGRAM_NAME + " - v." + OConstants.ORIENT_VERSION + " - PHASE 2 completed!\n\n";
    ONeo4jImporterContext.getInstance().getOutputManager().info(logString);
  }


  /**
   * Imports nodes and builds indices.
   * @param session
   */

  private void importVertices(Session session) throws Exception {

    String logString;
    boolean hasMultipleLabels;
    double value;

    if (migrateNodes) {

      logString = "Getting all Nodes from Neo4j and creating corresponding Vertices in OrientDB...\n";
      ONeo4jImporterContext.getInstance().getOutputManager().info(logString);


      /**
       * Counting Neo4j Nodes so that we can show a % on OrientDB vertices creation
       */
      try {
        String query = "MATCH (node) RETURN count(node) as count";
        StatementResult result = session.run(query);
        Record record = result.next();
        statistics.neo4jTotalNodes = record.get("count").asDouble();
      } catch (Neo4jException e) {
        String mess = "";
        ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
        ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
        throw new RuntimeException(e);
      } catch (Exception e) {
        String mess = "";
        ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
        ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
      }

      statistics.importingNodesStartTime = System.currentTimeMillis();

      try {
        String query = "MATCH (node) RETURN properties(node) as properties, ID(node) as id, labels(node) as labels";
        StatementResult result = session.run(query);

        while(result.hasNext()) {

          Record currentRecord = result.next();
          statistics.neo4jNodeCounter++;
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
            statistics.neo4jNodeMultipleLabelsCounter++;
            logString = "Found node ('" + currentRecord + "') with multiple labels. Only the first (" + orientVertexClass
                + ") will be used as Class when importing this node in OrientDB";
            ONeo4jImporterContext.getInstance().getOutputManager().debug(logString);
          }

          // if numberOfLabels=0 the neo4j node has no label
          if (numberOfLabels == 0) {
            statistics.neo4jNodeNoLabelCounter++;
            // set generic class for OrientDB
            orientVertexClass = "GenericClassNeo4jConversion";
            logString = "Found node ('" + currentRecord
                + "') with no labels. Class 'GenericClassNeo4jConversion' will be used when importing this node in OrientDB";
            ONeo4jImporterContext.getInstance().getOutputManager().debug(logString);
          }

          //gets the node properties
          Map<String, Object> resultMap = currentRecord.get("properties").asMap();
          Map<String, Object> nodeProperties = new LinkedHashMap<String,Object>();
//          nodeProperties.putAll(resultMap);


          Value properties = currentRecord.get("properties");
          for(String property: properties.keys()) {
            Value currValue = properties.get(property);
            Object convertedValue = this.convertValuetypeFromNeo4jToJava(currValue);
            nodeProperties.put(property, convertedValue);
          }

          //stores also the original neo4j nodeId in the property map - we will use it when creating the corresponding OrientDB vertex
          nodeProperties.put("neo4jNodeID", currentRecord.get("id").asObject());

          //store also the original labels
          nodeProperties.put("neo4jLabelList", multipleLabelsArray);

          try {
            // store the vertex on OrientDB
            Vertex myVertex = oDb.addVertex("class:" + orientVertexClass, nodeProperties);
            ONeo4jImporterContext.getInstance().getOutputManager().debug(myVertex.toString());
            statistics.orientDBImportedVerticesCounter++;
          } catch (Exception e) {
            String mess = "Found an error when trying to store node ('" + currentRecord + "') to OrientDB: " + e.getMessage();
            ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
            ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
          }
        }
      } catch (Neo4jException e) {
        String mess = "";
        ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
        ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
        throw new RuntimeException(e);
      } catch (Exception e) {
        String mess = "";
        ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
        ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
      }

      statistics.importingNodesStopTime = System.currentTimeMillis();

    }
  }

  private Object convertValuetypeFromNeo4jToJava(Value myPropertyValue) {

    Object convertedValue = null;

    if (null == myPropertyValue || myPropertyValue instanceof StringValue) {
      convertedValue = myPropertyValue.asString();
    } else if (myPropertyValue instanceof IntegerValue) {
      convertedValue = myPropertyValue.asInt();
    } else if (myPropertyValue instanceof LongValue) {
      convertedValue = myPropertyValue.asLong();
    } else if (myPropertyValue instanceof BooleanValue) {
      convertedValue = myPropertyValue.asBoolean();
    } else if (myPropertyValue instanceof ByteValue) {
      convertedValue = myPropertyValue.asByteArray();
    } else if (myPropertyValue instanceof FloatValue) {
      convertedValue = myPropertyValue.asFloat();
    } else if (myPropertyValue instanceof DoubleValue) {
      convertedValue = myPropertyValue.asDouble();
    } else if (myPropertyValue instanceof CharValue) {
      convertedValue = myPropertyValue.asString();
    } else if (myPropertyValue instanceof ShortValue) {
      convertedValue = myPropertyValue.asInt();
    } else {
      convertedValue = myPropertyValue.asString();
    }

    return convertedValue;
  }


  private void importIndicesOnVertices() {

    String logString;
    double value;

    /**
     * Building Indices on Vertex classes:
     *      - two indices on each OrientDB vertex class on properties neo4jNodeID & neo4jLabelList
     *      - index on neo4jNodeID: it will help in speeding up vertices lookup during relationships creation
     *      - index on neo4jLabelList: it will help querying by original Neo4j Label
     */

    statistics.internalVertexIndicesStartTime = System.currentTimeMillis();
    logString = "Creating internal Indices on properties 'neo4jNodeID' & 'neo4jLabelList' on all OrientDB Vertices Classes...\n";
    ONeo4jImporterContext.getInstance().getOutputManager().info(logString);

    Collection<OClass> vertexClasses = oDb.getRawGraph().getMetadata().getSchema().getClass("V").getAllSubclasses();
    statistics.orientDBVerticesClassCount = (double) vertexClasses.size();

    for (OClass currentClass : vertexClasses) {

      // index on property neo4jNodeID
      try {

        //first create the property
        currentClass.createProperty("neo4jNodeID", OType.LONG);

        //creates the index if the property creation was successful
        try {

          currentClass.getProperty("neo4jNodeID").createIndex(OClass.INDEX_TYPE.UNIQUE_HASH_INDEX);
          statistics.neo4jInternalVertexIndicesCounter++;
        } catch (Exception e) {
          String mess =
              "Found an error when trying to create a UNIQUE Index in OrientDB on the 'neo4jNodeID' Property of the vertex Class '"
                  + currentClass.getName() + "': " + e.getMessage();
          ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
          ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
        }
      } catch (Exception e) {
        String mess = "Found an error when trying to create the 'neo4jNodeID' Property in OrientDB on the vertex Class '"
            + currentClass.getName() + "': " + e.getMessage();
        ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
        ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
      }

      // index on property neo4jLabelList
      try {

        //first create the property
        currentClass.createProperty("neo4jLabelList", OType.EMBEDDEDLIST, OType.STRING);

        //creates the index if the property creation was successful
        try {

          currentClass.getProperty("neo4jLabelList").createIndex(OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX);
          statistics.neo4jInternalVertexIndicesCounter++;
        } catch (Exception e) {
          String mess =
              "Found an error when trying to create a NOT UNIQUE Index in OrientDB on the 'neo4jLabelList' Property of the vertex Class '"
                  + currentClass.getName() + "': " + e.getMessage();
          ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
          ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");          }
      } catch (Exception e) {
        String mess = "Found an error when trying to create the 'neo4jLabelList' Property in OrientDB on the vertex Class '"
            + currentClass.getName() + "': " + e.getMessage();
        ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
        ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
      }
    }

    statistics.internalVertexIndicesStopTime = System.currentTimeMillis();
  }

  /**
   * Imports all the relationships present in the source neo4j graph database.
   * @param session
   */

  private void importEdges(Session session) throws Exception {

    String logString;
    double value;
    importingRelsStartTime = 0L;
    importingRelsStopTime = 0L;

    if (migrateRels) {

      logString = "Getting all Relationships from Neo4j and creating corresponding Edges in OrientDB...\n";

      ONeo4jImporterContext.getInstance().getOutputManager().info(logString);

      //counting Neo4j Relationships so that we can show a % on OrientDB Edges creation
      try {
        String query = "MATCH ()-[r]->() RETURN count(r) as count";
        StatementResult result = session.run(query);
        Record record = result.next();
        statistics.neo4jTotalRels = record.get("count").asDouble();
      } catch (Neo4jException e) {
        String mess = "";
        ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
        ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
        throw new RuntimeException(e);
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
          statistics.neo4jRelCounter++;

          String currentRelationshipType = currentRecord.get("relationshipType").asString();
          ONeo4jImporterContext.getInstance().getOutputManager().debug("Current relationship type: " + currentRelationshipType);

          //get the relationship properties
          Map<String, Object> resultMap = currentRecord.get("relationshipProps").asMap();
          Map<String, Object> relationshipProperties = new LinkedHashMap<String,Object>();
          relationshipProperties.putAll(resultMap);

          //store also the original neo4j relationship id
          relationshipProperties.put("neo4jRelID", currentRecord.get("relationshipId").asObject());

          ONeo4jImporterContext.getInstance().getOutputManager().debug("Neo:" + currentRecord.get("outVertexID") +"-"+ currentRelationshipType  +"->"+ currentRecord.get("inVertexID"));

          //lookup the corresponding outVertex in OrientDB
          Iterator<Vertex> it = oDb.getVertices("neo4jNodeID", currentRecord.get("outVertexID")).iterator();
          Vertex outVertex = it.next();   // id in unique, thus the query contains just a vertex
          if(it.hasNext()) {
            throw new Exception("Out vertex lookup for the current relationship returned more than one vertex.");
          }

          //lookup the corresponding inVertex in OrientDB
          it = oDb.getVertices("neo4jNodeID", currentRecord.get("inVertexID")).iterator();
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
            statistics.orientDBImportedEdgesCounter++;

            ONeo4jImporterContext.getInstance().getOutputManager().debug("Orient:" + outOrientVertex.getProperty("@rid") +"-"+ currentRelationshipType  +"->"+ inOrientVertex.getProperty("@rid"));
          } catch (Exception e) {
            String mess = "Found an error when trying to create an Edge in OrientDB. Corresponding Relationship in Neo4j is '"
                + currentRecord + "': " + e.getMessage();
            ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
            ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
          }
        }
      } catch (Neo4jException e) {
        String mess = "";
        ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
        ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
        throw new RuntimeException(e);
      } catch (Exception e) {
        String mess = "";
        ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
        ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
      }
      importingRelsStopTime = System.currentTimeMillis();
    }
  }

  /**
   *
   */

  private void buildIndicesOnEdges() {

    String logString;
    double value;

    if (neo4jRelIdIndex) {

      statistics.internalEdgeIndicesStartTime = System.currentTimeMillis();

      if (migrateRels) {

        logString = "Creating internal Indices on properties 'neo4jRelID' on all OrientDB Edge Classes...\n";
        ONeo4jImporterContext.getInstance().getOutputManager().info(logString);

        Collection<OClass> edgeClasses = oDb.getRawGraph().getMetadata().getSchema().getClass("E").getAllSubclasses();
        statistics.orientDBEdgeClassesCount = (double) edgeClasses.size();

        for (OClass currentEdgeClass : edgeClasses) {

          // index on property neo4jRelID
          try {

            //first create the property
            currentEdgeClass.createProperty("neo4jRelID", OType.LONG);

            //creates the index if the property creation was successful
            try {
              currentEdgeClass.getProperty("neo4jRelID").createIndex(OClass.INDEX_TYPE.UNIQUE_HASH_INDEX);
              statistics.neo4jInternalEdgeIndicesCounter++;
            } catch (Exception e) {
              String mess =
                  "Found an error when trying to create a UNIQUE Index in OrientDB on the 'neo4jRelID' Property of the edge Class '"
                      + currentEdgeClass.getName() + "': " + e.getMessage();
              ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
              ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
            }
          } catch (Exception e) {
            String mess = "Found an error when trying to create the 'neo4jRelID' Property in OrientDB on the edge Class '" + currentEdgeClass.getName() + "': " + e.getMessage();
            ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
            ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
          }
        }

        statistics.internalEdgeIndicesStopTime = System.currentTimeMillis();
      }
    }
  }

}
