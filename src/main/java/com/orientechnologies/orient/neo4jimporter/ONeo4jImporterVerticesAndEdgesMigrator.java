package com.orientechnologies.orient.neo4jimporter;

import com.orientechnologies.orient.context.ONeo4jImporterContext;
import com.orientechnologies.orient.context.ONeo4jImporterStatistics;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.util.OGraphCommands;
import org.neo4j.driver.internal.value.*;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.Neo4jException;
import org.neo4j.driver.v1.types.Type;
import org.neo4j.driver.v1.exceptions.value.LossyCoercion;

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
  private       String                   keepLogString;
  private       String                   orientVertexClass;
  private       ODatabaseDocument        oDb;
  private       ONeo4jImporterStatistics statistics;
  private       long                     importingRelsStartTime;
  private       long                     importingRelsStopTime;
  private final int VERTICES_BATCH_SIZE = 1000;
  private final int EDGES_BATCH_SIZE = 300;


  public ONeo4jImporterVerticesAndEdgesMigrator(String keepLogString, boolean migrateRels, boolean migrateNodes, DecimalFormat df,
      String orientVertexClass, ODatabaseDocument oDb, ONeo4jImporterStatistics statistics,
      boolean relSampleOnly, boolean neo4jRelIdIndex) {

    this.keepLogString = keepLogString;
    this.migrateRels = migrateRels;
    this.migrateNodes = migrateNodes;
    this.relSampleOnly = relSampleOnly;
    this.neo4jRelIdIndex = neo4jRelIdIndex;
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
      ONeo4jImporterContext.getInstance().getMessageHandler().info("\nDone\n\n");
      this.statistics.importingElements = "nothing";


      /**
       * Building Indices on Vertex classes:
       *      - an index on each OrientDB Vertex class on property neo4jNodeID and neo4jLabelList
       */
      this.statistics.importingElements = "indices-on-vertices";
      this.importIndicesOnVertices();
      ONeo4jImporterContext.getInstance().getStatistics().notifyListeners();
      ONeo4jImporterContext.getInstance().getMessageHandler().info("\nDone\n\n");
      this.statistics.importingElements = "nothing";


      /**
       * Importing all relationships from Neo4j and creates the corresponding Edges in OrientDB
       */
      this.statistics.importingElements = "edges";
      this.importEdges(neo4jSession);
      ONeo4jImporterContext.getInstance().getStatistics().notifyListeners();
      ONeo4jImporterContext.getInstance().getMessageHandler().info("\nDone\n\n");
      this.statistics.importingElements = "nothing";


      /**
       * Building Indices on Edge classes:
       *      - an index on each OrientDB Edge class on property neo4jRelID (it will help querying by original Neo4j Rel IDs)
       */
      this.statistics.importingElements = "indices-on-edges";
      this.buildIndicesOnEdges();
      ONeo4jImporterContext.getInstance().getStatistics().notifyListeners();
      ONeo4jImporterContext.getInstance().getMessageHandler().info("\nDone\n\n");
      this.statistics.importingElements = "nothing";

    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    logString = PROGRAM_NAME + " - v." + OConstants.ORIENT_VERSION + " - PHASE 2 completed!\n\n";
    ONeo4jImporterContext.getInstance().getMessageHandler().info(logString);
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
      ONeo4jImporterContext.getInstance().getMessageHandler().info(logString);


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

        oDb.begin();
        int cont = 1;
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
            ONeo4jImporterContext.getInstance().getMessageHandler().debug(logString);
          }

          // if numberOfLabels=0 the neo4j node has no label
          if (numberOfLabels == 0) {
            statistics.neo4jNodeNoLabelCounter++;
            // set generic class for OrientDB
            orientVertexClass = "GenericClassNeo4jConversion";
            logString = "Found node ('" + currentRecord
                + "') with no labels. Class 'GenericClassNeo4jConversion' will be used when importing this node in OrientDB";
            ONeo4jImporterContext.getInstance().getMessageHandler().debug(logString);
          }

          //gets the node properties
          Map<String, Object> nodeProperties = new LinkedHashMap<String,Object>();

          Value properties = currentRecord.get("properties");
          for(String property: properties.keys()) {
            Value currValue = properties.get(property);
            Object convertedValue = this.convertValueTypeFromNeo4jToJava(currValue);
            nodeProperties.put(property, convertedValue);
          }

          //stores also the original neo4j nodeId in the property map - we will use it when creating the corresponding OrientDB vertex
          nodeProperties.put("neo4jNodeID", currentRecord.get("id").asLong());    // neo4jNodeID always stored as a Long

          //store also the original labels
          nodeProperties.put("neo4jLabelList", multipleLabelsArray);

          try {

            // store the vertex on OrientDB
            OVertex myVertex = this.addVertexToGraph(oDb, orientVertexClass, nodeProperties);
            ONeo4jImporterContext.getInstance().getMessageHandler().debug(myVertex.toString());
            statistics.orientDBImportedVerticesCounter++;

            if(cont % VERTICES_BATCH_SIZE == 0) {
              oDb.commit();
              oDb.getLocalCache().clear();
              oDb.begin();
            }
            cont++;
          } catch (Exception e) {
            oDb.rollback();
            String mess = "Found an error when trying to store node ('" + currentRecord + "') to OrientDB: " + e.getMessage();
            ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
            ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
          }
        }

        // committing last batch
        oDb.commit();
        oDb.getLocalCache().clear();

      } catch (Neo4jException e) {
        oDb.rollback();
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

  private Object convertValueTypeFromNeo4jToJava(Value myPropertyValue) {

    Object convertedValue = null;

    if (null == myPropertyValue || myPropertyValue instanceof StringValue) {
      convertedValue = myPropertyValue.asString();
    } else if (myPropertyValue instanceof IntegerValue) {
      try {
        convertedValue = myPropertyValue.asInt();
      } catch (LossyCoercion e) {
        // DO NOTHING: we accept losing precision
      }
    } else if (myPropertyValue instanceof BooleanValue) {
      convertedValue = myPropertyValue.asBoolean();
    } else if (myPropertyValue instanceof BytesValue) {
      convertedValue = myPropertyValue.asByteArray();
    } else if (myPropertyValue instanceof FloatValue) {
      try {
        convertedValue = myPropertyValue.asFloat();
      } catch (LossyCoercion e) {
        // DO NOTHING: we accept losing precision
      }
    } else if(myPropertyValue instanceof ListValue) {
      convertedValue = myPropertyValue.asList();
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
    ONeo4jImporterContext.getInstance().getMessageHandler().info(logString);

    Collection<OClass> vertexClasses = oDb.getMetadata().getSchema().getClass("V").getAllSubclasses();
    statistics.orientDBVerticesClassCount = (double) vertexClasses.size();

    for (OClass currentClass : vertexClasses) {

      // index on property neo4jNodeID
      try {

        //first create the property
        currentClass.createProperty("neo4jNodeID", OType.LONG);   // neo4jNodeID always stored as a Long

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
          ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
        }
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

      ONeo4jImporterContext.getInstance().getMessageHandler().info(logString);

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

        String query = "MATCH (a)-[r]->(b) RETURN ID(a) as outVertexID, r as relationship, ID(b) as inVertexID, ID(r) as relationshipId, "
            + "labels(a) as outVertexLabels, labels(b) as inVertexLabels, properties(r) as relationshipProps, type(r) as relationshipType";
        StatementResult result = session.run(query);

        oDb.begin();
        int cont = 1;

        while(result.hasNext()) {

          Record currentRecord = result.next();
          statistics.neo4jRelCounter++;

          String currentRelationshipType = currentRecord.get("relationshipType").asString();
          ONeo4jImporterContext.getInstance().getMessageHandler().debug("Current relationship type: " + currentRelationshipType);

          //get the relationship properties
          Map<String, Object> resultMap = currentRecord.get("relationshipProps").asMap();
          Map<String, Object> relationshipProperties = new LinkedHashMap<String,Object>();
          relationshipProperties.putAll(resultMap);

          //store also the original neo4j relationship id
          relationshipProperties.put("neo4jRelID", currentRecord.get("relationshipId").asLong());

          ONeo4jImporterContext.getInstance().getMessageHandler().debug("Neo:" + currentRecord.get("outVertexID") +"-"+ currentRelationshipType  +"->"+ currentRecord.get("inVertexID"));

          //lookup the corresponding outVertex in OrientDB
          List<Object> outVertexLabels = currentRecord.get("outVertexLabels").asList();
          String outVertexClass;
          if(outVertexLabels.size() > 1) {
            outVertexClass = "MultipleLabelNeo4jConversion";
          }
          else {
            outVertexClass = (String) outVertexLabels.get(0);
          }
          String[] propertyOfKey = {"neo4jNodeID"};
          Object[] valueOfKey = new Object[] {currentRecord.get("outVertexID").asObject()};
//          OResultSet vertices = OGraphCommands.getVertices(oDb, outVertexClass, propertyOfKey, valueOfKey);   // we can optimize the lookup by specifying the vertex class !!!
//          if(!vertices.hasNext()) {
//            throw new Exception("Out vertex lookup for the current relationship did not return any vertex.");
//          }
//          OVertex outVertex = vertices.next().getVertex().orElse(null);   // id in unique, thus the query contains just a vertex
//          if(vertices.hasNext()) {
//            throw new Exception("Out vertex lookup for the current relationship returned more than one vertex.");
//          }
//          vertices.close();
          OVertex outVertex = OGraphCommands.getVertex(oDb, outVertexClass, propertyOfKey[0], valueOfKey[0]);

          //lookup the corresponding inVertex in OrientDB
          List<Object> inVertexLabels = currentRecord.get("inVertexLabels").asList();
          String inVertexClass;
          if(inVertexLabels.size() > 1) {
            inVertexClass = "MultipleLabelNeo4jConversion";
          }
          else {
            inVertexClass = (String) inVertexLabels.get(0);
          }
          valueOfKey[0] = currentRecord.get("inVertexID").asObject();
//          vertices = OGraphCommands.getVertices(oDb, inVertexClass, propertyOfKey, valueOfKey);   // we can optimize the lookup by specifying the vertex class !!!
//          if(!vertices.hasNext()) {
//            throw new Exception("In vertex lookup for the current relationship did not return any vertex.");
//          }
//          OVertex inVertex = vertices.next().getVertex().orElse(null);
//          if(vertices.hasNext()) {
//            throw new Exception("In vertex lookup for the current relationship returned more than one vertex.");
//          }
//          vertices.close();
          OVertex inVertex = OGraphCommands.getVertex(oDb, outVertexClass, propertyOfKey[0], valueOfKey[0]);

          String orientEdgeClassName = currentRelationshipType;

          ONeo4jImporterContext.getInstance().getMessageHandler().debug("\nOrientDb Edge class name: " + orientEdgeClassName);

          /*
           * In neo4j we can have labels on nodes and relationship with the same name, but in OrientDB we cannot have vertex and edges classes with the same name.
           * To handle this case, we append an E_ to the relationship name in case the relationship name is the same of a vertex class.
           */

          Collection<OClass> vertexClasses = oDb.getMetadata().getSchema().getClass("V").getAllSubclasses();
          for (OClass currentClass: vertexClasses) {

            if (orientEdgeClassName.equalsIgnoreCase(currentClass.getName())) {
              //we have already a label on a vertex with the same name, changes the edge class by adding an "E_" prefix

              //prints just one warning per relationship type (fix for github issue #1)
              if (!oDb.getMetadata().getSchema().existsClass("E_" + orientEdgeClassName)) {

                logString =
                    "Found a Neo4j Relationship Type ('" + orientEdgeClassName + "') with same name of a Neo4j node Label ('"
                        + currentClass.getName() + "'). Importing in OrientDB relationships of this type as 'E_"
                        + orientEdgeClassName;
                ONeo4jImporterContext.getInstance().getMessageHandler().warn(logString);
              }
              orientEdgeClassName = "E_" + orientEdgeClassName;
            }
          }

          try {
            OEdge currentEdge = this.addEdgeToGraph(oDb, outVertex, inVertex, orientEdgeClassName, relationshipProperties);
            statistics.orientDBImportedEdgesCounter++;
            ONeo4jImporterContext.getInstance().getMessageHandler().debug("Orient:" + outVertex.getProperty("@rid") +"-"+ currentRelationshipType  +"->"+ inVertex.getProperty("@rid"));

            if(cont % EDGES_BATCH_SIZE == 0) {
              oDb.commit();
              oDb.getLocalCache().clear();
              oDb.begin();
            }
            cont++;
          } catch (Exception e) {
            oDb.rollback();
            String mess = "Found an error when trying to create an Edge in OrientDB. Corresponding Relationship in Neo4j is '"
                + currentRecord + "': " + e.getMessage();
            ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
            ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
          }
        }

        // committing last batch
        oDb.commit();
        oDb.getLocalCache().clear();

      } catch (Neo4jException e) {
        oDb.rollback();
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

  private Object getNeo4jRecordValue(Record currentRecord, String key, Session session) {

    Type type = currentRecord.get(key).type();
    Object value = null;

    if(type.equals(session.typeSystem().INTEGER())) {
      value = currentRecord.get(key).asInt();
    }
    else if(type.equals(session.typeSystem().BOOLEAN())) {
      value = currentRecord.get(key).asBoolean();

    }
    else if(type.equals(session.typeSystem().FLOAT())) {
      value = currentRecord.get(key).asFloat();

    }
    else if(type.equals(session.typeSystem().NUMBER())) {
      value = currentRecord.get(key).asNumber();

    }
    else if(type.equals(session.typeSystem().STRING())) {
      value = currentRecord.get(key).asString();
    }

    return value;
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
        ONeo4jImporterContext.getInstance().getMessageHandler().info(logString);

        Collection<OClass> edgeClasses = oDb.getMetadata().getSchema().getClass("E").getAllSubclasses();
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

  private OVertex addVertexToGraph(ODatabaseDocument orientGraph, String classAndClusterName, Map<String, Object> properties) {

    OVertex vertex = null;
    boolean alreadySaved = false;
    try {
      if (classAndClusterName != null) {
        if (!oDb.getMetadata().getSchema().existsClass(classAndClusterName)) {
          oDb.commit();
          oDb.createVertexClass(classAndClusterName);
          oDb.begin();
        }
        vertex = orientGraph.newVertex(classAndClusterName);
        if(properties != null) {
          this.setElementProperties(vertex, properties);
          alreadySaved = true;
        }
      }
    } catch (OValidationException e) {
      ONeo4jImporterContext.getInstance().getMessageHandler().debug(e.getMessage());
    }
    if(!alreadySaved) {
      vertex.save();
    }
    return vertex;
  }

  private OEdge addEdgeToGraph(ODatabaseDocument orientGraph, OVertex currentOutVertex, OVertex currentInVertex, String edgeType, Map<String, Object> properties) {

    OEdge edge = null;
    boolean alreadySaved = false;
    try {
      if (!oDb.getMetadata().getSchema().existsClass(edgeType)) {
        oDb.commit();
        oDb.createEdgeClass(edgeType);
        oDb.begin();
      }
      edge = orientGraph.newEdge(currentOutVertex, currentInVertex, edgeType);
      if(properties != null) {
        this.setElementProperties(edge, properties);
        alreadySaved = true;
      }
    } catch (OValidationException e) {
      ONeo4jImporterContext.getInstance().getMessageHandler().debug(e.getMessage());
    }
    if(!alreadySaved) {
      edge.save();
    }
    return edge;
  }

  private void setElementProperties(OElement element, Map<String, Object> properties) {

    try {

      for(String property: properties.keySet()) {
        Object value = properties.get(property);
        element.setProperty(property, value);
      }
      element.save();

    } catch (OValidationException e) {
      ONeo4jImporterContext.getInstance().getMessageHandler().debug(e.getMessage());
    }
  }

}
