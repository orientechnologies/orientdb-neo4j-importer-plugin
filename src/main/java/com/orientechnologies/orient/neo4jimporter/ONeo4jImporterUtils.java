package com.orientechnologies.orient.neo4jimporter;

import com.orientechnologies.orient.context.ONeo4jImporterContext;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

import java.util.logging.Level;

/**
 * Created by frank on 13/11/2016.
 */
public class ONeo4jImporterUtils {


  /**
   * It's used during the schema migration to import constraints and indices defined in neo4j on an empty set of nodes.
   * Thus is used when no OrientDB class was defined for an empty set of nodes, but it must be defined due to some constraints or indexes.
   *
   * It checks if the nodes with label passed as parameter have been imported in the MultipleLabelNeo4jConversion class.
   * If no vertices were imported with the passed label (among the other labels) the correspondent class does not exist, and so:
   * 1. the vertex class is created as part of the schema migration
   * 2. its indices (and corresponding properties) are created
   */

  public static String checkVertexClassCreation(String neo4jLabel, OrientGraphNoTx odb) {

    String logString = "";
    String orientDBClassName = neo4jLabel;

    String sqlQuery =
        "SELECT @rid FROM V WHERE @class = 'MultipleLabelNeo4jConversion' AND Neo4jLabelList CONTAINS '" + neo4jLabel
            + "' LIMIT 1";

    int u = 0;
    for (Vertex v : (Iterable<Vertex>) odb.command(new OCommandSQL(sqlQuery)).execute()) {
      u++;
      ONeo4jImporterContext.getInstance().getOutputManager().debug("\n\n\nfound it: " + orientDBClassName + " " + v);
      orientDBClassName = "MultipleLabelNeo4jConversion";
    }

    if (u == 0) {

      // case: the label is mapped to a single OrientDB class, and it will be created as not present in the target database yet.
      // In fact this method is called when the class does not exist in orientDB, because no nodes in neo4j are logically connected to it, but just an index some constraints.

      ONeo4jImporterContext.getInstance().getOutputManager().debug("\n\n\ndid not find it: " + orientDBClassName);
      OClass orientDBClass = odb.createVertexType(orientDBClassName);

      // in order to improve record lookup when filtering on Neo4j labels all classes must have the Neo4jLabelList properties and an index on it
      // when classes are created during nodes migration, this property is created automatically
      // here we need to create it for those additional classes that are created (empty) during schema migration
      try {

        //first create the property
        orientDBClass.createProperty("Neo4jLabelList", OType.EMBEDDEDLIST, OType.STRING);

        //creates the index if the property creation was successful
        try {
          orientDBClass.getProperty("Neo4jLabelList").createIndex(OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX);
        } catch (Exception e) {
          String mess =
              "Found an error when trying to create a NOT UNIQUE Index in OrientDB on the 'Neo4jLabelList' Property of the vertex Class '"
                  + orientDBClassName + "': " + e.getMessage();
          ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
          ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
        }
      } catch (Exception e) {
        String mess = "Found an error when trying to create the 'Neo4jLabelList' Property in OrientDB on the vertex Class '"
            + orientDBClassName + "': " + e.getMessage();
        ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
        ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
      }
    }
    return orientDBClassName;
  }

  public static boolean createOrientDBProperty(Session session, String neo4jLabel, String orientDBIndexClass, String neo4jPropKey, OrientGraphNoTx oDb, String myNeo4jConstraintType) {

    //To create a property in OrientDB first we need to understand the Neo4j property data type.
    //To do this we will use java instanceof, as there are no specific methods in the noe4j api to get the data type of a property
    //To be able to use instanceof, we first need to find a node that has that property

    OType orientOtype = null;
    boolean foundNode = false;
    long debugCounter = 0L;
    String logString = "";

    //find a node that has this property, then get the data type of this property
    try {

      String query = "MATCH (n) " + "WHERE n:" + neo4jLabel + " AND exists(n." + neo4jPropKey + ") " + "RETURN n." + neo4jPropKey + " as targetProp";
      StatementResult result = session.run(query);
//      ResourceIterator<Node> neo4jNodes = myNeo4jGraphDb.findNodes(neo4jLabel);

      while (result.hasNext()) {
        debugCounter++;

        String neo4jPropType = "";
        try {

          // get just the first node with the specific label and
          Record currentNode = result.next();

          ONeo4jImporterContext.getInstance().getOutputManager().debug("debugCounter is: " + debugCounter + ". Working on node " + currentNode.get("id").asString());
          Object propertyValue = currentNode.get("targetProp");
          if(propertyValue != null) {

            //map the Neo4j property type to an OrientDB property data type
            neo4jPropType = getNeo4jPropType(propertyValue);
            orientOtype = mapNeo4JToOrientDBPropertyType(neo4jPropType);

            //not all nodes with label myNeo4jLabel may have this property - even if we have a unique constraint on this property (it is unique in the nodes where the property exists). When we find a node with this property, we exit the loop
            foundNode = true;
          }

          ONeo4jImporterContext.getInstance().getOutputManager().debug("Property defined on this node: " + neo4jPropKey + " value: " + propertyValue + " data type: " + neo4jPropType);
          if(foundNode) {
            break;
          }
        } catch (Exception e) {
          String mess = "";
          ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
          ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
          break;
        }
      }

    } catch(Exception e) {
      String mess = "";
      ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
      ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
    }

    //Now that we know the data type of the property, we can create it in OrientDB

    //However, there may be cases where the constraints has been defined, but no nodes have been created yet. In this case we cannot know the data type. We will use STRING as default
    if (foundNode == false) {
      orientOtype = OType.STRING;
    }

    ONeo4jImporterContext.getInstance().getOutputManager().debug("Creating OrientDB Property '" + neo4jPropKey + "' of type '" + orientOtype + "' on Class '" + orientDBIndexClass + "' ");

    try {

      OProperty OrientDBProperty = oDb.getRawGraph().getMetadata().getSchema().getClass(orientDBIndexClass)
          .createProperty(neo4jPropKey, orientOtype);

      if (foundNode == false) {
        logString =
            "The Neo4j Property '" + neo4jPropKey + "' on the Neo4j Label '" + neo4jLabel + "' associated to a Neo4j '"
                + myNeo4jConstraintType
                + "' constraint/index has been imported as STRING because there are no nodes in Neo4j that have this property, hence it was not possible to determine the type of this Neo4j Property";
        ONeo4jImporterContext.getInstance().getOutputManager().info(logString);
      } else {
        logString =
            "Created Property '" + neo4jPropKey + "' on the Class '" + orientDBIndexClass + "' with type '" + orientOtype + "'";
        ONeo4jImporterContext.getInstance().getOutputManager().info(logString);
      }
      return true;
    } catch (Exception e) {
      String mess = "Found an error when trying to create a Property in OrientDB. Correspinding Property in Neo4j is '" + neo4jPropKey
              + "' on node label '" + orientDBIndexClass + "': " + e.getMessage();
      ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
      ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
      return false;
    }
  }

  public static OType mapNeo4JToOrientDBPropertyType(final String myNeo4jPropType) {

    switch (myNeo4jPropType) {
    case "String":
      return OType.STRING;
    case "Character":
      return OType.STRING;
    case "Integer":
      return OType.INTEGER;
    case "Long":
      return OType.LONG;
    case "Boolean":
      return OType.BOOLEAN;
    case "Byte":
      return OType.BYTE;
    case "Float":
      return OType.FLOAT;
    case "Double":
      return OType.DOUBLE;
    case "Short":
      return OType.SHORT;
    default:
      return OType.STRING;
    }

  }

  public static String getNeo4jPropType(final Object myPropertyValue) {

    String myNeo4jPropType = "String";

    if (null == myPropertyValue || myPropertyValue instanceof String) {
      myNeo4jPropType = "String";
    } else if (myPropertyValue instanceof Integer) {
      myNeo4jPropType = "Integer";
    } else if (myPropertyValue instanceof Long) {
      myNeo4jPropType = "Long";
    } else if (myPropertyValue instanceof Boolean) {
      myNeo4jPropType = "Boolean";
    } else if (myPropertyValue instanceof Byte) {
      myNeo4jPropType = "Byte";
    } else if (myPropertyValue instanceof Float) {
      myNeo4jPropType = "Float";
    } else if (myPropertyValue instanceof Double) {
      myNeo4jPropType = "Double";
    } else if (myPropertyValue instanceof Character) {
      myNeo4jPropType = "Character";
    } else if (myPropertyValue instanceof Short) {
      myNeo4jPropType = "Short";
    } else {
      myNeo4jPropType = "String";
    }

    return myNeo4jPropType;

  }

}
