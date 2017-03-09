package com.orientechnologies.orient.neo4jimporter;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;

import java.util.logging.Level;

/**
 * Created by frank on 13/11/2016.
 */
public class ONeo4jImporterUtils {

  public static String checkVertexClassCreation(String myOrientDBIndexClass, OrientGraphNoTx myOdb) {

    String myLogString = "";

    //cheks if this labels has been imported in the MultipleLabelNeo4jConversion label
    //If yes:
    //1. the vertex class is not created as part of the schema migration
    //2. its indices (and corresponding properties) are created in MultipleLabelNeo4jConversion
    String sqlQuery =
        "SELECT @rid FROM V WHERE @class = 'MultipleLabelNeo4jConversion' AND Neo4jLabelList CONTAINS '" + myOrientDBIndexClass
            + "' LIMIT 1";

    int u = 0;
    for (Vertex v : (Iterable<Vertex>) myOdb.command(new OCommandSQL(sqlQuery)).execute()) {
      u++;
      //System.out.println("\n\n\nfound it: " + myOrientDBIndexClass + " " + v); //debug
      myOrientDBIndexClass = "MultipleLabelNeo4jConversion";
    }

    if (u == 0) {
      //System.out.println("\n\n\ndid not find it: " + myOrientDBIndexClass); //debug
      myOdb.createVertexType(myOrientDBIndexClass);

      // in order to improve record lookup when filtering on Neo4j labels all classes must have the Neo4jLabelList properties and an index on it
      // when classes are created during nodes migration, this property is created automatically
      // here we need to create it for those additional classes that are created (empty) during schema migration

      // index on property Neo4jLabelList
      try {

        //first create the property
        myOdb.getRawGraph().getMetadata().getSchema().getClass(myOrientDBIndexClass)
            .createProperty("Neo4jLabelList", OType.EMBEDDEDLIST, OType.STRING);

        //creates the index if the property creation was successfull
        try {

          myOdb.getRawGraph().getMetadata().getSchema().getClass(myOrientDBIndexClass).getProperty("Neo4jLabelList")
              .createIndex(OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX);

        } catch (Exception e) {

          myLogString =
              "Found an error when trying to create a NOT UNIQUE Index in OrientDB on the 'Neo4jLabelList' Property of the vertex Class '"
                  + myOrientDBIndexClass + "': " + e.getMessage();
          ONeo4jImporter.importLogger.log(Level.SEVERE, myLogString);

        }
      } catch (Exception e) {

        myLogString = "Found an error when trying to create the 'Neo4jLabelList' Property in OrientDB on the vertex Class '"
            + myOrientDBIndexClass + "': " + e.getMessage();
        ONeo4jImporter.importLogger.log(Level.SEVERE, myLogString);

      }
      //
    }

    return myOrientDBIndexClass;

  }

//  public static boolean createOrientDBProperty(final Label myNeo4jLabel, final String myOrientDBIndexClass,
//      final String myNeo4jPropKey, final GraphDatabaseService myNeo4jGraphDb, OrientGraphNoTx myOdb,
//      final String myNeo4jConstraintType) {
//
//    //To create a property in OrientDB first we need to understand the Neo4j property data type.
//    //To do this we will use java instanceof, as there are no specific methods in the noe4j api to get the data type of a property
//    //To be able to use instanceof, we first need to find a node that has that property
//
//    //
//    OType orientOtype = null;
//    boolean foundNode = false;
//    long debugCounter = 0L;
//    String logString = "";
//    //
//
//    //find a node that has this property, then get the data type of this property
//    try (final Transaction tx = myNeo4jGraphDb.beginTx()) {
//
//      ResourceIterator<Node> neo4jNodes = myNeo4jGraphDb.findNodes(myNeo4jLabel);
//      try {
//
//        while (neo4jNodes.hasNext()) {
//
//          debugCounter++;
//
//          try {
//
//            Node myNode = neo4jNodes.next();
//
//            //not all nodes with label myNeo4jLabel may have this property - even if we have a unique constraint on this property (it is unique in the nodes where the property exists). When we find a node with this property, we exit the loop
//            if (myNode.hasProperty(myNeo4jPropKey)) {
//
//              foundNode = true;
//
//              //debug
//              //System.out.println("Found node. debugCounter is: " + debugCounter + ". Working on node " + myNode);
//
//              Object PropertyValue = myNode.getProperty(myNeo4jPropKey, null);
//
//              //map the Neo4j property type to an OrientDB property data type
//              orientOtype = mapNeo4JToOrientDBPropertyType(getNeo4jPropType(PropertyValue));
//
//              //debug
//              //System.out.println("Property defined on this node: " + myNeo4jPropKey + " value: " + PropertyValue + " data type: " + neo4jPropType);
//
//              break;
//            }
//
//          } catch (Exception e) {
//
//            logString = e.toString();
//            ONeo4jImporter.importLogger.log(Level.WARNING, logString);
//
//            break;
//          }
//
//        }
//
//      } finally {
//
//        neo4jNodes.close();
//
//      }
//
//    }
//    //
//
//    //Now that we know the data type of the property, we can create it in OrientDB
//
//    //However, there may be cases where the constraints has been defined, but no nodes have been created yet. In this case we cannot know the data type. We will use STRING as default
//    if (foundNode == false) {
//      orientOtype = OType.STRING;
//    }
//
//    //debug
//    //System.out.println("Creating OrientDB Property '" + myNeo4jPropKey + "' of type '" + orientOtype + "' on Class '" + myOrientDBIndexClass + "' ");
//
//    try {
//
//      OProperty OrientDBProperty = myOdb.getRawGraph().getMetadata().getSchema().getClass(myOrientDBIndexClass)
//          .createProperty(myNeo4jPropKey, orientOtype);
//
//      if (foundNode == false) {
//
//        logString =
//            "The Neo4j Property '" + myNeo4jPropKey + "' on the Neo4j Label '" + myNeo4jLabel.name() + "' associated to a Neo4j '"
//                + myNeo4jConstraintType
//                + "' constraint/index has been imported as STRING because there are no nodes in Neo4j that have this property, hence it was not possible to determine the type of this Neo4j Property";
//        ONeo4jImporter.importLogger.log(Level.INFO, logString);
//
//      } else {
//
//        logString =
//            "Created Property '" + myNeo4jPropKey + "' on the Class '" + myOrientDBIndexClass + "' with type '" + orientOtype + "'";
//        ONeo4jImporter.importLogger.log(Level.INFO, logString);
//
//      }
//
//      return true;
//
//    } catch (Exception e) {
//
//      logString =
//          "Found an error when trying to create a Property in OrientDB. Correspinding Property in Neo4j is '" + myNeo4jPropKey
//              + "' on node label '" + myOrientDBIndexClass + "': " + e.getMessage();
//      ONeo4jImporter.importLogger.log(Level.SEVERE, logString);
//
//      return false;
//
//    }
//    //
//
//  }

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
