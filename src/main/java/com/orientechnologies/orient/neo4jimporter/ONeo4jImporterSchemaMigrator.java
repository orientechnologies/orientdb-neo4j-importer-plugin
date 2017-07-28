package com.orientechnologies.orient.neo4jimporter;

import com.orientechnologies.orient.context.ONeo4jImporterContext;
import com.orientechnologies.orient.context.ONeo4jImporterStatistics;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.exceptions.Neo4jException;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.orientechnologies.orient.neo4jimporter.ONeo4jImporter.PROGRAM_NAME;

/**
 * Created by frank on 08/11/2016.
 */
class ONeo4jImporterSchemaMigrator {
  private String                   keepLogString;
  private DecimalFormat            df;
  private OrientGraph          oDb;
  private ONeo4jImporterStatistics statistics;
  private long                     importingSchemaStartTime;
  private long                     importingSchemaStopTime;

  public ONeo4jImporterSchemaMigrator(String keepLogString, DecimalFormat df, OrientGraph oDb, ONeo4jImporterStatistics statistics) {
    this.keepLogString = keepLogString;
    this.df = df;
    this.oDb = oDb;
    this.statistics = statistics;
  }

  public long getImportingSchemaStartTime() {
    return importingSchemaStartTime;
  }

  public long getImportingSchemaStopTime() {
    return importingSchemaStopTime;
  }

  public void invoke(Session neo4jSession) throws Exception {

    try {

      /**
       * Importing constraints
       */
      this.statistics.importingElements = "constraints";
      this.importConstraints(neo4jSession);
      ONeo4jImporterContext.getInstance().getStatistics().notifyListeners();
      ONeo4jImporterContext.getInstance().getOutputManager().info("\nDone\n\n");
      this.statistics.importingElements = "nothing";

      /**
       * Importing indices
       */
      this.statistics.importingElements = "indices";
      this.importIndices(neo4jSession);
      ONeo4jImporterContext.getInstance().getStatistics().notifyListeners();
      ONeo4jImporterContext.getInstance().getOutputManager().info("\nDone\n\n");
      this.statistics.importingElements = "nothing";

      String logString = PROGRAM_NAME + " - v." + OConstants.ORIENT_VERSION + " - PHASE 3 completed!\n";
      ONeo4jImporterContext.getInstance().getOutputManager().info(logString);

    } catch(Exception e) {
      throw new RuntimeException(e);
    }

  }

  private void importIndices(Session session) {

    String logString;
    boolean propertyCreationSuccess;
    double value;

    logString = "Getting Indices from Neo4j and creating corresponding ones in OrientDB...\n";
    ONeo4jImporterContext.getInstance().getOutputManager().info(logString);

    //counting Neo4j Indices so that we can show a % on OrientDB indices creation

    List<Record> indices = null;
    try {

      String query = "CALL db.indexes()";
      indices = session.run(query).list();  // storing result in a list in order to avoid another similar query later. List's dimension is not a problem.
      statistics.neo4jTotalIndices = indices.size();
    } catch(Exception e) {
      String mess = "";
      ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
      ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
    }

    try {
      String orientDBIndexClassName = "";

      for (Record currentIndexDefinition : indices) {

        statistics.neo4jIndicesCounter++;
        String indexDescription = currentIndexDefinition.get("description").asString();

        //the label this index is on (Neo4j indices are allowed on nodes only)
        String neo4jLabelOfIndex = indexDescription.substring(indexDescription.indexOf(":") + 1, indexDescription.indexOf("("));
        orientDBIndexClassName =  neo4jLabelOfIndex;

        ONeo4jImporterContext.getInstance().getOutputManager().debug("all index: on label " + neo4jLabelOfIndex);

        statistics.neo4jNonConstraintsIndicesCounter++;

        ONeo4jImporterContext.getInstance().getOutputManager().debug("non constraint index: on label " + neo4jLabelOfIndex);

        //gets the property this index is on
        String rawProps = indexDescription.substring(indexDescription.indexOf("(") + 1, indexDescription.indexOf(")"));
        String[] properties = rawProps.split(",");

        //create the index in OrientDB - we create NOT UNIQUE indices here (case of UNIQUE indices is handled above)

        //create class neo4jLabelOfIndex
        //there might be in fact cases where in neo4j the index has been defined, but no nodes have been created.
        //As a result, no nodes of that class have been imported in OrientDB, so that class does not exist in Orient yet
        if (!oDb.getRawGraph().getMetadata().getSchema().existsClass(neo4jLabelOfIndex)) {
          orientDBIndexClassName = ONeo4jImporterUtils.checkVertexClassCreation(neo4jLabelOfIndex, oDb);
        }

        //creates in OrientDB all the properties this index is defined on
        for(int i=0; i<properties.length; i++) {
          propertyCreationSuccess = ONeo4jImporterUtils
              .createOrientDBProperty(session, neo4jLabelOfIndex, orientDBIndexClassName, properties[i], oDb, "NOT UNIQUE");
        }

        //index creation
        try {

          OClass orientDBClass = oDb.getRawGraph().getMetadata().getSchema().getClass(orientDBIndexClassName);

          //if the index is created as a side effect of the creation of a uniqueness constraint, we handled the case already above
          List<String> propertiesList = Arrays.asList(properties);
          if(!orientDBClass.areIndexed(propertiesList)) {

            //creates the index

            String statement;
            if(propertiesList.size() == 1) {
              statement = "create index `" + orientDBClass + "." + propertiesList.get(0) + "`" + " on `" + orientDBClass.getName() + "` (" + rawProps + ") notunique_hash_index";
            }
            else {
              statement = "create index `" + orientDBClass + ".props`" + " on `" + orientDBClass.getName() + "` (" + rawProps + ") notunique_hash_index";
            }
            oDb.command(new OCommandSQL(statement)).execute();

            statistics.orientDBImportedIndicesCounter++;
          }
        } catch (Exception e) {
          String mess = "Found an error when trying to create a NOTUNIQUE Index in OrientDB. Node label '" + neo4jLabelOfIndex + "': " + e.getMessage();
          ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
          ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
        }

        //print progress
//        value = 100 * (statistics.neo4jNonConstraintsIndicesCounter / (statistics.neo4jTotalIndices
//            - statistics.neo4jTotalUniqueConstraints));
//        keepLogString =
//            df.format(statistics.orientDBImportedIndicesCounter) + " OrientDB Indices have been created (" + df.format(value)
//                + "% done)";
//        ONeo4jImporterContext.getInstance().getOutputManager().info("\r" + keepLogString);
//        value = 0;
      }
    } catch(Exception e) {
      String mess = "";
      ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
      ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
    }

    importingSchemaStopTime = System.currentTimeMillis();

//    if (statistics.orientDBImportedIndicesCounter == 0) {
//      keepLogString = df.format(statistics.orientDBImportedIndicesCounter) + " OrientDB Indices have been created";
//      ONeo4jImporterContext.getInstance().getOutputManager().info("\r  " + keepLogString);
//    }
  }


  private String importConstraints(Session session) throws Exception {

    String logString;
    double value;
    importingSchemaStartTime = System.currentTimeMillis();

    String neo4jPropKey = "";
    Boolean isConstraintsOnNode = false;
    Boolean isConstraintsOnRelationship = false;

    boolean propertyCreationSuccess = false;

    //     index workaround
    boolean indexWorkaround = true;
    if (indexWorkaround) {
      if (oDb.getRawGraph().getMetadata().getSchema().existsClass("MultipleLabelNeo4jConversion")) {
        OClass multipleLabelClass = oDb.getRawGraph().getMetadata().getSchema().getClass("MultipleLabelNeo4jConversion");

        if (multipleLabelClass.existsProperty("neo4jLabelList")) {
          if (oDb.getRawGraph().getMetadata().getIndexManager().existsIndex("MultipleLabelNeo4jConversion.neo4jLabelList")) {

            logString = "Rebuilding Index MultipleLabelNeo4jConversion.neo4jLabelList. Please wait...\n";
            ONeo4jImporterContext.getInstance().getOutputManager().info(logString);

            try{
              oDb.getRawGraph().getMetadata().getIndexManager().getClassIndex("MultipleLabelNeo4jConversion", "MultipleLabelNeo4jConversion.neo4jLabelList").rebuild();
              ONeo4jImporterContext.getInstance().getOutputManager().info("\r" + logString + "Done\n");
            } catch (Exception e) {
              ONeo4jImporterContext.getInstance().getOutputManager().info("\r" + logString + "Failed\n");
              String mess = "Found an error when trying to rebuild the index 'MultipleLabelNeo4jConversion.neo4jLabelList': " + e.getMessage();
              ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
              ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
            }
          }
        }
      }
    }
    // end index workaround

    logString = "Getting Constraints from Neo4j and creating corresponding ones in OrientDB...\n";
    ONeo4jImporterContext.getInstance().getOutputManager().info(logString);

    //counting Neo4j Constraints so that we can show a % on OrientDB Constraints creation
    try {

      String query = "CALL db.constraints()";
      StatementResult result = session.run(query);

      while(result.hasNext()) {
        Record currentRecord = result.next();
        Map<String, Object> neo4jConstraintDefinition = currentRecord.asMap();
        statistics.neo4jTotalConstraints++;
        if ("UNIQUENESS".equals(neo4jConstraintDefinition.get("type"))) {
          statistics.neo4jTotalUniqueConstraints++;
        }
        if ("NODE_PROPERTY_EXISTENCE".equals(neo4jConstraintDefinition.get("type"))) {
          statistics.neo4jTotalNodePropertyExistenceConstraints++;
        }
        if ("RELATIONSHIP_PROPERTY_EXISTENCE".equals(neo4jConstraintDefinition.get("type"))) {
          statistics.neo4jTotalRelPropertyExistenceConstraints++;
        }
      }
    } catch (Neo4jException e) {
      String mess = "";
      ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
      ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
      throw new RuntimeException(e);
    } catch(Exception e) {
      String mess = "";
      ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
      ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
    }

    try {

      // getting all constraints and iterating

      String query = "CALL db.constraints()";
      StatementResult result = session.run(query);

      while(result.hasNext()) {
        Record currentRecord = result.next();
        Map<String, Object> neo4jConstraintDefinition = currentRecord.asMap();
        statistics.neo4jConstraintsCounter++;

        //determine the type of the constraints - different actions will need to be taken according to this type
        String neo4jConstraintType = this.getConstraintType(neo4jConstraintDefinition);

        //determine the class where the constraints will be added in OrientDB
        //Neo4j allows constraints on both nodes and relationship. To get the OrientDB class, we have to separate the cases
        String orientDBIndexClass = "";
        String neo4jLabel = "";

        try {
          //we can get the label with the method getLabel() only if this constraint is associated with a node
          //this is associated with a node

          neo4jLabel = this.getConstraintLabel(neo4jConstraintDefinition);
          orientDBIndexClass = neo4jLabel;

          //create class orientDBIndexClass
          //there might be in fact cases where in neo4j the constraint as been defined, but no nodes have been created. As a result, no nodes of that class have been imported in OrientDB, so that class does not exist in Orient
          if (!oDb.getRawGraph().getMetadata().getSchema().existsClass(neo4jLabel)) {
            orientDBIndexClass = ONeo4jImporterUtils.checkVertexClassCreation(neo4jLabel, oDb);
          }

          isConstraintsOnNode = true;
          isConstraintsOnRelationship = false;

        } catch (IllegalStateException a) {
          //otherwise it is associated with a relationship
          //this is associated with a relationship. Do nothing
        }

        try {
          //we can get the relationship this constraint is associated with only if it is associated with a relationship

          String neo4jConstraintRelationshipType = this.getRelationshipType(neo4jConstraintDefinition);

          if(neo4jConstraintRelationshipType != null) {
            orientDBIndexClass = neo4jConstraintRelationshipType;

            //create class orientDBIndexClass
            //there might be in fact cases where in neo4j the constraint as been defined, but no nodes have been created. As a result, no nodes of that class have been imported in OrientDB, so that class does not exist in Orient
            if (oDb.getRawGraph().getMetadata().getSchema().existsClass(orientDBIndexClass) == false) {
              oDb.createEdgeType(orientDBIndexClass);
            }
            isConstraintsOnNode = false;
            isConstraintsOnRelationship = true;
          }

        } catch (IllegalStateException a) {
          //otherwise it is associated with a node
          //this is associated with a node. Do nothing
        }

        //we now know the type of this constraints and the class on which it is defined (orientDBIndexClass)

        //determine the property key on which the constraint has been defined
//        Iterable<String> neo4jConstraintKeys = neo4jConstraintDefinition.keySet();
//        for (final String myNeo4jConstraintPropKey : neo4jConstraintKeys) {
//          neo4jPropKey = myNeo4jConstraintPropKey;
//        }
        neo4jPropKey = this.getConstraintProperty(neo4jConstraintDefinition);

        //to import this constraint, we first have to create the corresponding property in OrientDB
        propertyCreationSuccess = ONeo4jImporterUtils.createOrientDBProperty(session, neo4jLabel, orientDBIndexClass, neo4jPropKey, oDb,
            neo4jConstraintType);

        // now that the property has been created, we need to take actions based on the neo4jConstraintType
        if (propertyCreationSuccess) {

          //taking actions depending on the type of the constraints
          if ("UNIQUENESS".equals(neo4jConstraintType)) {

            statistics.neo4jUniqueConstraintsCounter++;

            try {

              //unique constratins can be only on nodes. with this check we avoid odd things in very odd situations that may happen
              if (isConstraintsOnNode) {

                //we check that orientDBIndexClass is a vertex class; with this check we avoid odd things in very odd situations that may happen, e.g. node labels = rel types.  nodes with multiple types (that are imported in a single class). In such case it may happen that it try to create an index on the class, but that class does not exist as vertex class (because all nodes were imported into another class - but it exists as edge class)
                if (oDb.getRawGraph().getMetadata().getSchema().getClass(orientDBIndexClass)
                    .isSubClassOf(oDb.getRawGraph().getMetadata().getSchema().getClass("V"))) {

                  //we map Neo4j constraints of type UNIQUENESS to UNIQUE indices in Neo4j
                  OIndex OrientDBIndex = oDb.getRawGraph().getMetadata().getSchema().getClass(orientDBIndexClass)
                      .getProperty(neo4jPropKey).createIndex(OClass.INDEX_TYPE.UNIQUE);

                  ONeo4jImporterContext.getInstance().getOutputManager().debug("\nCreated index: " + OrientDBIndex);

                  statistics.orientDBImportedUniqueConstraintsCounter++;
                  statistics.orientDBImportedConstraintsCounter++;
                }
              }
            } catch (Exception e) {

              String mess = "Found an error when trying to create a UNIQUE Index in OrientDB. Corresponding Property in Neo4j is '"
                  + neo4jPropKey + "' on node label '" + orientDBIndexClass + "': " + e.getMessage();
              ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
              ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");

              //github issue #3 - tries to create a not unique index as workaround
              try {

                logString = "Trying to create a NOT UNIQUE Index as workaround...";
                ONeo4jImporterContext.getInstance().getOutputManager().info(logString);

                OIndex OrientDBIndex = oDb.getRawGraph().getMetadata().getSchema().getClass(orientDBIndexClass)
                    .getProperty(neo4jPropKey).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

                statistics.orientDBImportedNotUniqueWorkaroundCounter++;

              } catch (Exception e2) {
                String mess2 = "";
                ONeo4jImporterContext.getInstance().printExceptionMessage(e2, mess, "error");
                ONeo4jImporterContext.getInstance().printExceptionStackTrace(e2, "error");
              }
            }
          }

          if ("NODE_PROPERTY_EXISTENCE".equals(neo4jConstraintType)) {
          }

          if ("RELATIONSHIP_PROPERTY_EXISTENCE ".equals(neo4jConstraintType)) {
          }
        }

        //print progress
        value = 100 * (statistics.neo4jConstraintsCounter / statistics.neo4jTotalConstraints);

        if ("UNIQUENESS".equals(neo4jConstraintType)) {
//          keepLogString =
//              df.format(statistics.orientDBImportedUniqueConstraintsCounter) + " OrientDB UNIQUE Indices have been created";
        }
        if ("NODE_PROPERTY_EXISTENCE".equals(neo4jConstraintType)) {
        }
        if ("RELATIONSHIP_PROPERTY_EXISTENCE ".equals(neo4jConstraintType)) {
        }

//        keepLogString = keepLogString + " (" + df.format(value) + "% done)";
//        ONeo4jImporterContext.getInstance().getOutputManager().info("\r  " + keepLogString);
        value = 0;
      }

    } catch (Neo4jException e) {
      String mess = "";
      ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
      ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
      throw new RuntimeException(e);
    } catch(Exception e) {
      String mess = "";
      ONeo4jImporterContext.getInstance().printExceptionMessage(e, mess, "error");
      ONeo4jImporterContext.getInstance().printExceptionStackTrace(e, "error");
    }
    return neo4jPropKey;
  }


  public String getConstraintType(Map<String, Object> neo4jConstraintDefinition) {

    String constraintDescription = neo4jConstraintDefinition.get("description").toString();
    if(constraintDescription.contains("IS UNIQUE")) {
      return "UNIQUENESS";
    }
    else {
      return "UNKNOWN";
    }
  }

  private String getConstraintLabel(Map<String, Object> neo4jConstraintDefinition) {

    String constraintDescription = neo4jConstraintDefinition.get("description").toString();
    String label = constraintDescription.substring(constraintDescription.indexOf("(")+1, constraintDescription.indexOf(")"));
    label = label.replace(" ", "");

    if(label.contains(":")) {
      label = label.substring(label.indexOf(":")+1);
    }
    return label;
  }

  private String getRelationshipType(Map<String, Object> neo4jConstraintDefinition) {

    String constraintDescription = neo4jConstraintDefinition.get("description").toString();
    String relationshipType = null;
    if(constraintDescription.contains("exists(")) {
      relationshipType = constraintDescription.substring(constraintDescription.indexOf("exists(")+1, constraintDescription.lastIndexOf(")"));
      relationshipType = relationshipType.substring(0, relationshipType.indexOf("."));
    }
    return relationshipType;
  }

  private String getConstraintProperty(Map<String, Object> neo4jConstraintDefinition) {

    String constraintDescription = neo4jConstraintDefinition.get("description").toString();
    String propertyName = constraintDescription.substring(constraintDescription.indexOf(") ASSERT ") + 1, constraintDescription.indexOf("IS"));
    propertyName = propertyName.replace("ASSERT", "");
    propertyName = propertyName.replace(" ", "");
    propertyName = propertyName.substring(propertyName.indexOf(".") +1);
    return propertyName;
  }
}
