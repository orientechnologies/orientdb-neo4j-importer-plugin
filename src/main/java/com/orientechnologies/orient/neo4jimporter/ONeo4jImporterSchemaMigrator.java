package com.orientechnologies.orient.neo4jimporter;

import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import org.neo4j.driver.v1.*;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import static com.orientechnologies.orient.neo4jimporter.ONeo4jImporter.PROGRAM_NAME;

/**
 * Created by frank on 08/11/2016.
 */
class ONeo4jImporterSchemaMigrator {
  private String                 keepLogString;
  private DecimalFormat          df;
  private OrientGraphNoTx        oDb;
  private ONeo4jImporterCounters counters;
  private double                 importingSchemaStartTime;
  private double                 importingSchemaStopTime;

  public ONeo4jImporterSchemaMigrator(String keepLogString, DecimalFormat df, OrientGraphNoTx oDb, ONeo4jImporterCounters counters) {
    this.keepLogString = keepLogString;
    this.df = df;
    this.oDb = oDb;
    this.counters = counters;
  }

  public double getImportingSchemaStartTime() {
    return importingSchemaStartTime;
  }

  public double getImportingSchemaStopTime() {
    return importingSchemaStopTime;
  }

  public void invoke(Session neo4jSession) {

    try {

      /**
       * Importing constraints
       */
      this.importConstraints(neo4jSession);

      String logString;
      boolean propertyCreationSuccess;
      double value;

      /**
       * Importing indices
       */

      this.importIndices(neo4jSession);

      //prints number of unique constraints in the log
      ONeo4jImporter.importLogger.log(Level.INFO, keepLogString);

      System.out.println("\nDone");

      logString = PROGRAM_NAME + " - v." + OConstants.getVersion() + " - PHASE 3 completed!\n";
      ONeo4jImporter.importLogger.log(Level.INFO, logString);

      System.out.println();
      System.out.println("Import completed!");
    } catch(Exception e) {
      e.printStackTrace();
    }

  }

  private void importIndices(Session session) {
    String logString;
    boolean propertyCreationSuccess;
    double value;
    if (counters.orientDBImportedUniqueConstraintsCounter == 0) {
      keepLogString = df.format(counters.orientDBImportedUniqueConstraintsCounter) + " OrientDB UNIQUE Indices have been created";
      System.out.print("\r  " + keepLogString);
    }

    //prints number of unique constraints in the log
    ONeo4jImporter.importLogger.log(Level.INFO, keepLogString);

    System.out.println("\nDone");

    logString = "Getting Indices from Neo4j and creating corresponding ones in OrientDB...";

    System.out.println();
    System.out.println(logString);

    ONeo4jImporter.importLogger.log(Level.INFO, logString);

    //counting Neo4j Indices so that we can show a % on OrientDB indices creation

    List<Record> indices = null;
    try {

      String query = "CALL db.indexes()";
      indices = session.run(query).list();  // storing result in a list in order to avoid another similar query later. List's dimension is not a problem.
      counters.neo4jTotalIndices = indices.size();
    } catch(Exception e) {
      e.printStackTrace();
    }

    try {
      String orientDBIndexClassName = "";

      for (Record currentIndexDefinition : indices) {

        counters.neo4jIndicesCounter++;
        String indexDescription = currentIndexDefinition.get("description").asString();

        //the label this index is on (Neo4j indices are allowed on nodes only)
        String neo4jLabelOfIndex = indexDescription.substring(indexDescription.indexOf(":") + 1, indexDescription.indexOf("("));
        orientDBIndexClassName =  neo4jLabelOfIndex;

        //debug
        //System.out.println("all index: on label " + myNeo4jLabelOfIndex);

        counters.neo4jNonConstraintsIndicesCounter++;

        //debug
        //System.out.println("non constraint index: on label " + myNeo4jLabelOfIndex);

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

            counters.orientDBImportedIndicesCounter++;
          }
        } catch (Exception e) {
          logString = "Found an error when trying to create a NOTUNIQUE Index in OrientDB. Node label '" + neo4jLabelOfIndex + "': " + e.getMessage();
          ONeo4jImporter.importLogger.log(Level.SEVERE, logString);
        }

        //print progress
        value = 100 * (counters.neo4jNonConstraintsIndicesCounter / (counters.neo4jTotalIndices
            - counters.neo4jTotalUniqueConstraints));
        keepLogString =
            df.format(counters.orientDBImportedIndicesCounter) + " OrientDB Indices have been created (" + df.format(value)
                + "% done)";
        System.out.print("\r  " + keepLogString);
        value = 0;
      }
    } catch(Exception e) {
      e.printStackTrace();
    }

    importingSchemaStopTime = System.currentTimeMillis();

    if (counters.orientDBImportedIndicesCounter == 0) {
      keepLogString = df.format(counters.orientDBImportedIndicesCounter) + " OrientDB Indices have been created";
      System.out.print("\r  " + keepLogString);
    }
  }


  private String importConstraints(Session session) {

    String logString;
    double value;
    importingSchemaStartTime = System.currentTimeMillis();

    String neo4jPropKey = "";
    Boolean isConstraintsOnNode = false;
    Boolean isConstraintsOnRelationship = false;

    boolean propertyCreationSuccess = false;

//    boolean indexWorkaround = true;
    // index workaround
//    if (indexWorkaround) {
//      if (oDb.getRawGraph().getMetadata().getSchema().existsClass("MultipleLabelNeo4jConversion")) {
//        OClass multipleLabelClass = oDb.getRawGraph().getMetadata().getSchema().getClass("MultipleLabelNeo4jConversion");
//
//        if (multipleLabelClass.existsProperty("Neo4jLabelList")) {
//          if (oDb.getRawGraph().getMetadata().getIndexManager().existsIndex("MultipleLabelNeo4jConversion.Neo4jLabelList")) {
//
//            logString = "Rebuilding Index MultipleLabelNeo4jConversion.Neo4jLabelList. Please wait...";
//            System.out.println();
//            System.out.print(logString);
//
//            try{
//
//              oDb.getRawGraph().getMetadata().getIndexManager().getClassIndex("MultipleLabelNeo4jConversion", "MultipleLabelNeo4jConversion.Neo4jLabelList").rebuild();
//              System.out.print("\r" + logString + "Done\n");
//
//            } catch (Exception e) {
//
//              System.out.print("\r" + logString + "Failed\n");
//              logString =
//                  "Found an error when trying to rebuild the index 'MultipleLabelNeo4jConversion.Neo4jLabelList': " + e.getMessage();
//              ONeo4jImporter.importLogger.log(Level.SEVERE, logString);
//            }
//          }
//        }
//      }
//    }
    // end index workaround

    logString = "Getting Constraints from Neo4j and creating corresponding ones in OrientDB...";

    System.out.println();
    System.out.println(logString);

    ONeo4jImporter.importLogger.log(Level.INFO, logString);

    //counting Neo4j Constraints so that we can show a % on OrientDB Constraints creation
    try {

      String query = "CALL db.constraints()";
      StatementResult result = session.run(query);

      while(result.hasNext()) {
        Record currentRecord = result.next();
        Map<String, Object> neo4jConstraintDefinition = currentRecord.asMap();
        counters.neo4jTotalConstraints++;
        if ("UNIQUENESS".equals(neo4jConstraintDefinition.get("type"))) {
          counters.neo4jTotalUniqueConstraints++;
        }
        if ("NODE_PROPERTY_EXISTENCE".equals(neo4jConstraintDefinition.get("type"))) {
          counters.neo4jTotalNodePropertyExistenceConstraints++;
        }
        if ("RELATIONSHIP_PROPERTY_EXISTENCE".equals(neo4jConstraintDefinition.get("type"))) {
          counters.neo4jTotalRelPropertyExistenceConstraints++;
        }
      }
    } catch(Exception e) {
      e.printStackTrace();
    }

    try {

      // getting all constraints and iterating

      String query = "CALL db.constraints()";
      StatementResult result = session.run(query);

      while(result.hasNext()) {
        Record currentRecord = result.next();
        Map<String, Object> neo4jConstraintDefinition = currentRecord.asMap();
        counters.neo4jConstraintsCounter++;

        //determine the type of the constraints - different actions will need to be taken according to this type
        String neo4jConstraintType = neo4jConstraintDefinition.get("type").toString();
        //System.out.println(neo4jConstraintType); //Can be: NODE_PROPERTY_EXISTENCE, RELATIONSHIP_PROPERTY_EXISTENCE, UNIQUENESS (on nodes only)

        //determine the class where the constraints will be added in OrientDB
        //Neo4j allows constraints on both nodes and relationship. To get the OrientDB class, we have to separate the cases
        String orientDBIndexClass = "";
        String neo4jLabel = "";

        try {
          //we can get the label with the method getLabel() only if this constraint is associated with a node
          //this is associated with a node

          neo4jLabel = neo4jConstraintDefinition.get("label").toString();
          orientDBIndexClass = neo4jLabel;

          //System.out.println("constraint label: " + orientDBIndexClass);

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

          String neo4jConstraintRelationshipType = neo4jConstraintDefinition.get("relationshipType").toString();
          orientDBIndexClass = neo4jConstraintRelationshipType;

          //System.out.println("constraint rel type: " + orientDBIndexClass);

          //create class orientDBIndexClass
          //there might be in fact cases where in neo4j the constraint as been defined, but no nodes have been created. As a result, no nodes of that class have been imported in OrientDB, so that class does not exist in Orient
          if (oDb.getRawGraph().getMetadata().getSchema().existsClass(orientDBIndexClass) == false) {
            oDb.createEdgeType(orientDBIndexClass);
          }
          isConstraintsOnNode = false;
          isConstraintsOnRelationship = true;
        } catch (IllegalStateException a) {
          //otherwise it is associated with a node
          //this is associated with a node. Do nothing
        }

        //we now know the type of this constraints and the class on which it is defined (orientDBIndexClass)

        //determine the property key on which the constraint has been defined
        Iterable<String> neo4jConstraintKeys = neo4jConstraintDefinition.keySet();
        for (final String myNeo4jConstraintPropKey : neo4jConstraintKeys) {
          neo4jPropKey = myNeo4jConstraintPropKey;
          //System.out.println(neo4jPropKey);
        }

        //to import this constraint, we first have to create the corresponding property in OrientDB
        propertyCreationSuccess = ONeo4jImporterUtils.createOrientDBProperty(session, neo4jLabel, orientDBIndexClass, neo4jPropKey, oDb,
            neo4jConstraintType);

        // now that the property has been created, we need to take actions based on the neo4jConstraintType
        if (propertyCreationSuccess) {

          //taking actions depending on the type of the constraints
          if ("UNIQUENESS".equals(neo4jConstraintType)) {

            counters.neo4jUniqueConstraintsCounter++;

            try {

              //unique constratins can be only on nodes. with this check we avoid odd things in very odd situations that may happen
              if (isConstraintsOnNode) {

                //we check that orientDBIndexClass is a vertex class; with this check we avoid odd things in very odd situations that may happen, e.g. node labels = rel types.  nodes with multiple types (that are imported in a single class). In such case it may happen that it try to create an index on the class, but that class does not exist as vertex class (because all nodes were imported into another class - but it exists as edge class)
                if (oDb.getRawGraph().getMetadata().getSchema().getClass(orientDBIndexClass)
                    .isSubClassOf(oDb.getRawGraph().getMetadata().getSchema().getClass("V"))) {

                  //we map Neo4j constraints of type UNIQUENESS to UNIQUE indices in Neo4j
                  OIndex OrientDBIndex = oDb.getRawGraph().getMetadata().getSchema().getClass(orientDBIndexClass)
                      .getProperty(neo4jPropKey).createIndex(OClass.INDEX_TYPE.UNIQUE);

                  //debug
                  //System.out.println("\nCreated index: " + OrientDBIndex);

                  counters.orientDBImportedUniqueConstraintsCounter++;
                  counters.orientDBImportedConstraintsCounter++;
                }
              }
            } catch (Exception e) {

              logString = "Found an error when trying to create a UNIQUE Index in OrientDB. Correspinding Property in Neo4j is '"
                  + neo4jPropKey + "' on node label '" + orientDBIndexClass + "': " + e.getMessage();
              ONeo4jImporter.importLogger.log(Level.SEVERE, logString);

              //github issue #3 - tries to create a not unique index as workaround
              try {

                logString = "Trying to create a NOT UNIQUE Index as workaround...";
                ONeo4jImporter.importLogger.log(Level.INFO, logString);

                OIndex OrientDBIndex = oDb.getRawGraph().getMetadata().getSchema().getClass(orientDBIndexClass)
                    .getProperty(neo4jPropKey).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

                counters.orientDBImportedNotUniqueWorkaroundCounter++;

              } catch (Exception ex) {
                ex.printStackTrace();
              }
            }
          }

          if ("NODE_PROPERTY_EXISTENCE".equals(neo4jConstraintType)) {
          }

          if ("RELATIONSHIP_PROPERTY_EXISTENCE ".equals(neo4jConstraintType)) {
          }
        }

        //print progress
        value = 100 * (counters.neo4jConstraintsCounter / counters.neo4jTotalConstraints);

        if ("UNIQUENESS".equals(neo4jConstraintType)) {
          keepLogString =
              df.format(counters.orientDBImportedUniqueConstraintsCounter) + " OrientDB UNIQUE Indices have been created";
        }
        if ("NODE_PROPERTY_EXISTENCE".equals(neo4jConstraintType)) {
        }
        if ("RELATIONSHIP_PROPERTY_EXISTENCE ".equals(neo4jConstraintType)) {
        }

        keepLogString = keepLogString + " (" + df.format(value) + "% done)";
        System.out.print("\r  " + keepLogString);
        value = 0;
      }

    } catch(Exception e) {
      e.printStackTrace();
    }
    return neo4jPropKey;
  }
}
