package com.orientechnologies.orient.neo4jimporter;

import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.Vertex;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;

import java.text.DecimalFormat;
import java.util.logging.Level;

import static com.orientechnologies.orient.neo4jimporter.ONeo4jImporter.*;

/**
 * Created by frank on 08/11/2016.
 */
class ONeo4jImporterSchemaMigrator {
  private String                 keepLogString;
  private DecimalFormat          df;
  private GraphDatabaseService   neo4jGraphDb;
  private OrientGraphNoTx        oDb;
  private ONeo4jImporterCounters counters;
  private double                 importingSchemaStartTime;
  private double                 importingSchemaStopTime;

  public ONeo4jImporterSchemaMigrator(String keepLogString, DecimalFormat df,
      GraphDatabaseService neo4jGraphDb,
      OrientGraphNoTx oDb, ONeo4jImporterCounters counters) {
    this.keepLogString = keepLogString;
    this.df = df;
    this.neo4jGraphDb = neo4jGraphDb;
    this.oDb = oDb;
    this.counters = counters;
  }

  public double getImportingSchemaStartTime() {
    return importingSchemaStartTime;
  }

  public double getImportingSchemaStopTime() {
    return importingSchemaStopTime;
  }

  public ONeo4jImporterSchemaMigrator invoke() {
    String logString;
    double value;
    importingSchemaStartTime = System.currentTimeMillis();

    String neo4jPropKey = "";
	Boolean isConstraintsOnNode = false;
	Boolean isConstraintsOnRelationship = false;
	
    Label neo4jLabel = null;
    boolean propertyCreationSuccess = false;
    //

    //
    logString = "Getting Constraints from Neo4j and creating corresponding ones in OrientDB...";

    System.out.println();
    System.out.println(logString);

    ONeo4jImporter.importLogger.log(Level.INFO, logString);

    //counting Neo4j Constraints so that we can show a % on OrientDB Constraints creation
    try (final Transaction tx = neo4jGraphDb.beginTx()) {

      Schema neo4jSchema = neo4jGraphDb.schema();

      Iterable<ConstraintDefinition> Neo4jConstraintDefinition = neo4jSchema.getConstraints();
      for (final ConstraintDefinition myNeo4jConstraintDefinition : Neo4jConstraintDefinition) {
        if ("UNIQUENESS".equals(myNeo4jConstraintDefinition.getConstraintType().toString())) {
          counters.neo4jTotalUniqueConstraints++;
        }
      }
    }
    //

    try (final Transaction tx = neo4jGraphDb.beginTx()) {

      Schema neo4jSchema = neo4jGraphDb.schema();

      // getting all constraints and iterating
      Iterable<ConstraintDefinition> Neo4jConstraintDefinition = neo4jSchema.getConstraints();
      for (final ConstraintDefinition myNeo4jConstraintDefinition : Neo4jConstraintDefinition) {

        counters.neo4jConstraintsCounter++;

        //determine the type of the constaints - different actions will need to be taken according to this type
        ConstraintType neo4jConstraintType = myNeo4jConstraintDefinition.getConstraintType();
        //System.out.println(neo4jConstraintType); //Can be: NODE_PROPERTY_EXISTENCE, RELATIONSHIP_PROPERTY_EXISTENCE, UNIQUENESS (on nodes only)

        //determine the class where the constraints will be added in OrientDB
        //Neo4j allows constraints on both nodes and relationship. To get the OrientDB class, we have to separate the cases
        String orientDBIndexClass = "";

        try {
          //we can get the label with the method getLabel() only if this constraint is associated with a node
          //this is associated with a node

          neo4jLabel = myNeo4jConstraintDefinition.getLabel();
          orientDBIndexClass = neo4jLabel.name();

          //System.out.println("constraint label: " + orientDBIndexClass);

          //create class orientDBIndexClass
          //there might be in fact cases where in neo4j the constraint as been defined, but no nodes have been created. As a result, no nodes of that class have been imported in OrientDB, so that class does not exist in Orient
          if (oDb.getRawGraph().getMetadata().getSchema().existsClass(orientDBIndexClass) == false) {
            
			//cheks if this labels has been imported in the MultipleLabelNeo4jConversion label			
			//If yes:
			//1. the vertex class is not created as part of the schema migration 
			//2. its indices (and corresponding properties) are created in MultipleLabelNeo4jConversion			
			String sqlQuery = "SELECT @rid FROM V WHERE @class = 'MultipleLabelNeo4jConversion' AND Neo4jLabelList CONTAINS '" + orientDBIndexClass + "' LIMIT 1";
						
			int u = 0;			
			for (Vertex v : (Iterable<Vertex>) oDb.command(
					new OCommandSQL(sqlQuery)).execute()) {
				u++;
				//System.out.println("\n\n\nfound it: " + orientDBIndexClass + " " + v); //debug
				orientDBIndexClass = "MultipleLabelNeo4jConversion";				
			}
			
			if (u == 0) {			
				//System.out.println("\n\n\ndid not find it: " + orientDBIndexClass); //debug 
				oDb.createVertexType(orientDBIndexClass);
				
				// in order to improve record lookup when filtering on Neo4j labels all classes must have the Neo4jLabelList properties and an index on it
				// when classes are created during nodes migration, this property is created automatically
				// here we need to create it for those additional classes that are created (empty) during schema migration
				
				// index on property Neo4jLabelList		
				try {

				  //first create the property
				  oDb.getRawGraph().getMetadata().getSchema().getClass(orientDBIndexClass).createProperty("Neo4jLabelList",
					  OType.EMBEDDEDLIST, OType.STRING);

				  //creates the index if the property creation was successfull
				  try {

					oDb.getRawGraph().getMetadata().getSchema().getClass(orientDBIndexClass).getProperty(
						"Neo4jLabelList").createIndex(OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX);

				  } catch (Exception e) {

					logString =
						"Found an error when trying to create a NOT UNIQUE Index in OrientDB on the 'Neo4jLabelList' Property of the vertex Class '"
							+ orientDBIndexClass + "': " + e.getMessage();
					ONeo4jImporter.importLogger.log(Level.SEVERE, logString);

				  }
				} catch (Exception e) {

				  logString = "Found an error when trying to create the 'Neo4jLabelList' Property in OrientDB on the vertex Class '"
					  + orientDBIndexClass + "': " + e.getMessage();
				  ONeo4jImporter.importLogger.log(Level.SEVERE, logString);

				}				
				//				
				
			}
          }
		  
		  isConstraintsOnNode = true;
		  isConstraintsOnRelationship = false;

        } catch (IllegalStateException a) {
          //otherwise it is associated with a relationship
          //this is associated with a relationship. Do nothing
        }

        try {
          //we can get the relationship this constraint is associated with only if this is a relationship
          //this is associated with a relationship

          String myNeo4jConstraintRelationshipType = myNeo4jConstraintDefinition.getRelationshipType().name();

          orientDBIndexClass = myNeo4jConstraintRelationshipType;

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
        //

        //we now know the type of this constraints and the class on which it is defined (orientDBIndexClass)

        //determine the property key on which the constraint has been defined
        Iterable<String> myNeo4jConstraintPropertyKeys = myNeo4jConstraintDefinition.getPropertyKeys();
        for (final String myNeo4jConstraintPropKey : myNeo4jConstraintPropertyKeys) {
          neo4jPropKey = myNeo4jConstraintPropKey;

          //System.out.println(neo4jPropKey);
        }

        //to import this constraint, we first have to create the corresponding property in OrientDB
        propertyCreationSuccess = ONeo4jImporterUtils.createOrientDBProperty(neo4jLabel, orientDBIndexClass, neo4jPropKey, neo4jGraphDb,
            oDb,
            neo4jConstraintType.toString());

        // now that the property has been created, we need to take actions based on the neo4jConstraintType
        if (propertyCreationSuccess) {

          //taking actions depending on the type of the constraints
          if ("UNIQUENESS".equals(neo4jConstraintType.toString())) {

            try {
			
			  //unique constratins can be only on nodes. with this check we avoid odd things in very odd situations that may happen
			  if (isConstraintsOnNode) {
				  
				  //we check that orientDBIndexClass is a vertex class; with this check we avoid odd things in very odd situations that may happen, e.g. node labels = rel types.  nodes with multiple types (that are imported in a single class). In such case it may happen that it try to create an index on the class, but that class does not exist as vertex class (because all nodes were imported into another class - but it exists as edge class)
				  if (oDb.getRawGraph().getMetadata().getSchema().getClass(orientDBIndexClass).isSubClassOf(oDb.getRawGraph().getMetadata().getSchema().getClass("V"))){			  
			  
					  //we map Neo4j constraints of type UNIQUENESS to UNIQUE indices in Neo4j
					  OIndex OrientDBIndex = oDb.getRawGraph().getMetadata().getSchema().getClass(orientDBIndexClass).getProperty(
						  neo4jPropKey).createIndex(OClass.INDEX_TYPE.UNIQUE);

					  //debug
					  //System.out.println("\nCreated index: " + OrientDBIndex);

					  counters.orientDBImportedUniqueConstraintsCounter++;
					  counters.orientDBImportedConstraintsCounter++;

					  value = 100 * (counters.orientDBImportedUniqueConstraintsCounter / counters.neo4jTotalUniqueConstraints);
					  keepLogString =
						  df.format(counters.orientDBImportedUniqueConstraintsCounter) + " OrientDB UNIQUE Indices have been created ("
							  + df.format(
							  value) + "% done)";
					  System.out.print("\r  " + keepLogString);
					  value = 0;
				  }
			  
			  }

            } catch (Exception e) {

              logString = "Found an error when trying to create a UNIQUE Index in OrientDB. Correspinding Property in Neo4j is '"
                  + neo4jPropKey + "' on node label '" + orientDBIndexClass + "': " + e.getMessage();
              ONeo4jImporter.importLogger.log(Level.SEVERE, logString);
			  
			  //github issue #3 - tries to create a not unique index as workaround
			  try{
				  
				logString = "Trying to create a NOT UNIQUE Index as workaround...";
				ONeo4jImporter.importLogger.log(Level.INFO, logString);  
				  
				OIndex OrientDBIndex = oDb.getRawGraph().getMetadata().getSchema().getClass(orientDBIndexClass).getProperty(
						  neo4jPropKey).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
						  
				counters.orientDBImportedNotUniqueWorkaroundCounter++;
				
			  } catch (Exception ex) {
				  
			  }
			  //

            }
          }
        }
      }
    }

    if (counters.orientDBImportedUniqueConstraintsCounter == 0) {
      keepLogString = df.format(counters.orientDBImportedUniqueConstraintsCounter) + " OrientDB UNIQUE Indices have been created";
      System.out.print("\r  " + keepLogString);

    }

    //prints number of unique constraints in the log
    ONeo4jImporter.importLogger.log(Level.INFO, keepLogString);

    System.out.println("\nDone");
    //

    //
    logString = "Getting Indices from Neo4j and creating corresponding ones in OrientDB...";

    System.out.println();
    System.out.println(logString);

    ONeo4jImporter.importLogger.log(Level.INFO, logString);

    //counting Neo4j Indices so that we can show a % on OrientDB indices creation
    try (final Transaction tx = neo4jGraphDb.beginTx()) {

      Schema neo4jSchema = neo4jGraphDb.schema();

      Iterable<IndexDefinition> neo4jIndexDefinition = neo4jSchema.getIndexes();
      for (final IndexDefinition myNeo4jIndexDefinition : neo4jIndexDefinition) {
        counters.neo4jTotalIndices++;
      }
    }
    //

    try (final Transaction tx = neo4jGraphDb.beginTx()) {

      Schema neo4jSchema = neo4jGraphDb.schema();

      Iterable<IndexDefinition> neo4jIndexDefinition = neo4jSchema.getIndexes();
      for (final IndexDefinition myNeo4jIndexDefinition : neo4jIndexDefinition) {

        counters.neo4jIndicesCounter++;

        neo4jLabel = myNeo4jIndexDefinition.getLabel();

        //the label this index is on (Neo4j indices are allowed on nodes only)
        String myNeo4jLabelOfIndex = myNeo4jIndexDefinition.getLabel().name();

        //debug
        //System.out.println("all index: on label " + myNeo4jLabelOfIndex);

        //if the index is created as a side effect of the creation of a uniqueness constraint, we handled the case already above
        if (myNeo4jIndexDefinition.isConstraintIndex() == false) {

          counters.neo4jNonConstraintsIndicesCounter++;

          //debug
          //System.out.println("non constraint index: on label " + myNeo4jLabelOfIndex);

          neo4jPropKey = "";

          //gets the property this index is on
          Iterable<String> myNeo4jIndexPropertyKeys = myNeo4jIndexDefinition.getPropertyKeys();
          for (final String myNeo4jIndexPropKey : myNeo4jIndexPropertyKeys) {

            neo4jPropKey = myNeo4jIndexPropKey;

            //System.out.println("on property: " + neo4jPropKey);

          }

          //create the index in OrientDB - we create NOT UNIQUE indices here (case of UNIQUE indices is handled above)

          //create class myNeo4jLabelOfIndex
          //there might be in fact cases where in neo4j the index as been defined, but no nodes have been created. As a result, no nodes of that class have been imported in OrientDB, so that class does not exist in Orient

          if (oDb.getRawGraph().getMetadata().getSchema().existsClass(myNeo4jLabelOfIndex) == false) {
            oDb.createVertexType(myNeo4jLabelOfIndex);
          }
          //

          //creates in OrientDB the property this index is defined on
          propertyCreationSuccess = ONeo4jImporterUtils.createOrientDBProperty(neo4jLabel, myNeo4jLabelOfIndex, neo4jPropKey,
              neo4jGraphDb, oDb,
              "NOT UNIQUE");

          if (propertyCreationSuccess) {
            //index creation
            try {

              //creates the index
              OIndex OrientDBIndex = oDb.getRawGraph().getMetadata().getSchema().getClass(myNeo4jLabelOfIndex).getProperty(
                  neo4jPropKey).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

              //System.out.println("\nCreated index: " + OrientDBIndex + "." + neo4jPropKey);

              counters.orientDBImportedIndicesCounter++;

              value = 100 * (counters.orientDBImportedIndicesCounter / (counters.neo4jTotalIndices
                  - counters.orientDBImportedUniqueConstraintsCounter));
              keepLogString =
                  df.format(counters.orientDBImportedIndicesCounter) + " OrientDB Indices have been created (" + df.format(value)
                      + "% done)";
              System.out.print("\r  " + keepLogString);
              value = 0;

            } catch (Exception e) {

              logString =
                  "Found an error when trying to create a NOTUNIQUE Index in OrientDB. Correspinding Property in Neo4j is '"
                      + neo4jPropKey + "' on node label '" + myNeo4jLabelOfIndex + "': " + e.getMessage();
              ONeo4jImporter.importLogger.log(Level.SEVERE, logString);

            }
            //
          }
        }
      }
    }

    importingSchemaStopTime = System.currentTimeMillis();

    if (counters.orientDBImportedIndicesCounter == 0) {
      keepLogString = df.format(counters.orientDBImportedIndicesCounter) + " OrientDB Indices have been created";
      System.out.print("\r  " + keepLogString);

    }

    //prints number of unique constraints in the log
    ONeo4jImporter.importLogger.log(Level.INFO, keepLogString);

    System.out.println("\nDone");

    logString = PROGRAM_NAME + " - v." + OConstants.getVersion() + " - PHASE 3 completed!\n";
    ONeo4jImporter.importLogger.log(Level.INFO, logString);
    //

    //
    System.out.println();
    System.out.println("Import completed!");
    //
    return this;
  }
}
