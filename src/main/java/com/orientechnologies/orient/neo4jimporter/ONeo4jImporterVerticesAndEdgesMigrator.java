package com.orientechnologies.orient.neo4jimporter;

import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

import java.text.DecimalFormat;
import java.util.Collection;
import java.util.Map;
import java.util.logging.Level;

import static com.orientechnologies.orient.neo4jimporter.ONeo4jImporter.*;

/**
 * Created by frank on 08/11/2016.
 */
class ONeo4jImporterVerticesAndEdgesMigrator {
  private final boolean                migrateRels;
  private final boolean                migrateNodes;
  private final boolean                relSampleOnly;  
  private final DecimalFormat          df;
  private final GraphDatabaseService   neo4jGraphDb;
  private       String                 keepLogString;
  private       String                 orientVertexClass;
  private       OrientGraphNoTx        oDb;
  private       ONeo4jImporterCounters counters;
  private       double                 importingRelsStartTime;
  private       double                 importingRelsStopTime;

  public ONeo4jImporterVerticesAndEdgesMigrator(String keepLogString, boolean migrateRels,
      boolean migrateNodes,
      DecimalFormat df,
      GraphDatabaseService neo4jGraphDb, String orientVertexClass, OrientGraphNoTx oDb, ONeo4jImporterCounters counters, boolean relSampleOnly) {
    this.keepLogString = keepLogString;
    this.migrateRels = migrateRels;	
    this.migrateNodes = migrateNodes;
	this.relSampleOnly = relSampleOnly;
    this.df = df;
    this.neo4jGraphDb = neo4jGraphDb;
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

  public ONeo4jImporterVerticesAndEdgesMigrator invoke() {
    String logString;
    double value;
	boolean hasMultipleLabels = false;
	//gets all nodes from Neo4j and creates corresponding Vertices in OrientDB
    if (migrateNodes) {

      logString = "Getting all Nodes from Neo4j and creating corresponding Vertices in OrientDB...";

      System.out.println();
      System.out.println(logString);

      ONeo4jImporter.importLogger.log(Level.INFO, logString);

      //counting Neo4j Nodes so that we can show a % on OrientDB vertices creation
      try (final Transaction tx = neo4jGraphDb.beginTx()) {

        Iterable<Node> neo4jNodes = neo4jGraphDb.getAllNodes();

        for (final Node myNode : neo4jNodes) {
          counters.neo4jTotalNodes++;
        }

      }
      //

      counters.importingNodesStartTime = System.currentTimeMillis();

      try (final Transaction tx = neo4jGraphDb.beginTx()) {

        Iterable<Node> neo4jNodes = neo4jGraphDb.getAllNodes();

        for (final Node myNode : neo4jNodes) {

          counters.neo4jNodeCounter++;

          //System.out.println(myNode); //debug

          final Iterable<Label> nodeLabels = myNode.getLabels();
		  
		  //
		  int i=0;
		  for (final Label myLabel : nodeLabels) {
			i++;
		  }
		  String[] multipleLabelsArray;
          multipleLabelsArray = new String[i];
		  //

          //determines the class to use in OrientDB, starting from the original Neo4j label. First thing we check if this node has multiple labels
          int q = 0;		  		    
		  String multipleLabelClass = "MultipleLabelNeo4jConversion";
          for (final Label myLabel : nodeLabels) {
            q++;
			
			if (q == 1){
				orientVertexClass = myLabel.name();							
				
			}
			multipleLabelsArray[q-1] = myLabel.name();

          }
		  
          if (q >= 2) {
			 
			 hasMultipleLabels = true;
			 
			 orientVertexClass = multipleLabelClass;
			 
             counters.neo4jNodeMultipleLabelsCounter++;
			 
             logString = "Found node ('" + myNode + "') with multiple labels. Only the first (" + orientVertexClass
                  + ") will be used as Class when importing this node in OrientDB";
             ONeo4jImporter.importLogger.log(Level.FINE, logString);

          }
		  
          // if q=0 the neo4j node has no label because q is incremented in the for cycle of the nodeLabels iterable
          if (q == 0) {

            counters.neo4jNodeNoLabelCounter++;

            // set generic class for OrientDB
            orientVertexClass = "GenericClassNeo4jConversion";

            logString = "Found node ('" + myNode
                + "') with no labels. Class 'GenericClassNeo4jConversion' will be used when importing this node in OrientDB";
            ONeo4jImporter.importLogger.log(Level.FINE, logString);

          }
          //

          //gets the node properties
          Map<String, Object> myNodeProperties = myNode.getAllProperties();

          //stores also the original neo4j nodeId in the property map - we will use it when creating the corresponding OrientDB vertex
          myNodeProperties.put("Neo4jNodeID", myNode.getId());
		  
		  //store also the original labels
		  myNodeProperties.put("Neo4jLabelList", multipleLabelsArray);

          //System.out.println (myNodeProperties); //debug

          try {
            // store the vertex on OrientDB
            Vertex myVertex = oDb.addVertex("class:" + orientVertexClass, myNodeProperties);

            //System.out.print(myVertex); //debug

            counters.orientDBImportedVerticesCounter++;
			
            value = 100.0 * (counters.orientDBImportedVerticesCounter / counters.neo4jTotalNodes);
            keepLogString =
                df.format(counters.orientDBImportedVerticesCounter) + " OrientDB Vertices have been created (" + df.format(value)
                    + "% done)";
            System.out.print("\r  " + keepLogString);
            value = 0;

          } catch (Exception e) {

            logString = "Found an error when trying to store node ('" + myNode + "') to OrientDB: " + e.getMessage();
            ONeo4jImporter.importLogger.log(Level.SEVERE, logString);

          }

        }

        if (counters.orientDBImportedVerticesCounter == 0) {
          keepLogString = df.format(counters.orientDBImportedVerticesCounter) + " OrientDB Vertices have been created";
          System.out.print("\r  " + keepLogString);
        }

        //prints number of created vertices in the log
        ONeo4jImporter.importLogger.log(Level.INFO, keepLogString);

        System.out.println("\nDone");
      }

      counters.importingNodesStopTime = System.currentTimeMillis();
      //

      //creates an index on each OrientDB vertices class on Neo4jNodeID property - this will help in speeding up vertices lookup during relationships creation
      counters.internalIndicesStartTime = System.currentTimeMillis();

      logString = "Creating internal Indices on properties 'Neo4jNodeID' & 'Neo4jLabelList' on all OrientDB Vertices Classes...";

      System.out.println();
      System.out.println(logString);

      ONeo4jImporter.importLogger.log(Level.INFO, logString);

      Collection<OClass> ClassCollection = oDb.getRawGraph().getMetadata().getSchema().getClass("V").getAllSubclasses();

      counters.orientDBVerticesClassCount = (double) ClassCollection.size();

      for (OClass classCollectionElement : ClassCollection) {

        //System.out.println(classCollectionElement); //debug

        try {

          //first create the property
          oDb.getRawGraph().getMetadata().getSchema().getClass(classCollectionElement.getName()).createProperty("Neo4jNodeID",
              OType.LONG);

          //creates the index if the property creation was successfull
          try {

            oDb.getRawGraph().getMetadata().getSchema().getClass(classCollectionElement.getName()).getProperty(
                "Neo4jNodeID").createIndex(OClass.INDEX_TYPE.UNIQUE);

            counters.neo4jInternalIndicesCounter++;

            value = 100.0 * (counters.neo4jInternalIndicesCounter / (counters.orientDBVerticesClassCount*2));
            keepLogString =
                df.format(counters.neo4jInternalIndicesCounter) + " OrientDB Indices have been created (" + df.format(value)
                    + "% done)";
            System.out.print("\r  " + keepLogString);
            value = 0;

          } catch (Exception e) {

            logString =
                "Found an error when trying to create a UNIQUE Index in OrientDB on the 'Neo4jNodeID' Property of the vertex Class '"
                    + classCollectionElement.getName() + "': " + e.getMessage();
            ONeo4jImporter.importLogger.log(Level.SEVERE, logString);

          }
        } catch (Exception e) {

          logString = "Found an error when trying to create the 'Neo4jNodeID' Property in OrientDB on the vertex Class '"
              + classCollectionElement.getName() + "': " + e.getMessage();
          ONeo4jImporter.importLogger.log(Level.SEVERE, logString);

        }
		
		// index on property Neo4jLabelList		
			try {

			  //first create the property
			  oDb.getRawGraph().getMetadata().getSchema().getClass(classCollectionElement.getName()).createProperty("Neo4jLabelList",
				  OType.EMBEDDEDLIST, OType.STRING);

			  //creates the index if the property creation was successfull
			  try {

				oDb.getRawGraph().getMetadata().getSchema().getClass(classCollectionElement.getName()).getProperty(
					"Neo4jLabelList").createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

				counters.neo4jInternalIndicesCounter++;

				value = 100.0 * (counters.neo4jInternalIndicesCounter / (counters.orientDBVerticesClassCount*2));
				keepLogString =
					df.format(counters.neo4jInternalIndicesCounter) + " OrientDB Indices have been created (" + df.format(value)
						+ "% done)";
				System.out.print("\r  " + keepLogString);
				value = 0;

			  } catch (Exception e) {

				logString =
					"Found an error when trying to create a NOT UNIQUE Index in OrientDB on the 'Neo4jLabelList' Property of the vertex Class '"
						+ classCollectionElement.getName() + "': " + e.getMessage();
				ONeo4jImporter.importLogger.log(Level.SEVERE, logString);

			  }
			} catch (Exception e) {

			  logString = "Found an error when trying to create the 'Neo4jLabelList' Property in OrientDB on the vertex Class '"
				  + classCollectionElement.getName() + "': " + e.getMessage();
			  ONeo4jImporter.importLogger.log(Level.SEVERE, logString);

			}		
		
		//

      }

      counters.internalIndicesStopTime = System.currentTimeMillis();

      if (counters.neo4jInternalIndicesCounter == 0) {
        keepLogString = df.format(counters.neo4jInternalIndicesCounter) + " OrientDB Indices have been created";
        System.out.print("\r  " + keepLogString);

      }

      //prints number of created internal indices in the log
      ONeo4jImporter.importLogger.log(Level.INFO, keepLogString);

      System.out.println("\nDone");

    }
    //

    // gets all relationships from Neo4j and creates the corresponding Edges in OrientDB

    importingRelsStartTime = 0L;
    importingRelsStopTime = 0L;

    if (migrateRels) {

      logString = "Getting all Relationships from Neo4j and creating corresponding Edges in OrientDB...";

      System.out.println();
      System.out.println(logString);

      ONeo4jImporter.importLogger.log(Level.INFO, logString);

      //counting Neo4j Relationships so that we can show a % on OrientDB Edges creation
      try (final Transaction tx = neo4jGraphDb.beginTx()) {

        Iterable<Relationship> neo4jRelationships = neo4jGraphDb.getAllRelationships();

        for (final Relationship myRelationship : neo4jRelationships) {
          counters.neo4jTotalRels++;
        }

      }
      //

      importingRelsStartTime = System.currentTimeMillis();

      try (final Transaction tx = neo4jGraphDb.beginTx()) {

        Iterable<Relationship> neo4jRelationships = neo4jGraphDb.getAllRelationships();

        for (final Relationship myRelationship : neo4jRelationships) {

          counters.neo4jRelCounter++;
		  
		//if (relSampleOnly){			  
		//	  if(counters.neo4jRelCounter == 1001){
		//		  break;
		//}			  
		//}

          //System.out.println(myRelationship);

          RelationshipType myRelationshipType = myRelationship.getType();

          //get the relationship properties
          Map<String, Object> myRelationshipProperties = myRelationship.getAllProperties();

          //store also the original neo4j relationship id
          myRelationshipProperties.put("Neo4jRelID", myRelationship.getId());

          //get the out node of this relationships
          Node neo4jOutNode = myRelationship.getStartNode();

          //get the in node of this relationships
          Node neo4jInNode = myRelationship.getEndNode();

          // so we have neo4jOutNode - myRelationship -> neo4jInNode

          //debug
          //System.out.println("Neo:" + neo4jOutNode +"-"+ myRelationshipType.name()  +"->"+ neo4jInNode);

          //lookup the corresponding OUTVertex in OrientDB
          Iterable<Vertex> outVertex = oDb.getVertices("Neo4jNodeID", neo4jOutNode.getId());
          for (final Vertex myOutVertex : outVertex) {

            //cast from Vertex to OrientVertex so that we can make use of more functionalities
            OrientVertex myOutOrientVertex = (OrientVertex) myOutVertex;

            //lookup the corresponding inVertex in OrientDB
            Iterable<Vertex> inVertex = oDb.getVertices("Neo4jNodeID", neo4jInNode.getId());
            for (final Vertex myInVertex : inVertex) {

              OrientVertex myInOrientVertex = (OrientVertex) myInVertex;

              String orientEdgeClass = myRelationshipType.name();

              //System.out.println ("\n" + orientEdgeClass);

              //if (orientEdgeClass != null) {
              //	if (orientEdgeClass.startsWith(":")){
              //		//remove :
              //		orientEdgeClass = orientEdgeClass.substring(1);
              //	}
              //}

              //in neo4j we can have labels on nodes and relationship with the same name
              //to handle this case, we append an E_ to the relationship name in case the relationship name is the same of a vertex class
              Collection<OClass> checkClassCollection = oDb.getRawGraph().getMetadata().getSchema().getClass(
                  "V").getAllSubclasses();
              for (OClass classCollectionElement : checkClassCollection) {

                //debug
                //System.out.println ("\n" + orientEdgeClass + " " + classCollectionElement.getName());

                if (orientEdgeClass.equalsIgnoreCase(classCollectionElement.getName())) {
                  //we have already a label on a vertex with the same name, changes the edge class by adding an "E_" prefix
					
					//prints just one warning per relationship type (fix for github issue #1)
					if(oDb.getRawGraph().getMetadata().getSchema().existsClass("E_" + orientEdgeClass) == false){
					
						logString = "Found a Neo4j Relationship Type ('" + orientEdgeClass + "') with same name of a Neo4j node Label ('"
							+ classCollectionElement.getName() + "'). Importing in OrientDB relationships of this type as 'E_" + orientEdgeClass;
						ONeo4jImporter.importLogger.log(Level.WARNING, logString);					
						
					}
					
					orientEdgeClass = "E_" + orientEdgeClass;
                }
              }
              //

              // Converting map myRelationshipProperties to an Object[], so that it can be passed to addEdge method below
              // This will allow to create edges with a single create operation, instead of a create and update operation similar to the following:
              //OrientEdge myOrientEdge = oDb.addEdge("class:" + orientEdgeClass, myOutVertex, myInVertex, orientEdgeClass);
              //myOrientEdge.setProperties(myRelationshipProperties);
              Object[] edgeProps = new Object[myRelationshipProperties.size() * 2];
              int i = 0;
              for (Map.Entry entry : myRelationshipProperties.entrySet()) {
                edgeProps[i++] = entry.getKey();
                edgeProps[i++] = entry.getValue();
              }
              //

              try {
                OrientEdge myOrientEdge = myOutOrientVertex.addEdge(orientEdgeClass, myInOrientVertex, edgeProps);

                counters.orientDBImportedEdgesCounter++;

                //debug
                //System.out.println("Orient:" + myOutOrientVertex.getProperty("Neo4jID") +"-"+ myRelationshipType.name()  +"->"+ myInOrientVertex.getProperty("Neo4jID"));

                value = 100 * (counters.orientDBImportedEdgesCounter / counters.neo4jTotalRels);
                keepLogString =
                    df.format(counters.orientDBImportedEdgesCounter) + " OrientDB Edges have been created (" + df.format(value)
                        + "% done)";
                System.out.print("\r  " + keepLogString);
                value = 0;

              } catch (Exception e) {

                logString = "Found an error when trying to create an Edge in OrientDB. Corresponding Relationship in Neo4j is '"
                    + myRelationship + "': " + e.getMessage();
                ONeo4jImporter.importLogger.log(Level.SEVERE, logString);
              }
            }
          }
        }
      }

      importingRelsStopTime = System.currentTimeMillis();

      if (counters.orientDBImportedEdgesCounter == 0) {
        keepLogString = df.format(counters.orientDBImportedEdgesCounter) + " OrientDB Edges have been created";
        System.out.print("\r  " + keepLogString);

      }

      //prints number of created edges in the log
      ONeo4jImporter.importLogger.log(Level.INFO, keepLogString);

      System.out.println("\nDone");
    }
    //

    //
    logString = PROGRAM_NAME + " - v." + OConstants.getVersion() + " - PHASE 2 completed!\n";
    ONeo4jImporter.importLogger.log(Level.INFO, logString);
    //
    return this;
  }
}
