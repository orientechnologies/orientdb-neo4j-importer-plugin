/*
 *
 *  * Copyright 2014 Orient Technologies.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *  
 */
 
package com.orientechnologies.orient.neo4jimporter;

import java.text.DecimalFormat;

import java.io.File;
import java.io.IOException;
 
import java.util.Map;
import java.util.Map.Entry;
import java.util.Collection;
import java.util.logging.*;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.Schema; 
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;

import org.neo4j.io.fs.FileUtils;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;

import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManagerProxy;
import com.orientechnologies.orient.core.index.OSimpleKeyIndexDefinition;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty; 
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.OConstants;

import com.orientechnologies.common.log.OLogManager;

import com.orientechnologies.common.io.OFileUtils;

/**
* The main class of the ONeo4jImporter. It is instantiated from the ONeo4jImporterCommandLineParser
*
* Author: Santo Leto
*
* Start: 
*	- java -XX:MaxDirectMemorySize=2g -classpath "path_to_neo4j/lib/*;path_to_orientdb/lib/*;../" -Djava.util.logging.config.file=../log/orientdb-neo4j-importer-logs.properties com.orientechnologies.orient.ONeo4jImporter -neo4jlibdir="path_to_neo4j/lib" -neo4jdbdir="path_to_neo4j\data\databases\graph.db" 
*
* Description:
*	- This program migrates a Neo4j database into an OrientDB database. The migration consists of several phases:
*	-- Phase 1: Initialization of Neo4j and OrientDB Servers
*	-- Phase 2: Migration of vertices and edges
*	-- Phase 3: Schema migration
*	-- Phase 4: Shutdown of the server and summary info
*
* General details to keep in mind:
*	- In case a node in Neo4j has no Label, it will be imported in OrientDB in the Class "GenericClassNeo4jConversion"
*	- Original Neo4j IDs are stored as properties in the imported OrientDB vertices and edges ('Neo4jNodeID' for vertices and 'Neo4jRelID' for edges). Such properties can be (manually) removed at the end of the import, if not needed 
*	- During the import, an index is created on the property 'Neo4jNodeID' for all imported vertices Labels (Classes in OrientDB). This is to speed up vertices lookup during edge creation. The created indices can be (manually) removed at the end of the import, if not needed 
*	- In case a Neo4j Relationship has the same name of a Neo4j Label, e.g. "RelationshipName", the script will import into OrientDB that relationship in the class "E_RelationshipName" (i.e. prefixing the Neo4j relationship type with an "E_")
*	- During the creation of properties in OrientDB, Neo4j Char data type is mapped to a String data type
*
* Schema details to keep in mind:
*	- If in Neo4j there are no constraints or indices, the imported OrientDB database is schemaless
*	- If in Neo4j there are constraints or indices, the imported OrientDB database is schema-hybrid (with some properties defined). In particular, for any constraints and indices:
*		- The Neo4j property where the constraint or index is defined on is determined, and, using java 'instanceof' we determine the data type of this property (there's no way, in fact, to get the data type via the Neo4j API)
*		- Once we know the data type, we map it to an orientdb data type and we create a property with the corresponding data type
*	- If a Neo4j unique constraint is found, a corresponding unique index is created in OrientDB 
*   - If a Neo4j index is found, a corresponding (notunique) OrientDB index is created
*
* Limitations:
* 	- Currently only `local` migrations are allowed
*	- Schema limitations:
*	-- In case a node in Neo4j has multiple labels, only the first label is imported in OrientDB	
*	-- Neo4j Nodes with same label but different case, e.g. LABEL and LAbel will be aggregated into a single OrientDB vertex class 
*	-- Neo4j Relationship with same name but different case, e.g. relaTIONship and RELATIONSHIP will be aggregated into a single edge class  
*	-- Migration of Neo4j "existence" constraints (available only in Neo4j Enterprise) is not implemented 
*
*/

public class ONeo4jImporter {
	
	private final ONeo4jImporterSettings settings;
	
	public ONeo4jImporter (final ONeo4jImporterSettings settings) throws Exception {		
		this.settings = settings;
	}

   	public static void main(String[] args) {
		
		//
		String programName = "OrientDB's Neo4j Importer";		
		//
		
		//			
		System.out.println();		
        System.out.println(String.format(programName + " v.%s - %s", OConstants.getVersion(), OConstants.COPYRIGHT));		
		System.out.println();
		//
		
		//parses the command line parameters, and starts the import (.execute). Then exits
	    int returnValue = 1;
		try {
		  
		  final ONeo4jImporter neo4jImporter = ONeo4jImporterCommandLineParser.getNeo4jImporter(args);	
		  
		  returnValue = neo4jImporter.execute(programName);
		  
		} catch (Exception ex) {
		  System.err.println(ex.getMessage());
		}
		
		System.exit(returnValue);
		//
	
	}
	
	public int execute(String myProgramName) throws Exception {
		
		//
		int returnCode = 1;		
		String logString = "";		
		String keepLogString = "";				
		double startTime = System.currentTimeMillis();
		double value;
		
		boolean migrateRels = true;  
		boolean migrateNodes = true; 
		
		DecimalFormat df = new DecimalFormat("#");
		DecimalFormat dfd = new DecimalFormat("#.##");
		//
		
		logString = myProgramName + " - v." + OConstants.getVersion() + " started!\n";
		importLogger.log( Level.INFO, logString);	
		
		// parameters (from command line)
		boolean overwriteOrientDBDir = settings.overwriteOrientDbDir; 		
		String neo4jLibPath = settings.neo4jLibPath; //actually unused right now - but important to start the program from the command line 
		String neo4jDBPath = settings.neo4jDbPath;								
		String orientDbFolder = settings.orientDbDir;					
		//
		
		// check existance of orientDbFolder and takes action accordingly to option overwriteOrientDBDir		
		final File f = new File(orientDbFolder);
		if (f.exists()){
			if(overwriteOrientDBDir){
				
				logString = "Directory '" + orientDbFolder + "' exists already and the overwrite option '-o' is 'true'. Directory will be erased";
				importLogger.log( Level.WARNING, logString );
				
				OFileUtils.deleteRecursively(f);	
				
			} else {
				
				//we exit the program 
				logString = "ERROR: The directory '" + orientDbFolder + "' exists and the overwrite option '-o' is 'false' (default). Please delete the directory or run the program with the '-o true' option. Exiting";
				
				System.out.print(logString);
				System.out.print("\n\n");
				
				importLogger.log( Level.SEVERE, logString );
				
				System.exit(1);
				
			}
		}
		//
		
		
		
		//
		// PHASE 1 : INITIALIZATION
		//
		
		
				
		double initializationStartTime = System.currentTimeMillis();				
				
		//		
		System.out.println( "Please make sure that there are no running servers on:" );
		System.out.println( "  '" + neo4jDBPath + "' (Neo4j)" );
		System.out.println( "and:");
		System.out.println( "  '" + orientDbFolder + "' (OrientDB)" );
		//
		
		//
		System.out.println();
        System.out.print( "Initializing Neo4j..." );
		
		File DB_PATH = new File( neo4jDBPath );
		
		GraphDatabaseService neo4jGraphDb = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
		registerNeo4jShutdownHook( neo4jGraphDb );
		
		logString = "Initializing Neo4j...Done";
		
		System.out.print("\r" + logString + "\n");
		importLogger.log( Level.INFO, logString );
		//

		
		//
		System.out.println();
        System.out.print( "Initializing OrientDB..." );
				
		String dbUrl ="plocal:" + orientDbFolder;
		
	    OGlobalConfiguration.USE_WAL.setValue(false);		
		OGlobalConfiguration.WAL_SYNC_ON_PAGE_FLUSH.setValue(false);
				
		OrientGraphFactory oFactory = new OrientGraphFactory(dbUrl, "admin", "admin");
		oFactory.declareIntent(new OIntentMassiveInsert());
		OrientGraphNoTx oDb = oFactory.getNoTx();
		
		oDb.setStandardElementConstraints(false); 
		
		String orientVertexClass = "";
		
		logString = "Initializing OrientDB...Done";
		
        System.out.print("\r" + logString + "\n");
		importLogger.log( Level.INFO, logString );
		//
						
		//
		System.out.println();
        System.out.println( "Importing Neo4j database:");
		System.out.println( "  '" + neo4jDBPath + "'");
		System.out.println( "into OrientDB database:");
		System.out.println( "  '" + orientDbFolder + "'");
		//
		
		logString =  myProgramName + " - v." + OConstants.getVersion() + " - PHASE 1 completed!\n";
		importLogger.log( Level.INFO, logString );

		double initializationStopTime = System.currentTimeMillis();				
		
		
		
		//
		// PHASE 2 : MIGRATION OF VERTICES AND EDGES
		//
		
		
		
		//
		double neo4jNodeCounter=0L;
		double neo4jNodeNoLabelCounter=0L;
		double neo4jNodeMultipleLabelsCounter=0L;
		double orientDBImportedVerticesCounter=0L;
		double neo4jRelCounter=0L;
		double orientDBImportedEdgesCounter=0L;
		double neo4jConstraintsCounter=0L;
		double orientDBImportedConstraintsCounter=0L;
		double orientDBImportedUniqueConstraintsCounter=0L;
		double neo4jIndicesCounter=0L;
		double neo4jNonConstraintsIndicesCounter=0L;
		double neo4jInternalIndicesCounter=0L;
		double orientDBImportedIndicesCounter=0L;				
		double neo4jTotalNodes=0L;
		double neo4jTotalRels=0L;
		double neo4jTotalUniqueConstraints=0L;
		double neo4jTotalIndices=0L;						
		//
		
		//
		double importingNodesStartTime = 0L;		
		double importingNodesStopTime = 0L;		
		double internalIndicesStartTime = 0L;
		double orientDBVerticesClassCount = 0L;
		double internalIndicesStopTime = 0L;		
		//
				
		//gets all nodes from Neo4j and creates corresponding Vertices in OrientDB
		if (migrateNodes){
			
			logString = "Getting all Nodes from Neo4j and creating corresponding Vertices in OrientDB...";
			
			System.out.println();
			System.out.println( logString );
			
			importLogger.log( Level.INFO, logString );
			
			//counting Neo4j Nodes so that we can show a % on OrientDB vertices creation
			try (final Transaction tx = neo4jGraphDb.beginTx()) {
					
				Iterable<Node> neo4jNodes = neo4jGraphDb.getAllNodes();
				
				for (final Node myNode : neo4jNodes) {
					neo4jTotalNodes++;
				}
				
			}
			//
			
			importingNodesStartTime = System.currentTimeMillis();		
			
			try (final Transaction tx = neo4jGraphDb.beginTx()) {
					
				Iterable<Node> neo4jNodes =  neo4jGraphDb.getAllNodes();		
			
				for (final Node myNode : neo4jNodes) {
					
					neo4jNodeCounter++;
									
					//System.out.println(myNode); //debug 
					
					final Iterable<Label> nodeLabels = myNode.getLabels();
					
					//determines the class to use in OrientDB, starting from the original Neo4j label. First thing we check if this node has multiple labels 
					int q = 0;				
					for (final Label myLabel : nodeLabels) {		
						q++;	
						
						orientVertexClass = myLabel.name();
						
						//takes only the first label, in case of multi labels
						String[] parts = orientVertexClass.split(":");
						
						orientVertexClass = parts[0];
						
						if (parts.length >= 2) {
							
							neo4jNodeMultipleLabelsCounter++;
						
							
							//System.out.println("\nWARNING: Found node ('" + myNode + "') with multiple labels. Only the first (" + orientVertexClass + ") will be used as Class when importing this node in OrientDB");	
							
							logString = "Found node ('" + myNode + "') with multiple labels. Only the first (" + orientVertexClass + ") will be used as Class when importing this node in OrientDB";
							importLogger.log( Level.WARNING, logString );
								
													
						}
		
					}	
					
					// if q=0 the neo4j node has no label because q is incremented in the for cicly of the nodeLabels itarable
					if (q==0){
						
						neo4jNodeNoLabelCounter++;
						
						// set generic class for OrientDB 					
						orientVertexClass = "GenericClassNeo4jConversion";
						
						//System.out.println("\nWARNING: Found node ('" + myNode + "') with no labels. Class 'GenericClassNeo4jConversion' will be used when importing this node in OrientDB");	
						
						logString = "Found node ('" + myNode + "') with no labels. Class 'GenericClassNeo4jConversion' will be used when importing this node in OrientDB";
						importLogger.log( Level.WARNING, logString);
						
					}
					//
					
					//gets the node properties
					Map<String,Object> myNodeProperties = myNode.getAllProperties();
					
					//stores also the original neo4j nodeId in the property map - we will use it when creating the corresponding OrientDB vertex
					myNodeProperties.put("Neo4jNodeID", myNode.getId());
					
					//System.out.println (myNodeProperties); //debug 
					
					try {				
						// store the vertex on OrientDB 
						Vertex myVertex = oDb.addVertex("class:" + orientVertexClass, myNodeProperties);	
											
						//System.out.print(myVertex); //debug 
					
						orientDBImportedVerticesCounter++;
						
						value = 100.0 * (orientDBImportedVerticesCounter / neo4jTotalNodes);
						keepLogString = df.format(orientDBImportedVerticesCounter) + " OrientDB Vertices have been created (" + df.format(value) + "% done)";
						System.out.print("\r  " + keepLogString);		
						value =0;
					
					}catch (Exception e){
						
						logString = "Found an error when trying to store node ('" + myNode + "') to OrientDB: " + e.getMessage();
						importLogger.log( Level.SEVERE, logString);
											
					}
					
				}
				
				if (orientDBImportedVerticesCounter==0){					
					keepLogString = df.format(orientDBImportedVerticesCounter) + " OrientDB Vertices have been created";
					System.out.print("\r  " + keepLogString);												
				}
				
				//prints number of created vertices in the log 
				importLogger.log( Level.INFO, keepLogString );
				
				System.out.println ("\nDone");
			}

			importingNodesStopTime = System.currentTimeMillis();				
			//
			
			//creates an index on each OrientDB vertices class on Neo4jNodeID property - this will help in speeding up vertices lookup during relationships creation 		
			internalIndicesStartTime = System.currentTimeMillis();
			
			logString = "Creating internal Indices on property 'Neo4jNodeID' on all OrientDB Vertices Classes...";	
			
			System.out.println();
			System.out.println( logString );
			
			importLogger.log( Level.INFO, logString );	
			
			Collection<OClass> ClassCollection = oDb.getRawGraph().getMetadata().getSchema().getClass("V").getAllSubclasses();
			
			orientDBVerticesClassCount = (double)ClassCollection.size();
			
			for (OClass classCollectionElement : ClassCollection) {
				
				//System.out.println(classCollectionElement); //debug 
				
				try {
					
					//first create the property 
					oDb.getRawGraph().getMetadata().getSchema().getClass(classCollectionElement.getName()).createProperty("Neo4jNodeID",OType.LONG);
						
					//creates the index if the property creation was successfull
					try {
						
						oDb.getRawGraph().getMetadata().getSchema().getClass(classCollectionElement.getName()).getProperty("Neo4jNodeID").createIndex(OClass.INDEX_TYPE.UNIQUE); 		
						
						neo4jInternalIndicesCounter++;
											
						value = 100.0 * (neo4jInternalIndicesCounter / orientDBVerticesClassCount);
						keepLogString = df.format(neo4jInternalIndicesCounter) + " OrientDB Indices have been created (" + df.format(value) + "% done)";
						System.out.print("\r  " + keepLogString);			
						value =0;
						
					}catch (Exception e){
						
						logString = "Found an error when trying to create a UNIQUE Index in OrientDB on the 'Neo4jNodeID' Property of the vertices Class '" + classCollectionElement.getName() + "': " + e.getMessage();
						importLogger.log( Level.SEVERE, logString );
											
					}
				}catch (Exception e){
					
					logString = "Found an error when trying to create the 'Neo4jNodeID' Property in OrientDB on the vertices Class '" + classCollectionElement.getName() + "': " + e.getMessage();
					importLogger.log( Level.SEVERE, logString );
						
				}

			}		
			
			internalIndicesStopTime = System.currentTimeMillis();
			
			if (neo4jInternalIndicesCounter==0){						
				keepLogString = df.format(neo4jInternalIndicesCounter) + " OrientDB Indices have been created";
				System.out.print("\r  " + keepLogString);		
										
			}
				
			//prints number of created internal indices in the log 
			importLogger.log( Level.INFO, keepLogString );
						
			System.out.println ("\nDone");
			
		}
		//
			
			
		// gets all relationships from Neo4j and creates the corresponding Edges in OrientDB 
		
		double importingRelsStartTime = 0L;
		double importingRelsStopTime = 0L;
		
		if (migrateRels) {
			
			logString = "Getting all Relationships from Neo4j and creating corresponding Edges in OrientDB...";
			
			System.out.println();
			System.out.println( logString );
			
			importLogger.log( Level.INFO, logString );
					
			//counting Neo4j Relationships so that we can show a % on OrientDB Edges creation
			try (final Transaction tx = neo4jGraphDb.beginTx()) {
					
				Iterable<Relationship> neo4jRelationships =  neo4jGraphDb.getAllRelationships();	
				
				for (final Relationship myRelationship : neo4jRelationships) {
					neo4jTotalRels++;
				}
				
			}		
			//
			
			importingRelsStartTime = System.currentTimeMillis();
			
			try (final Transaction tx = neo4jGraphDb.beginTx()) {
					
				Iterable<Relationship> neo4jRelationships =  neo4jGraphDb.getAllRelationships();	
				
				for (final Relationship myRelationship : neo4jRelationships) {
					
					neo4jRelCounter++;
							
					//System.out.println(myRelationship);				
					
					RelationshipType myRelationshipType = myRelationship.getType();
					
					//get the relationship properties
					Map<String,Object> myRelationshipProperties = myRelationship.getAllProperties();
					
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
							Collection<OClass> checkClassCollection = oDb.getRawGraph().getMetadata().getSchema().getClass("V").getAllSubclasses();
							for (OClass classCollectionElement : checkClassCollection) {
								
								//debug 
								//System.out.println ("\n" + orientEdgeClass + " " + classCollectionElement.getName());
								
								if(orientEdgeClass.equalsIgnoreCase(classCollectionElement.getName())){
									//we have already a label on a vertex with the same name, changes the edge class by adding an "E_" prefix
																		
									//System.out.println ("\n\nWARNING: Found a Neo4j Relationship ('" + orientEdgeClass + "') with same name of a Neo4j node Label ('"+ classCollectionElement.getName() + "'). Importing this relationship in OrientDB as 'E_" + orientEdgeClass + "'\n");
									
									logString = "Found a Neo4j Relationship ('" + orientEdgeClass + "') with same name of a Neo4j node Label ('"+ classCollectionElement.getName() + "'). Importing this relationship in OrientDB as 'E_" + orientEdgeClass;
									importLogger.log( Level.WARNING, logString );
									
									orientEdgeClass = "E_" + orientEdgeClass;
								}	
							}						
							//
													
							// Converting map myRelationshipProperties to an Object[], so that it can be passed to addEdge method below
							// This will allow to create edges with a single create operation, instead of a create and update operation similar to the following:
								//OrientEdge myOrientEdge = oDb.addEdge("class:" + orientEdgeClass, myOutVertex, myInVertex, orientEdgeClass);
								//myOrientEdge.setProperties(myRelationshipProperties);								
							Object[] edgeProps = new Object[myRelationshipProperties.size()*2];
							int i=0;
							for(Map.Entry entry:myRelationshipProperties.entrySet()){
							   edgeProps[i++] = entry.getKey();
							   edgeProps[i++] = entry.getValue();
							} 
							//
							
							try{
								OrientEdge myOrientEdge = myOutOrientVertex.addEdge(orientEdgeClass, myInOrientVertex, edgeProps);
							
								orientDBImportedEdgesCounter++;
							
								//debug 
								//System.out.println("Orient:" + myOutOrientVertex.getProperty("Neo4jID") +"-"+ myRelationshipType.name()  +"->"+ myInOrientVertex.getProperty("Neo4jID"));
								
								value = 100 * ( orientDBImportedEdgesCounter / neo4jTotalRels );
								keepLogString = df.format(orientDBImportedEdgesCounter) + " OrientDB Edges have been created (" + df.format(value) + "% done)";
								System.out.print("\r  " + keepLogString);
								value = 0;
								
							} catch (Exception e) {
								
								logString = "Found an error when trying to create an Edge in OrientDB. Correspinding Relationship in Neo4j is '" + myRelationship + "': " + e.getMessage();
								importLogger.log( Level.SEVERE, logString );
							}													
						}					
					}
				}				
			}
			
			importingRelsStopTime = System.currentTimeMillis();
			
			if (orientDBImportedEdgesCounter==0){						
				keepLogString = df.format(orientDBImportedEdgesCounter) + " OrientDB Edges have been created";
				System.out.print("\r  " + keepLogString);		
										
			}
				
			//prints number of created edges in the log 
			importLogger.log( Level.INFO, keepLogString );			
			
			System.out.println ("\nDone");
		}		
		//
		
		//
		logString = myProgramName + " - v." + OConstants.getVersion() + " - PHASE 2 completed!\n";
		importLogger.log( Level.INFO, logString );		
		//
		
		
				
		//
		// PHASE 3 : SCHEMA MIGRATION 
		//
		
		
		
		//
		double importingSchemaStartTime = System.currentTimeMillis();
		
		String neo4jPropKey = "";				
		Label neo4jLabel = null;		
		boolean propertyCreationSuccess = false;
		//
				
		//
		logString = "Getting Constraints from Neo4j and creating corresponding ones in OrientDB...";
		
		System.out.println();
		System.out.println( logString );
		
		importLogger.log( Level.INFO, logString );		
		
		//counting Neo4j Constraints so that we can show a % on OrientDB Constraints creation
		try (final Transaction tx = neo4jGraphDb.beginTx()) {
			
			Schema neo4jSchema = neo4jGraphDb.schema();
			
			Iterable<ConstraintDefinition> Neo4jConstraintDefinition =  neo4jSchema.getConstraints();	
			for (final ConstraintDefinition myNeo4jConstraintDefinition : Neo4jConstraintDefinition) {	
				if("UNIQUENESS".equals(myNeo4jConstraintDefinition.getConstraintType().toString())){
					neo4jTotalUniqueConstraints++;
				}
			}
		}
		//
				
		try (final Transaction tx = neo4jGraphDb.beginTx()) {
			
			Schema neo4jSchema = neo4jGraphDb.schema();
			
			// getting all constraints and iterating
			Iterable<ConstraintDefinition> Neo4jConstraintDefinition =  neo4jSchema.getConstraints();	
			for (final ConstraintDefinition myNeo4jConstraintDefinition : Neo4jConstraintDefinition) {	
				
				neo4jConstraintsCounter++;
				
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
					if(oDb.getRawGraph().getMetadata().getSchema().existsClass(orientDBIndexClass) == false){
						oDb.createVertexType(orientDBIndexClass);						
					}
					
				} catch (IllegalStateException a) {
					//otherwise it is associated with a relationship
					//this is associated with a relationship. Do nothing
				}

				try{
					//we can get the relationship this constraint is associated with only if this is a relationship
					//this is associated with a relationship
					
					String myNeo4jConstraintRelationshipType = myNeo4jConstraintDefinition.getRelationshipType().name();
					
					orientDBIndexClass = myNeo4jConstraintRelationshipType;
					
					//System.out.println("constraint rel type: " + orientDBIndexClass);
					
					//create class orientDBIndexClass
					//there might be in fact cases where in neo4j the constraint as been defined, but no nodes have been created. As a result, no nodes of that class have been imported in OrientDB, so that class does not exist in Orient
					if(oDb.getRawGraph().getMetadata().getSchema().existsClass(orientDBIndexClass) == false){
						oDb.createEdgeType(orientDBIndexClass);						
					}
					
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
				propertyCreationSuccess = createOrientDBProperty( neo4jLabel, orientDBIndexClass, neo4jPropKey, neo4jGraphDb, oDb, neo4jConstraintType.toString() );
				
				// now that the property has been created, we need to take actions based on the neo4jConstraintType 				
				if (propertyCreationSuccess){									
				
					//taking actions depending on the type of the constraints 
					if("UNIQUENESS".equals(neo4jConstraintType.toString())){
												
						try {
							
							//we map Neo4j constraints of type UNIQUENESS to UNIQUE indices in Neo4j 
							OIndex OrientDBIndex = oDb.getRawGraph().getMetadata().getSchema().getClass(orientDBIndexClass).getProperty(neo4jPropKey).createIndex(OClass.INDEX_TYPE.UNIQUE); 				
							
							//debug
							//System.out.println("\nCreated index: " + OrientDBIndex);
						
							orientDBImportedUniqueConstraintsCounter++;
							orientDBImportedConstraintsCounter++;
							
							value = 100 * ( orientDBImportedUniqueConstraintsCounter / neo4jTotalUniqueConstraints );
							keepLogString = df.format(orientDBImportedUniqueConstraintsCounter) + " OrientDB UNIQUE Indices have been created (" + df.format(value) + "% done)";
							System.out.print("\r  " + keepLogString);
							value = 0;							
						
						}catch (Exception e){
							
							logString = "Found an error when trying to create a UNIQUE Index in OrientDB. Correspinding Property in Neo4j is '" + neo4jPropKey + "' on node label '" + orientDBIndexClass + "': " + e.getMessage();
							importLogger.log( Level.SEVERE, logString );
							
						}						
					}				
				}					
			}
		}

		if (orientDBImportedUniqueConstraintsCounter==0){						
			keepLogString = df.format(orientDBImportedUniqueConstraintsCounter) + " OrientDB UNIQUE Indices have been created";
			System.out.print("\r  " + keepLogString);		
									
		}
			
		//prints number of unique constraints in the log 
		importLogger.log( Level.INFO, keepLogString );	
			
		System.out.println ("\nDone");	
		//

		//
		logString = "Getting Indices from Neo4j and creating corresponding ones in OrientDB...";
		
		System.out.println();
		System.out.println( logString );
		
		importLogger.log( Level.INFO, logString );				
        		
		//counting Neo4j Indices so that we can show a % on OrientDB indices creation
		try (final Transaction tx = neo4jGraphDb.beginTx()) {
			
			Schema neo4jSchema = neo4jGraphDb.schema();
			
			Iterable<IndexDefinition> neo4jIndexDefinition =  neo4jSchema.getIndexes();	
			for (final IndexDefinition myNeo4jIndexDefinition : neo4jIndexDefinition) {					
				neo4jTotalIndices++;
			}
		}
		//
		
		try (final Transaction tx = neo4jGraphDb.beginTx()) {
			
			Schema neo4jSchema = neo4jGraphDb.schema();
			
			Iterable<IndexDefinition> neo4jIndexDefinition =  neo4jSchema.getIndexes();	
			for (final IndexDefinition myNeo4jIndexDefinition : neo4jIndexDefinition) {	
				
				neo4jIndicesCounter++;
				
				neo4jLabel = myNeo4jIndexDefinition.getLabel();					
				
				//the label this index is on (Neo4j indices are allowed on nodes only)				
				String myNeo4jLabelOfIndex = myNeo4jIndexDefinition.getLabel().name();
				
				//debug
				//System.out.println("all index: on label " + myNeo4jLabelOfIndex);
				
				//if the index is created as a side effect of the creation of a uniqueness constraint, we handled the case already above
				if (myNeo4jIndexDefinition.isConstraintIndex() == false){
					
					neo4jNonConstraintsIndicesCounter++;
					
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
								
					if(oDb.getRawGraph().getMetadata().getSchema().existsClass(myNeo4jLabelOfIndex) == false){
						oDb.createVertexType(myNeo4jLabelOfIndex);						
					}						
					//
					
					//creates in OrientDB the property this index is defined on
					propertyCreationSuccess = createOrientDBProperty( neo4jLabel, myNeo4jLabelOfIndex, neo4jPropKey, neo4jGraphDb, oDb, "NOT UNIQUE" );
					
					if (propertyCreationSuccess){									
						//index creation
						try{
							
							//creates the index 
							OIndex OrientDBIndex = oDb.getRawGraph().getMetadata().getSchema().getClass(myNeo4jLabelOfIndex).getProperty(neo4jPropKey).createIndex(OClass.INDEX_TYPE.NOTUNIQUE); 
														
							//System.out.println("\nCreated index: " + OrientDBIndex + "." + neo4jPropKey);
							
							orientDBImportedIndicesCounter++;
							
							value = 100 * ( orientDBImportedIndicesCounter / (neo4jTotalIndices - orientDBImportedUniqueConstraintsCounter) );
							keepLogString = df.format(orientDBImportedIndicesCounter) + " OrientDB Indices have been created (" + df.format(value) + "% done)";
							System.out.print("\r  " + keepLogString);
							value = 0;							
					
						}catch (Exception e){
							
							logString = "Found an error when trying to create a NOTUNIQUE Index in OrientDB. Correspinding Property in Neo4j is '" + neo4jPropKey + "' on node label '" + myNeo4jLabelOfIndex + "': " + e.getMessage();
							importLogger.log( Level.SEVERE, logString );
					
						}
						//
					}
				}
			}
		}
		
		double importingSchemaStopTime = System.currentTimeMillis();
		
		if (orientDBImportedIndicesCounter==0){						
			keepLogString = df.format(orientDBImportedIndicesCounter) + " OrientDB Indices have been created";
			System.out.print("\r  " + keepLogString);		
									
		}
			
		//prints number of unique constraints in the log 
		importLogger.log( Level.INFO, keepLogString );	
		
		System.out.println ("\nDone");	
		
		logString =  myProgramName + " - v." + OConstants.getVersion() + " - PHASE 3 completed!\n";
		importLogger.log( Level.INFO, logString );
		//

		
		//
		System.out.println();
        System.out.println( "Import completed!" );		
		//

		

		//
		// PHASE 4 : SHUTDOWN OF THE SERVERS AND SUMMARY INFO
		//


		
		//
		logString = "Shutting down OrientDB...";
		
		System.out.println();
		System.out.print( logString );
		
		importLogger.log( Level.INFO, logString );						
				
		oDb.shutdown();
		oFactory.close();	
		
		System.out.print( "\rShutting down OrientDB...Done" );		
		//

		//
        logString = "Shutting down Neo4j...";
		
		System.out.println();
		System.out.print( logString );
		
		importLogger.log( Level.INFO, logString );			

        neo4jGraphDb.shutdown();
		
		System.out.print( "\rShutting down Neo4j...Done" );
		System.out.println();
		//
		
		//
		double stopTime = System.currentTimeMillis();		
        double elapsedTime = (stopTime - startTime);
		double elapsedTimeSeconds = elapsedTime / (1000);
		
		double initializationElapsedTime = (initializationStopTime - initializationStartTime);
		double initializationElapsedTimeSeconds = initializationElapsedTime / (1000);
				
		double importingNodesElapsedTime = importingNodesStopTime - importingNodesStartTime;		
		double importingNodesElapsedTimeSeconds = importingNodesElapsedTime / (1000);
		
		double importingRelsElapsedTime = importingRelsStopTime - importingRelsStartTime;		
		double importingRelsElapsedTimeSeconds = importingRelsElapsedTime / (1000);
		
		double importingSchemaElapsedTime = importingSchemaStopTime - importingSchemaStartTime;		
		double importingSchemaElapsedTimeSeconds = importingSchemaElapsedTime / (1000);
		
		double internalIndicesElapsedTime = internalIndicesStopTime - internalIndicesStartTime;		
		double internalIndicesElapsedTimeSeconds = internalIndicesElapsedTime / (1000);
		//
		
		//
		System.out.println();
		System.out.println( "===============" );
        System.out.println( "Import Summary:" );
		System.out.println( "===============" );
		System.out.println();
		System.out.println( "- Found Neo4j Nodes                                                        : " + df.format(neo4jNodeCounter));
		System.out.println( "-- With at least one Label                                                 :  " + df.format((neo4jNodeCounter - neo4jNodeNoLabelCounter)));		
		System.out.println( "--- With multiple Labels                                                   :   " + df.format(neo4jNodeMultipleLabelsCounter));
		System.out.println( "-- Without Labels                                                          :  " + df.format(neo4jNodeNoLabelCounter));				
		  System.out.print( "- Imported OrientDB Vertices                                               : " + df.format(orientDBImportedVerticesCounter));
		if (neo4jNodeCounter>0){
			value=(orientDBImportedVerticesCounter/neo4jNodeCounter)*100;			
			System.out.print( " (" + df.format(value) + "%)");
			value=0;
		}
		
		System.out.println();
		System.out.println();
		System.out.println( "- Found Neo4j Relationships                                                : " + df.format(neo4jRelCounter));
		  System.out.print( "- Imported OrientDB Edges                                                  : " + df.format(orientDBImportedEdgesCounter));
		if(neo4jRelCounter > 0){
			value=(orientDBImportedEdgesCounter/neo4jRelCounter)*100;
			System.out.print (" (" + df.format(value) + "%)");
			value=0;
		}
		
		System.out.println();
		System.out.println();
		System.out.println( "- Found Neo4j Constraints                                                  : " + df.format(neo4jConstraintsCounter));
	      System.out.print( "- Imported OrientDB Constraints (UNIQUE Indices created)                   : " + df.format(orientDBImportedConstraintsCounter));
		if(neo4jConstraintsCounter>0){
			value = ( orientDBImportedConstraintsCounter / neo4jConstraintsCounter )*100;
			System.out.print( " (" + df.format(value) + "%)");
			value=0;
		}		
		
		System.out.println();		
		System.out.println();
		System.out.println( "- Found Neo4j (non-constraint) Indices                                     : " + df.format(neo4jNonConstraintsIndicesCounter));
		  System.out.print( "- Imported OrientDB Indices                                                : " + df.format(orientDBImportedIndicesCounter));
		if(neo4jNonConstraintsIndicesCounter>0){
			value=(orientDBImportedIndicesCounter/neo4jNonConstraintsIndicesCounter)*100;
			System.out.print( " (" + df.format(value) + "%)");	
			value=0;			
		}
		
		/*
		System.out.println();
		  System.out.print( "- Imported (in previous step) OrientDB indices                             : " + df.format(orientDBImportedUniqueConstraintsCounter));
		if(neo4jIndicesCounter>0){
			value = ( orientDBImportedUniqueConstraintsCounter / neo4jIndicesCounter )*100;
			System.out.print( " (" + df.format(value) + "%)");
			value=0;
		}
		*/		

		System.out.println();		
		System.out.println();
		System.out.println( "- Additional created Indices (on vertex properties 'Neo4jNodeID')          : " + df.format(neo4jInternalIndicesCounter));

		System.out.println();		
		System.out.println( "- Total Import time:                                                       : " + df.format(elapsedTimeSeconds) + " seconds");		
		
		System.out.println( "-- Initialization time                                                     :  " + df.format(initializationElapsedTimeSeconds) + " seconds");
		  System.out.print( "-- Time to Import Nodes                                                    :  " + df.format(importingNodesElapsedTimeSeconds) + " seconds");
		if(importingNodesElapsedTimeSeconds>0){
			value=(orientDBImportedVerticesCounter/importingNodesElapsedTimeSeconds);
			System.out.print( " (" + dfd.format(value) + " nodes/sec)");
			value=0;
		}		
		
		System.out.println();		
		  System.out.print( "-- Time to Import Relationships                                            :  " + df.format(importingRelsElapsedTimeSeconds) + " seconds");
		if(importingRelsElapsedTimeSeconds>0){
			value=(orientDBImportedEdgesCounter/importingRelsElapsedTimeSeconds);
			System.out.print( " (" + dfd.format(value) + " rels/sec)");
			value=0;
		}			

		System.out.println();		
		  System.out.print( "-- Time to Import Constraints and Indices                                  :  " + df.format(importingSchemaElapsedTimeSeconds) + " seconds");
		if(importingSchemaElapsedTimeSeconds>0){
			value=( ( orientDBImportedConstraintsCounter + orientDBImportedIndicesCounter ) / importingSchemaElapsedTimeSeconds );
			System.out.print( " (" + dfd.format(value) + " indices/sec)");
			value=0;		
		}
				
		System.out.println();		
		  System.out.print( "-- Time to create internal Indices (on vertex properties 'Neo4jNodeID')    :  " + df.format(internalIndicesElapsedTimeSeconds) + " seconds");
		if(internalIndicesElapsedTimeSeconds>0){
			value=( neo4jInternalIndicesCounter / internalIndicesElapsedTimeSeconds );
			System.out.print( " (" + dfd.format(value) + " indices/sec)");
			value=0;		
		}		
		
		System.out.println("\n");				
		//

		//				
		logString =  myProgramName + " - v." + OConstants.getVersion() + " - PHASE 4 completed!\n";
		importLogger.log( Level.INFO, logString );
		
		logString =  myProgramName + " - v." + OConstants.getVersion() + " - Import completed!\n";
		importLogger.log( Level.INFO, logString );
		//
		
		//
		returnCode = 0;
		return returnCode;
		//
		
    }
	
	private static void registerNeo4jShutdownHook( final GraphDatabaseService myNeo4jGraphDb )
	{
		Runtime.getRuntime().addShutdownHook( new Thread()
		{
			@Override
			public void run()
			{
				myNeo4jGraphDb.shutdown();
			}
		} );
	
	}
	
	private static boolean createOrientDBProperty(final Label myNeo4jLabel, final String myOrientDBIndexClass, final String myNeo4jPropKey, final GraphDatabaseService myNeo4jGraphDb, OrientGraphNoTx myOdb, final String myNeo4jConstraintType) {
				
		//To create a property in OrientDB first we need to understand the Neo4j property data type. 
		//To do this we will use java instanceof, as there are no specific methods in the noe4j api to get the data type of a property		
		//To be able to use instanceof, we first need to find a node that has that property 
		
		//
		String neo4jPropType = "";			
		OType orientOtype = null;       
		boolean foundNode = false; 
		long debugCounter = 0L;
		String logString = "";
		//
		
		//find a node that has this property, then get the data type of this property		
		try (final Transaction tx = myNeo4jGraphDb.beginTx()) {
					
			ResourceIterator<Node> neo4jNodes = myNeo4jGraphDb.findNodes(myNeo4jLabel);				
			try {

				 while ( neo4jNodes.hasNext() ) {
					
					debugCounter++;
					
					try {
						
						Node myNode = neo4jNodes.next();	
						
						//not all nodes with label myNeo4jLabel may have this property - even if we have a unique constraint on this property (it is unique in the nodes where the property exists). When we find a node with this property, we exit the loop				
						if (myNode.hasProperty(myNeo4jPropKey)){
							
							foundNode = true;
							
							//debug
							//System.out.println("Found node. debugCounter is: " + debugCounter + ". Working on node " + myNode); 
							
							Object PropertyValue = myNode.getProperty( myNeo4jPropKey, null );
							
							//get the Neo4j property data type 
							neo4jPropType = getNeo4jPropType(PropertyValue);
							
							//map the Neo4j property type to an OrientDB property data type 
							orientOtype = MapNeo4jToOrientDBPropertyType (neo4jPropType);
							
							//debug
							//System.out.println("Property defined on this node: " + myNeo4jPropKey + " value: " + PropertyValue + " data type: " + neo4jPropType); 
							
							break;
						}						
						
					}catch (Exception e){	
						
						logString = e.toString();
						importLogger.log( Level.WARNING, logString );
						
						break;
					}
					
				 }
			
			} finally {				
				
				neo4jNodes.close();
				
			}
			
		}
		//
										
		//Now that we know the data type of the property, we can create it in OrientDB 
		
		//However, there may be cases where the constraints has been defined, but no nodes have been created yet. In this case we cannot know the data type. We will use STRING as default 
		if (foundNode==false){	
			neo4jPropType = "Sting";		
			orientOtype	= OType.STRING;
		}
		
		//debug 
		//System.out.println("Creating OrientDB Property '" + myNeo4jPropKey + "' of type '" + orientOtype + "' on Class '" + myOrientDBIndexClass + "' "); 
					
		try{
				
			OProperty OrientDBProperty = myOdb.getRawGraph().getMetadata().getSchema().getClass(myOrientDBIndexClass).createProperty(myNeo4jPropKey,orientOtype);	
			
			if (foundNode==false){							
				
				logString = "The Neo4j Property '" + myNeo4jPropKey + "' on the Neo4j Label '" + myNeo4jLabel.name() + "' associated to a Neo4j '" + myNeo4jConstraintType + "' constraint/index has been imported as STRING because there are no nodes in Neo4j that have this property, hence it was not possible to determine the type of this Neo4j Property";
				importLogger.log( Level.INFO, logString );
				
				
			}else{
				
				logString = "Created Property '" + myNeo4jPropKey + "' on the Class '" + myOrientDBIndexClass + "' with type '" + orientOtype + "'";
				importLogger.log( Level.INFO, logString );
				
			}
			
			return true;
			
		}catch (Exception e){
			
			logString = "Found an error when trying to create a Property in OrientDB. Correspinding Property in Neo4j is '" + myNeo4jPropKey + "' on node label '" + myOrientDBIndexClass + "': " + e.getMessage();
			importLogger.log( Level.SEVERE, logString );
			
			return false;
		
		}		
		//	
		
	}
	
	private static OType MapNeo4jToOrientDBPropertyType (final String myNeo4jPropType) {
		
		OType myOrientOtype;
	  
		if (myNeo4jPropType == "String") {
				myOrientOtype=OType.STRING;	  
		} else if (myNeo4jPropType == "Integer") {								
				myOrientOtype=OType.INTEGER;
		} else if (myNeo4jPropType == "Long") {								
				myOrientOtype=OType.LONG;							
		} else if (myNeo4jPropType == "Boolean") {
				myOrientOtype=OType.BOOLEAN;
		} else if (myNeo4jPropType == "Byte") {
				myOrientOtype=OType.BYTE;									
		} else if (myNeo4jPropType == "Float") {
				myOrientOtype=OType.FLOAT;											
		} else if (myNeo4jPropType == "Double") {
				myOrientOtype=OType.DOUBLE;																				
		} else if (myNeo4jPropType == "Character") {
				myOrientOtype=OType.STRING; //mapping to String data type as OrientDB does not have a char
		} else if (myNeo4jPropType == "Short") {
				myOrientOtype=OType.SHORT;									
		} else {
				myOrientOtype=OType.STRING;									
		}	  
		
		return myOrientOtype;
		
	}

	private static String getNeo4jPropType (final Object myPropertyValue) {
		
		String myNeo4jPropType = "String";
		
		if (null == myPropertyValue || myPropertyValue instanceof String) {
				// System.out.println("found match: String");
				myNeo4jPropType="String";				
		} else if (myPropertyValue instanceof Integer) {
				// System.out.println("found match: Integer");
				myNeo4jPropType="Integer";				
		} else if (myPropertyValue instanceof Long) {
				// System.out.println("found match: Long");
				myNeo4jPropType="Long";				
		} else if (myPropertyValue instanceof Boolean) {
				// System.out.println("found match: Boolean");
				myNeo4jPropType="Boolean";				
		} else if (myPropertyValue instanceof Byte) {
				// System.out.println("found match: Byte");
				myNeo4jPropType="Byte";				
		} else if (myPropertyValue instanceof Float) {
				// System.out.println("found match: Float");
				myNeo4jPropType="Float";				
		} else if (myPropertyValue instanceof Double) {
				// System.out.println("found match: Double");
				myNeo4jPropType="Double";				
		} else if (myPropertyValue instanceof Character) {
				// System.out.println("found match: Character");
				myNeo4jPropType="Character";				
		} else if (myPropertyValue instanceof Short) {
				// System.out.println("found match: Short");
				myNeo4jPropType="Short";				
		} else {
				//System.out.println("no match found - return: String");
				myNeo4jPropType="String";				
		}	  
		
		return myNeo4jPropType;
		
	}
	
	private static final Logger importLogger = Logger.getLogger( "OrientDB.Neo4j.Importer" );
	
}
	
	