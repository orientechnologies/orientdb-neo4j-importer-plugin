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
*	- If a Neo4j "existence" constraint is found (available only in Neo4j Enterprise) --> this case is still to be implemented
*   - If a Neo4j index is found, a corresponding (notunique) OrientDB index is created
*
* Limitations:
* 	- Currently only `local` migrations are allowed
*	- Schema limitations:
*	-- In case a node in Neo4j has multiple labels, only the first label is imported in OrientDB	
*	-- Neo4j Nodes with same label but different case, e.g. LABEL and LAbel will be aggregated into a single OrientDB vertex class 
*	-- Neo4j Relationship with same name but different case, e.g. relaTIONship and RELATIONSHIP will be aggregated into a single edge class  
*
*/

public class ONeo4jImporter {
	
	private final ONeo4jImporterSettings settings;
	
	public ONeo4jImporter (final ONeo4jImporterSettings settings) throws Exception {		
		this.settings = settings;
	}

   	public static void main(String[] args) {
		
		//
		String ProgramName = "OrientDB's Neo4j Importer";		
		//
		
		//			
		System.out.println();		
        System.out.println(String.format(ProgramName + " v.%s - %s", OConstants.getVersion(), OConstants.COPYRIGHT));		
		System.out.println();
		//
				
	    int returnValue = 1;
		try {
		  
		  final ONeo4jImporter neo4jImporter = ONeo4jImporterCommandLineParser.getNeo4jImporter(args);	
		  
		  returnValue = neo4jImporter.execute(ProgramName);
		} catch (Exception ex) {
		  System.err.println(ex.getMessage());
		}
		System.exit(returnValue);
	
	}
	
	public int execute(String myProgramName) throws Exception {
		
		ImportLogger.log( Level.INFO, myProgramName + " - v." + OConstants.getVersion() + " started!\n" );		
		
		//
		int returnCode = 1;		
		String logString ="";		
		double startTime = System.currentTimeMillis();
		double value;
		
		DecimalFormat df = new DecimalFormat("#");
		DecimalFormat dfd = new DecimalFormat("#.##");
		//
		
		// parameters (from command line)
		boolean overwriteOrientDBDir = settings.overwriteOrientDbDir; 		
		String Neo4jLibPath = settings.Neo4jLibPath; //actually unused right now - but important to start the program from the command line 
		String Neo4jDBPath = settings.Neo4jDbPath;								
		String OrientDbFolder = settings.OrientDbDir;					
		//
		
		// check existance of OrientDbFolder and takes action accordingly to option overwriteOrientDBDir		
		final File f = new File(OrientDbFolder);
		if (f.exists()){
			if(overwriteOrientDBDir){
				ImportLogger.log( Level.WARNING, "Directory '" + OrientDbFolder + "' exists already and the overwrite option '-o' is 'true'. Directory will be erased" );
				
				OFileUtils.deleteRecursively(f);		
			} else {
				
				//we exit the program 
				logString = "ERROR: The directory '" + OrientDbFolder + "' exists and the overwrite option '-o' is 'false' (default). Please delete the directory or run the program with the '-o true' option. Exiting";
				
				System.out.print(logString);
				System.out.print("\n\n");
				
				ImportLogger.log( Level.SEVERE, logString );
				
				System.exit(1);
				
			}
		}
		//
		
		
		
		//
		// PHASE 1 : INITIALIZATION
		//
		
		
				
		double InitializationStartTime = System.currentTimeMillis();				
				
		//		
		System.out.println( "Please make sure that there are no running servers on:" );
		System.out.println( "  '" + Neo4jDBPath + "' (Neo4j)" );
		System.out.println( "and:");
		System.out.println( "  '" + OrientDbFolder + "' (OrientDB)" );
		//
		
		//
		System.out.println();
        System.out.print( "Initializing Neo4j..." );
		
		File DB_PATH = new File( Neo4jDBPath );
		
		GraphDatabaseService Neo4jGraphDb = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
		registerNeo4jShutdownHook( Neo4jGraphDb );
		
		System.out.print("\rInitializing Neo4j...Done\n");
		//

		
		//
		System.out.println();
        System.out.print( "Initializing OrientDB..." );
				
		String dbUrl ="plocal:" + OrientDbFolder;
		
		//OLogManager.instance().setConsoleLevel("OFF");
		
	    OGlobalConfiguration.USE_WAL.setValue(false);		
		OGlobalConfiguration.WAL_SYNC_ON_PAGE_FLUSH.setValue(false);
				
		OrientGraphFactory Ofactory = new OrientGraphFactory(dbUrl, "admin", "admin");
		Ofactory.declareIntent(new OIntentMassiveInsert());
		OrientGraphNoTx Odb = Ofactory.getNoTx();
		
		String OrientVertexClass ="";
		
        System.out.print("\rInitializing OrientDB...Done\n");
		//
						
		//
		System.out.println();
        System.out.println( "Importing Neo4j database:");
		System.out.println( "  '" + Neo4jDBPath + "'");
		System.out.println( "into OrientDB database:");
		System.out.println( "  '" + OrientDbFolder + "'");
		//
		
		logString =  myProgramName + " - v." + OConstants.getVersion() + " - PHASE 1 completed!\n";
		ImportLogger.log( Level.INFO, logString  );

		double InitializationStopTime = System.currentTimeMillis();				
		
		
		
		//
		// PHASE 2 : MIGRATION OF VERTICES AND EDGES
		//
		
		
		
		
		//gets all nodes from Neo4j and creates corresponding Vertices in OrientDB
		System.out.println();
        System.out.println( "Getting all Nodes from Neo4j and creating corresponding Vertices in OrientDB..." );
				
		double Neo4jNodeCounter=0L;
		double Neo4jNodeNoLabelCounter=0L;
		double Neo4jNodeMultipleLabelsCounter=0L;
		double OrientDBImportedVerticesCounter=0L;
		double Neo4jRelCounter=0L;
		double OrientDBImportedEdgesCounter=0L;
		double Neo4jConstraintsCounter=0L;
		double OrientDBImportedConstraintsCounter=0L;
		double OrientDBImportedUniqueConstraintsCounter=0L;
		double Neo4jIndicesCounter=0L;
		double Neo4jNonConstraintsIndicesCounter=0L;
		double Neo4jInternalIndicesCounter=0L;
		double OrientDBImportedIndicesCounter=0L;				
		double Neo4jTotalNodes=0L;
		double Neo4jTotalRels=0L;
		double Neo4jTotalUniqueConstraints=0L;
		double Neo4jTotalIndices=0L;
		
		//counting Neo4j Nodes so that we can show a % on OrientDB vertices creation
		try (final Transaction tx = Neo4jGraphDb.beginTx()) {
				
		    Iterable<Node> Neo4jNodes =  Neo4jGraphDb.getAllNodes();
			
			for (final Node myNode : Neo4jNodes) {
				Neo4jTotalNodes++;
			}
			
		}
		//
		
		double ImportingNodesStartTime = System.currentTimeMillis();		
		
		try (final Transaction tx = Neo4jGraphDb.beginTx()) {
				
		    Iterable<Node> Neo4jNodes =  Neo4jGraphDb.getAllNodes();		
		
			for (final Node myNode : Neo4jNodes) {
				
				Neo4jNodeCounter++;
								
				//System.out.println(myNode); //debug 
				
				final Iterable<Label> nodeLabels = myNode.getLabels();
				
				//determines the class to use in OrientDB, starting from the original Neo4j label. First thing we check if this node has multiple labels 
				int q = 0;				
				for (final Label myLabel : nodeLabels) {		
					q++;	
					
					OrientVertexClass=myLabel.name();
					
					//takes only the first label, in case of multi labels
					String[] parts = OrientVertexClass.split(":");
					
					OrientVertexClass=parts[0];
					
					if (parts.length >= 2) {
						
						Neo4jNodeMultipleLabelsCounter++;
					
						
						//System.out.println("\nWARNING: Found node ('" + myNode + "') with multiple labels. Only the first (" + OrientVertexClass + ") will be used as Class when importing this node in OrientDB");	
						
						ImportLogger.log( Level.WARNING, "Found node ('" + myNode + "') with multiple labels. Only the first (" + OrientVertexClass + ") will be used as Class when importing this node in OrientDB" );
							
												
					}
	
				}	
				
				// if q=0 the neo4j node has no label because q is incremented in the for cicly of the nodeLabels itarable
				if (q==0){
					
					Neo4jNodeNoLabelCounter++;
					
					// set generic class for OrientDB 					
					OrientVertexClass="GenericClassNeo4jConversion";
					
					//System.out.println("\nWARNING: Found node ('" + myNode + "') with no labels. Class 'GenericClassNeo4jConversion' will be used when importing this node in OrientDB");	
					
					ImportLogger.log( Level.WARNING, "Found node ('" + myNode + "') with no labels. Class 'GenericClassNeo4jConversion' will be used when importing this node in OrientDB" );
					
				}
				//
				
				//gets the node properties
				Map<String,Object> myNodeProperties = myNode.getAllProperties();
				
				//stores also the original neo4j nodeId in the property map - we will use it when creating the corresponding OrientDB vertex
				myNodeProperties.put("Neo4jNodeID", myNode.getId());
				
				//System.out.println (myNodeProperties); //debug 
				
				try {				
					// store the vertex on OrientDB 
					Vertex myVertex = Odb.addVertex("class:" + OrientVertexClass, myNodeProperties);	
										
					//System.out.print(myVertex); //debug 
				
					OrientDBImportedVerticesCounter++;
					
					value = 100.0 * (OrientDBImportedVerticesCounter / Neo4jTotalNodes);					
					System.out.print("\r  " + df.format(OrientDBImportedVerticesCounter) + " OrientDB Vertices have been created (" + df.format(value) + "% done)");		
					value =0;
				
				}catch (Exception e){
					
					ImportLogger.log( Level.SEVERE, "Found an error when trying to store node ('" + myNode + "') to OrientDB: " + e.getMessage() );
										
				}
				
			}
			System.out.println ("\nDone");
		}

		double ImportingNodesStopTime = System.currentTimeMillis();				
		//
		
		//creates an index on each OrientDB vertices class on Neo4jNodeID property - this will help in speeding up vertices lookup during relationships creation 		
		double InternalIndicesStartTime = System.currentTimeMillis();
		
		System.out.println();
        System.out.println( "Creating internal Indices on property 'Neo4jNodeID' on all OrientDB Vertices Classes..." );		
		Collection<OClass> ClassCollection = Odb.getRawGraph().getMetadata().getSchema().getClass("V").getAllSubclasses();
		
		double OrientDBVerticesClassCount = (double)ClassCollection.size();
		
		for (OClass ClassCollectionElement : ClassCollection) {
			
			//System.out.println(ClassCollectionElement); //debug 
			
			try {
				
				//first create the property 
				Odb.getRawGraph().getMetadata().getSchema().getClass(ClassCollectionElement.getName()).createProperty("Neo4jNodeID",OType.LONG);
					
				//creates the index if the property creation was successfull
				try {
					
					Odb.getRawGraph().getMetadata().getSchema().getClass(ClassCollectionElement.getName()).getProperty("Neo4jNodeID").createIndex(OClass.INDEX_TYPE.UNIQUE); 		
					
					Neo4jInternalIndicesCounter++;
										
					value = 100.0 * (Neo4jInternalIndicesCounter / OrientDBVerticesClassCount);
					System.out.print("\r  " + df.format(Neo4jInternalIndicesCounter) + " OrientDB Indices have been created (" + df.format(value) + "% done)");			
					value =0;
					
				}catch (Exception e){
										
					ImportLogger.log( Level.SEVERE, "Found an error when trying to create a UNIQUE Index in OrientDB on the 'Neo4jNodeID' Property of the vertices Class '" + ClassCollectionElement.getName() + "': " + e.getMessage() );
										
				}
			}catch (Exception e){
				
				ImportLogger.log( Level.SEVERE, "Found an error when trying to create the 'Neo4jNodeID' Property in OrientDB on the vertices Class '" + ClassCollectionElement.getName() + "': " + e.getMessage() );
					
			}

		}		
		
		double InternalIndicesStopTime = System.currentTimeMillis();
		
        System.out.println ("\nDone");
		//
		
		// gets all relationships from Neo4j and creates the corresponding Edges in OrientDB 
		System.out.println();
        System.out.println( "Getting all Relationships from Neo4j and creating corresponding Edges in OrientDB..." );
				
		//counting Neo4j Relationships so that we can show a % on OrientDB Edges creation
		try (final Transaction tx = Neo4jGraphDb.beginTx()) {
				
		    Iterable<Relationship> Neo4jRelationships =  Neo4jGraphDb.getAllRelationships();	
			
			for (final Relationship myRelationship : Neo4jRelationships) {
				Neo4jTotalRels++;
			}
			
		}		
		//
		
		double ImportingRelsStartTime = System.currentTimeMillis();
		
		try (final Transaction tx = Neo4jGraphDb.beginTx()) {
				
		    Iterable<Relationship> Neo4jRelationships =  Neo4jGraphDb.getAllRelationships();	
			
			for (final Relationship myRelationship : Neo4jRelationships) {
				
				Neo4jRelCounter++;
						
				//System.out.println(myRelationship);				
				
				RelationshipType myRelationshipType = myRelationship.getType();
				
				//get the relationship properties
				Map<String,Object> myRelationshipProperties = myRelationship.getAllProperties();
				
				//store also the original neo4j relationship id
				myRelationshipProperties.put("Neo4jRelID", myRelationship.getId());
								
				//get the out node of this relationships					
				Node Neo4jOutNode = myRelationship.getStartNode();

				//get the in node of this relationships					
				Node Neo4jInNode = myRelationship.getEndNode();
				
				// so we have Neo4jOutNode - myRelationship -> Neo4jInNode										
				
				//debug
				//System.out.println("Neo:" + Neo4jOutNode +"-"+ myRelationshipType.name()  +"->"+ Neo4jInNode);					

				//lookup the corresponding OUTVertex in OrientDB 
				Iterable<Vertex> outVertex = Odb.getVertices("Neo4jNodeID", Neo4jOutNode.getId());		
				for (final Vertex myOutVertex : outVertex) {				

					//cast from Vertex to OrientVertex so that we can make use of more functionalities 				
					OrientVertex myOutOrientVertex = (OrientVertex) myOutVertex;				
						
					//lookup the corresponding inVertex in OrientDB 
					Iterable<Vertex> inVertex = Odb.getVertices("Neo4jNodeID", Neo4jInNode.getId());		
					for (final Vertex myInVertex : inVertex) {		
					
						OrientVertex myInOrientVertex = (OrientVertex) myInVertex;	

						String OrientEdgeClass = myRelationshipType.name();
						
						//in neo4j we can have labels on nodes and relationship with the same name 
						//to handle this case, we append an E_ to the relationship name in case the relationship name is the same of a vertex class 
						Collection<OClass> CheckClassCollection = Odb.getRawGraph().getMetadata().getSchema().getClass("V").getAllSubclasses();
						for (OClass ClassCollectionElement : CheckClassCollection) {
							
							//debug 
							//System.out.println ("\n" + OrientEdgeClass + " " + ClassCollectionElement.getName());
							
							if(OrientEdgeClass.equalsIgnoreCase(ClassCollectionElement.getName())){
								//we have already a label on a vertex with the same name, changes the edge class by adding an "E_" prefix
																	
								//System.out.println ("\n\nWARNING: Found a Neo4j Relationship ('" + OrientEdgeClass + "') with same name of a Neo4j node Label ('"+ ClassCollectionElement.getName() + "'). Importing this relationship in OrientDB as 'E_" + OrientEdgeClass + "'\n");
								
								ImportLogger.log( Level.WARNING, "WARNING: Found a Neo4j Relationship ('" + OrientEdgeClass + "') with same name of a Neo4j node Label ('"+ ClassCollectionElement.getName() + "'). Importing this relationship in OrientDB as 'E_" + OrientEdgeClass );
								
								OrientEdgeClass = "E_" + OrientEdgeClass;
							}	
						}						
						//
												
						// Converting map myRelationshipProperties to an Object[], so that it can be passed to addEdge method below
						// This will allow to create edges with a single create operation, instead of a create and update operation similar to the following:
							//OrientEdge myOrientEdge = Odb.addEdge("class:" + OrientEdgeClass, myOutVertex, myInVertex, OrientEdgeClass);
							//myOrientEdge.setProperties(myRelationshipProperties);								
						Object[] EdgeProps = new Object[myRelationshipProperties.size()*2];
						int i=0;
						for(Map.Entry entry:myRelationshipProperties.entrySet()){
						   EdgeProps[i++] = entry.getKey();
						   EdgeProps[i++] = entry.getValue();
						} 
						//
						
						try{
							OrientEdge myOrientEdge = myOutOrientVertex.addEdge("class:" + OrientEdgeClass, myInOrientVertex, EdgeProps);
						
							OrientDBImportedEdgesCounter++;
						
							//debug 
							//System.out.println("Orient:" + myOutOrientVertex.getProperty("Neo4jID") +"-"+ myRelationshipType.name()  +"->"+ myInOrientVertex.getProperty("Neo4jID"));
							
							value = 100 * ( OrientDBImportedEdgesCounter / Neo4jTotalRels );
							
							System.out.print("\r  " + df.format(OrientDBImportedEdgesCounter) + " OrientDB Edges have been created (" + df.format(value) + "% done)");
							value = 0;
							
						} catch (Exception e) {
							
							ImportLogger.log( Level.SEVERE, "Found an error when trying to create an Edge in OrientDB. Correspinding Relationship in Neo4j is '" + myRelationship + "': " + e.getMessage() );
						}													
					}					
				}
			}				
		}
		
		double ImportingRelsStopTime = System.currentTimeMillis();
		
		System.out.println ("\nDone");
		
		logString = myProgramName + " - v." + OConstants.getVersion() + " - PHASE 2 completed!\n";
		ImportLogger.log( Level.INFO, logString  );		
		//
		
		
				
		//
		// PHASE 3 : SCHEMA MIGRATION 
		//
		
		
		
		//
		double ImportingSchemaStartTime = System.currentTimeMillis();
		
		String Neo4jPropKey="";				
		Label Neo4jLabel = null;		
		boolean propertyCreationSuccess = false;
		//
				
		//
		System.out.println();
        System.out.println( "Getting Constraints from Neo4j and creating corresponding ones in OrientDB..." );
		
		//counting Neo4j Constraints so that we can show a % on OrientDB Constraints creation
		try (final Transaction tx = Neo4jGraphDb.beginTx()) {
			
			Schema Neo4jSchema = Neo4jGraphDb.schema();
			
			Iterable<ConstraintDefinition> Neo4jConstraintDefinition =  Neo4jSchema.getConstraints();	
			for (final ConstraintDefinition myNeo4jConstraintDefinition : Neo4jConstraintDefinition) {	
				if("UNIQUENESS".equals(myNeo4jConstraintDefinition.getConstraintType().toString())){
					Neo4jTotalUniqueConstraints++;
				}
			}
		}
		//
				
		try (final Transaction tx = Neo4jGraphDb.beginTx()) {
			
			Schema Neo4jSchema = Neo4jGraphDb.schema();
			
			// getting all constraints and iterating
			Iterable<ConstraintDefinition> Neo4jConstraintDefinition =  Neo4jSchema.getConstraints();	
			for (final ConstraintDefinition myNeo4jConstraintDefinition : Neo4jConstraintDefinition) {	
				
				Neo4jConstraintsCounter++;
				
				//determine the type of the constaints - different actions will need to be taken according to this type 
				ConstraintType Neo4jConstraintType = myNeo4jConstraintDefinition.getConstraintType();
				//System.out.println(Neo4jConstraintType); //Can be: NODE_PROPERTY_EXISTENCE, RELATIONSHIP_PROPERTY_EXISTENCE, UNIQUENESS (on nodes only)
				
				//determine the class where the constraints will be added in OrientDB
				//Neo4j allows constraints on both nodes and relationship. To get the OrientDB class, we have to separate the cases				
				String OrientDBIndexClass = "";
					
				try {
					//we can get the label with the method getLabel() only if this constraint is associated with a node 					
					//this is associated with a node 
					
					Neo4jLabel = myNeo4jConstraintDefinition.getLabel();					
					OrientDBIndexClass = Neo4jLabel.name();
					
					//System.out.println("constraint label: " + OrientDBIndexClass);
					
					//create class OrientDBIndexClass
					//there might be in fact cases where in neo4j the constraint as been defined, but no nodes have been created. As a result, no nodes of that class have been imported in OrientDB, so that class does not exist in Orient
					if(Odb.getRawGraph().getMetadata().getSchema().existsClass(OrientDBIndexClass) == false){
						Odb.createVertexType(OrientDBIndexClass);						
					}
					
				} catch (IllegalStateException a) {
					//otherwise it is associated with a relationship
					//this is associated with a relationship. Do nothing
				}

				try{
					//we can get the relationship this constraint is associated with only if this is a relationship
					//this is associated with a relationship
					
					String myNeo4jConstraintRelationshipType = myNeo4jConstraintDefinition.getRelationshipType().name();
					
					OrientDBIndexClass = myNeo4jConstraintRelationshipType;
					
					//System.out.println("constraint rel type: " + OrientDBIndexClass);
					
					//create class OrientDBIndexClass
					//there might be in fact cases where in neo4j the constraint as been defined, but no nodes have been created. As a result, no nodes of that class have been imported in OrientDB, so that class does not exist in Orient
					if(Odb.getRawGraph().getMetadata().getSchema().existsClass(OrientDBIndexClass) == false){
						Odb.createEdgeType(OrientDBIndexClass);						
					}
					
				} catch (IllegalStateException a) {
					//otherwise it is associated with a node 
					//this is associated with a node. Do nothing
				}
				//
				
				//we now know the type of this constraints and the class on which it is defined (OrientDBIndexClass)
				
				//determine the property key on which the constraint has been defined 
				Iterable<String> myNeo4jConstraintPropertyKeys = myNeo4jConstraintDefinition.getPropertyKeys();
				for (final String myNeo4jConstraintPropKey : myNeo4jConstraintPropertyKeys) {					
					Neo4jPropKey = myNeo4jConstraintPropKey;
					
					//System.out.println(Neo4jPropKey);				
				}					
								
				//to import this constraint, we first have to create the corresponding property in OrientDB 				
				propertyCreationSuccess = createOrientDBProperty( Neo4jLabel, OrientDBIndexClass, Neo4jPropKey, Neo4jGraphDb, Odb, Neo4jConstraintType.toString() );
				
				// now that the property has been created, we need to take actions based on the Neo4jConstraintType 				
				if (propertyCreationSuccess){									
				
					//taking actions depending on the type of the constraints 
					if("UNIQUENESS".equals(Neo4jConstraintType.toString())){
												
						try {
							
							//we map Neo4j constraints of type UNIQUENESS to UNIQUE indices in Neo4j 
							OIndex OrientDBIndex = Odb.getRawGraph().getMetadata().getSchema().getClass(OrientDBIndexClass).getProperty(Neo4jPropKey).createIndex(OClass.INDEX_TYPE.UNIQUE); 				
							
							//debug
							//System.out.println("\nCreated index: " + OrientDBIndex);
						
							OrientDBImportedUniqueConstraintsCounter++;
							OrientDBImportedConstraintsCounter++;
							
							value = 100 * ( OrientDBImportedUniqueConstraintsCounter / Neo4jTotalUniqueConstraints );
					
							System.out.print("\r  " + df.format(OrientDBImportedUniqueConstraintsCounter) + " OrientDB Indices have been created (" + df.format(value) + "% done)");
							value = 0;							
						
						}catch (Exception e){
							
							ImportLogger.log( Level.SEVERE, "Found an error when trying to create a UNIQUE Index in OrientDB. Correspinding Property in Neo4j is '" + Neo4jPropKey + "' on node label '" + OrientDBIndexClass + "': " + e.getMessage() );
							
						}						
					}				
				}					
			}
		}		
		System.out.println ("\nDone");	
		//

		//
		System.out.println();
        System.out.println( "Getting Indices from Neo4j and creating corresponding ones in OrientDB..." );
		
		//counting Neo4j Indices so that we can show a % on OrientDB indices creation
		try (final Transaction tx = Neo4jGraphDb.beginTx()) {
			
			Schema Neo4jSchema = Neo4jGraphDb.schema();
			
			Iterable<IndexDefinition> Neo4jIndexDefinition =  Neo4jSchema.getIndexes();	
			for (final IndexDefinition myNeo4jIndexDefinition : Neo4jIndexDefinition) {					
				Neo4jTotalIndices++;
			}
		}
		//
		
		try (final Transaction tx = Neo4jGraphDb.beginTx()) {
			
			Schema Neo4jSchema = Neo4jGraphDb.schema();
			
			Iterable<IndexDefinition> Neo4jIndexDefinition =  Neo4jSchema.getIndexes();	
			for (final IndexDefinition myNeo4jIndexDefinition : Neo4jIndexDefinition) {	
				
				Neo4jIndicesCounter++;
				
				Neo4jLabel = myNeo4jIndexDefinition.getLabel();					
				
				//the label this index is on (Neo4j indices are allowed on nodes only)				
				String myNeo4jLabelOfIndex = myNeo4jIndexDefinition.getLabel().name();
				
				//debug
				//System.out.println("all index: on label " + myNeo4jLabelOfIndex);
				
				//if the index is created as a side effect of the creation of a uniqueness constraint, we handled the case already above
				if (myNeo4jIndexDefinition.isConstraintIndex() == false){
					
					Neo4jNonConstraintsIndicesCounter++;
					
					//debug
					//System.out.println("non constraint index: on label " + myNeo4jLabelOfIndex);
				
					Neo4jPropKey="";
					
					//gets the property this index is on
					Iterable<String> myNeo4jIndexPropertyKeys = myNeo4jIndexDefinition.getPropertyKeys();
					for (final String myNeo4jIndexPropKey : myNeo4jIndexPropertyKeys) {					
						
						Neo4jPropKey = myNeo4jIndexPropKey;
						
						//System.out.println("on property: " + Neo4jPropKey);
						
					}
					
					//create the index in OrientDB - we create NOT UNIQUE indices here (case of UNIQUE indices is handled above)	
											
					//create class myNeo4jLabelOfIndex
					//there might be in fact cases where in neo4j the index as been defined, but no nodes have been created. As a result, no nodes of that class have been imported in OrientDB, so that class does not exist in Orient
								
					if(Odb.getRawGraph().getMetadata().getSchema().existsClass(myNeo4jLabelOfIndex) == false){
						Odb.createVertexType(myNeo4jLabelOfIndex);						
					}						
					//
					
					//creates in OrientDB the property this index is defined on
					propertyCreationSuccess = createOrientDBProperty( Neo4jLabel, myNeo4jLabelOfIndex, Neo4jPropKey, Neo4jGraphDb, Odb, "NOT UNIQUE" );
					
					if (propertyCreationSuccess){									
						//index creation
						try{
							
							//creates the index 
							OIndex OrientDBIndex = Odb.getRawGraph().getMetadata().getSchema().getClass(myNeo4jLabelOfIndex).getProperty(Neo4jPropKey).createIndex(OClass.INDEX_TYPE.NOTUNIQUE); 
														
							//System.out.println("\nCreated index: " + OrientDBIndex + "." + Neo4jPropKey);
							
							OrientDBImportedIndicesCounter++;
							
							value = 100 * ( OrientDBImportedIndicesCounter / (Neo4jTotalIndices - OrientDBImportedUniqueConstraintsCounter) );
					
							System.out.print("\r  " + df.format(OrientDBImportedIndicesCounter) + " OrientDB Indices have been created (" + df.format(value) + "% done)");
							value = 0;							
					
						}catch (Exception e){
							
							ImportLogger.log( Level.SEVERE, "Found an error when trying to create a NOTUNIQUE Index in OrientDB. Correspinding Property in Neo4j is '" + Neo4jPropKey + "' on node label '" + myNeo4jLabelOfIndex + "': " + e.getMessage() );
					
						}
						//
					}
				}
			}
		}
		
		double ImportingSchemaStopTime = System.currentTimeMillis();
		
		System.out.println ("\nDone");	
		
		logString =  myProgramName + " - v." + OConstants.getVersion() + " - PHASE 3 completed!\n";
		ImportLogger.log( Level.INFO, logString );
		//

		
		//
		System.out.println();
        System.out.println( "Import completed!" );		
		//

		

		//
		// PHASE 4 : SHUTDOWN OF THE SERVERS AND SUMMARY INFO
		//


		
		//		
		System.out.println();
        System.out.print( "Shutting down OrientDB..." );
				
		Odb.shutdown();
		Ofactory.close();	
		
		System.out.print( "\rShutting down OrientDB...Done" );		
		//

		//
        System.out.println();
        System.out.print( "Shutting down Neo4j..." );

        Neo4jGraphDb.shutdown();
		System.out.print( "\rShutting down Neo4j...Done" );
		System.out.println();
		
		double stopTime = System.currentTimeMillis();		
        double elapsedTime = (stopTime - startTime);
		double elapsedTimeSeconds = elapsedTime / (1000);
		
		double InitializationElapsedTime = (InitializationStopTime - InitializationStartTime);
		double InitializationElapsedTimeSeconds = InitializationElapsedTime / (1000);
				
		double ImportingNodesElapsedTime = ImportingNodesStopTime - ImportingNodesStartTime;		
		double ImportingNodesElapsedTimeSeconds = ImportingNodesElapsedTime / (1000);
		
		double ImportingRelsElapsedTime = ImportingRelsStopTime - ImportingRelsStartTime;		
		double ImportingRelsElapsedTimeSeconds = ImportingRelsElapsedTime / (1000);
		
		double ImportingSchemaElapsedTime = ImportingSchemaStopTime - ImportingSchemaStartTime;		
		double ImportingSchemaElapsedTimeSeconds = ImportingSchemaElapsedTime / (1000);
		
		double InternalIndicesElapsedTime = InternalIndicesStopTime - InternalIndicesStartTime;		
		double InternalIndicesElapsedTimeSeconds = InternalIndicesElapsedTime / (1000);
		
		System.out.println();
		System.out.println( "===============" );
        System.out.println( "Import Summary:" );
		System.out.println( "===============" );
		System.out.println();
		System.out.println( "- Found Neo4j Nodes                                                        : " + df.format(Neo4jNodeCounter));
		System.out.println( "-- With at least one Label                                                 :  " + df.format((Neo4jNodeCounter - Neo4jNodeNoLabelCounter)));		
		System.out.println( "--- With multiple Labels                                                   :   " + df.format(Neo4jNodeMultipleLabelsCounter));
		System.out.println( "-- Without Labels                                                          :  " + df.format(Neo4jNodeNoLabelCounter));				
		  System.out.print( "- Imported OrientDB Vertices                                               : " + df.format(OrientDBImportedVerticesCounter));
		if (Neo4jNodeCounter>0){
			value=(OrientDBImportedVerticesCounter/Neo4jNodeCounter)*100;			
			System.out.print( " (" + df.format(value) + "%)");
			value=0;
		}
		
		System.out.println();
		System.out.println();
		System.out.println( "- Found Neo4j Relationships                                                : " + df.format(Neo4jRelCounter));
		  System.out.print( "- Imported OrientDB Edges                                                  : " + df.format(OrientDBImportedEdgesCounter));
		if(Neo4jRelCounter > 0){
			value=(OrientDBImportedEdgesCounter/Neo4jRelCounter)*100;
			System.out.print (" (" + df.format(value) + "%)");
			value=0;
		}
		
		System.out.println();
		System.out.println();
		System.out.println( "- Found Neo4j Constraints                                                  : " + df.format(Neo4jConstraintsCounter));
	      System.out.print( "- Imported OrientDB Constraints (Indices created)                          : " + df.format(OrientDBImportedConstraintsCounter));
		if(Neo4jConstraintsCounter>0){
			value = ( OrientDBImportedConstraintsCounter / Neo4jConstraintsCounter )*100;
			System.out.print( " (" + df.format(value) + "%)");
			value=0;
		}		
		
		System.out.println();		
		System.out.println();
		System.out.println( "- Found Neo4j (non-constraint) Indices                                     : " + df.format(Neo4jNonConstraintsIndicesCounter));
		  System.out.print( "- Imported OrientDB Indices                                                : " + df.format(OrientDBImportedIndicesCounter));
		if(Neo4jNonConstraintsIndicesCounter>0){
			value=(OrientDBImportedIndicesCounter/Neo4jNonConstraintsIndicesCounter)*100;
			System.out.print( " (" + df.format(value) + "%)");	
			value=0;			
		}
		
		/*
		System.out.println();
		  System.out.print( "- Imported (in previous step) OrientDB indices                             : " + df.format(OrientDBImportedUniqueConstraintsCounter));
		if(Neo4jIndicesCounter>0){
			value = ( OrientDBImportedUniqueConstraintsCounter / Neo4jIndicesCounter )*100;
			System.out.print( " (" + df.format(value) + "%)");
			value=0;
		}
		*/		

		System.out.println();		
		System.out.println();
		System.out.println( "- Additional created Indices (on vertex properties 'Neo4jNodeID')          : " + df.format(Neo4jInternalIndicesCounter));

		System.out.println();		
		System.out.println( "- Total Import time:                                                       : " + df.format(elapsedTimeSeconds) + " seconds");		
		
		System.out.println( "-- Initialization time                                                     :  " + df.format(InitializationElapsedTimeSeconds) + " seconds");
		  System.out.print( "-- Time to Import Nodes                                                    :  " + df.format(ImportingNodesElapsedTimeSeconds) + " seconds");
		if(ImportingNodesElapsedTimeSeconds>0){
			value=(OrientDBImportedVerticesCounter/ImportingNodesElapsedTimeSeconds);
			System.out.print( " (" + dfd.format(value) + " nodes/sec)");
			value=0;
		}		
		
		System.out.println();		
		  System.out.print( "-- Time to Import Relationships                                            :  " + df.format(ImportingRelsElapsedTimeSeconds) + " seconds");
		if(ImportingRelsElapsedTimeSeconds>0){
			value=(OrientDBImportedEdgesCounter/ImportingRelsElapsedTimeSeconds);
			System.out.print( " (" + dfd.format(value) + " rels/sec)");
			value=0;
		}			

		System.out.println();		
		  System.out.print( "-- Time to Import Constraints and Indices                                  :  " + df.format(ImportingSchemaElapsedTimeSeconds) + " seconds");
		if(ImportingSchemaElapsedTimeSeconds>0){
			value=( ( OrientDBImportedConstraintsCounter + OrientDBImportedIndicesCounter ) / ImportingSchemaElapsedTimeSeconds );
			System.out.print( " (" + dfd.format(value) + " indices/sec)");
			value=0;		
		}
				
		System.out.println();		
		  System.out.print( "-- Time to create internal Indices (on vertex properties 'Neo4jNodeID')    :  " + df.format(InternalIndicesElapsedTimeSeconds) + " seconds");
		if(InternalIndicesElapsedTimeSeconds>0){
			value=( Neo4jInternalIndicesCounter / InternalIndicesElapsedTimeSeconds );
			System.out.print( " (" + dfd.format(value) + " indices/sec)");
			value=0;		
		}		
		
		System.out.println("\n");		
		
		//

		//				
		logString =  myProgramName + " - v." + OConstants.getVersion() + " - PHASE 4 completed!\n";
		ImportLogger.log( Level.INFO, logString  );
		
		logString =  myProgramName + " - v." + OConstants.getVersion() + " - Import completed!";
		ImportLogger.log( Level.INFO, logString  );
		//
		
		returnCode = 0;
		return returnCode;
		
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
		
		String Neo4jPropType = "";			
		OType OrientOtype = null;       
		boolean foundNode = false; 
		
		//find a node that has this property, then get the data type of this property
		ResourceIterator<Node> Neo4jNodes = myNeo4jGraphDb.findNodes(myNeo4jLabel);				
		try {
			 while ( Neo4jNodes.hasNext() ) {
				
				Node myNode = Neo4jNodes.next();	
				
				//not all nodes with label myNeo4jLabel may have this property - even if we have a unique constraint on this property (it is unique in the nodes where the property exists). When we find a node with this property, we exit the loop				
				if (myNode.hasProperty(myNeo4jPropKey)){
					
					foundNode = true;
					
					//System.out.println("working on node " + myNode); //debug
					
					Object PropertyValue = myNode.getProperty( myNeo4jPropKey, null );
					
					//get the Neo4j property data type 
					Neo4jPropType = Neo4jPropType(PropertyValue);
					
					//map the Neo4j property type to an OrientDB property data type 
					OrientOtype = MapNeo4jToOrientDBPropertyType (Neo4jPropType);
					
					//System.out.println("Property defined on this node: " + myNeo4jPropKey + " value: " + PropertyValue + " data type: " + Neo4jPropType); //debug
					
					break;
				}
				
			 }
		}
		finally {					
			Neo4jNodes.close();
		}
		//
										
		//Now that we know the data type of the property, we can create it in OrientDB 
		
		//However, there may be cases where the constraints has been defined, but no nodes have been created yet. In this case we cannot know the data type. We will use STRING as default 
		if (foundNode==false){	
			Neo4jPropType="Sting";		
			OrientOtype	= OType.STRING;
		}
			
		//System.out.println("Creating OrientDB Property '" + myNeo4jPropKey + "' of type '" + OrientOtype + "' on Class '" + myOrientDBIndexClass + "' "); //debug 
					
		try{
				
			OProperty OrientDBProperty = myOdb.getRawGraph().getMetadata().getSchema().getClass(myOrientDBIndexClass).createProperty(myNeo4jPropKey,OrientOtype);	
			
			if (foundNode==false){							
				
				ImportLogger.log( Level.INFO, "The Neo4j Property '" + myNeo4jPropKey + "' on the Neo4j Label '" + myNeo4jLabel.name() + "' associated to a Neo4j '" + myNeo4jConstraintType + "' constraint/index has been imported as STRING because there are no nodes in Neo4j that have this property, hence it was not possible to determine the type of this Neo4j Property" );
				
				
			}else{

				ImportLogger.log( Level.INFO, "Created Property '" + myNeo4jPropKey + "' on the Class '" + myOrientDBIndexClass + "' with type '" + OrientOtype + "'" );
				
			}
			
			return true;
			
		}catch (Exception e){
			
			ImportLogger.log( Level.SEVERE, "Found an error when trying to create a Property in OrientDB. Correspinding Property in Neo4j is '" + myNeo4jPropKey + "' on node label '" + myOrientDBIndexClass + "': " + e.getMessage() );
			
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

	private static String Neo4jPropType (final Object myPropertyValue) {
		
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
	
	private static final Logger ImportLogger = Logger.getLogger( "OrientDB.Neo4j.Importer" );

	
}
	
	