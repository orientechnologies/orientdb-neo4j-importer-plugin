package com.orientechnologies.orient.neo4jimporter;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.assertj.core.api.Assertions;
import org.junit.Test;

/**
 * Created by frank on 08/11/2016.
 */
public class ONeo4jImporterTest {

  @Test
  public void shouldImportEmptyDb() throws Exception {

    //String[] args = new String[] {
	//	"-neo4jdbdir" , "neo4jdbs/databases/graphdb_empty_db", 
	//	"-neo4jlibdir", "D:/neo4j/neo4j-community-3.0.6/lib", 
	//	"-odbdir", "target/migrated_databases/graphdb_empty_db"};

    //launch
    //ONeo4jImporterMain.main(args);

    //open the generated Orient Graph and do assertions
    //ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:target/migrated_databases/graphdb_empty_db");
    //db.open("admin", "admin");

    //Assertions.assertThat(db.getMetadata().getSchema().getClass("NodeLabelA")).isNotNull();
	//Assertions.assertThat(db.getMetadata().getSchema().getClass("NodeLabelB")).isNotNull();
	//Assertions.assertThat(db.getMetadata().getSchema().getClass("NodeLabelC")).isNotNull();

  }
}