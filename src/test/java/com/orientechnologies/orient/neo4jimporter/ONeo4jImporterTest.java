package com.orientechnologies.orient.neo4jimporter;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.assertj.core.api.Assertions;
import org.junit.Test;

/**
 * Created by frank on 08/11/2016.
 */
public class ONeo4jImporterTest {

  @Test
  public void shouldImportNodesOnlyDb() throws Exception {

    //provide right params
    String[] args = new String[] {};

    //launch
    ONeo4jImporter.main(args);

    //open the generated Orient Grapgh and do assertions

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:./target/onlyNodes");

    db.open("admin", "admin");

    Assertions.assertThat(db.getMetadata().getSchema().getClass("class_created")).isNotNull();

  }
}