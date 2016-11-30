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

    ONeo4jImporterSettings settings = new ONeo4jImporterSettings();

    settings.neo4jDbPath = "./neo4jdbs/databases/graphdb_nodes_only";
    settings.orientDbDir = "target/migrated_databases/graphdb_nodes_only";
    settings.overwriteOrientDbDir = true;

    ONeo4jImporter importer = new ONeo4jImporter(settings);

    importer.execute();

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:target/migrated_databases/graphdb_nodes_only");
    db.open("admin", "admin");

    Assertions.assertThat(db.getMetadata().getSchema().getClass("NodeLabelA")).isNotNull();
    Assertions.assertThat(db.getMetadata().getSchema().getClass("NodeLabelB")).isNotNull();
    Assertions.assertThat(db.getMetadata().getSchema().getClass("NodeLabelC")).isNotNull();

  }
}