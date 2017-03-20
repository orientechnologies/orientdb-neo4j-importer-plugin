package com.orientechnologies.orient.neo4jimporter;

import com.orientechnologies.orient.context.ONeo4jImporterContext;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.assertj.core.api.Assertions;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Created by frank on 08/11/2016.
 * <p>
 * Tests are documented in the README.md
 * Reminder:
 *    when nodes are migrated, for each vertex class, two internal properties and two indices are created declaredProperties=2
 *    when edges are migrated, for each edge class, an internal property and an index are created declaredProperties=1
 * Ideas for other tests:
 * - test with edges
 */
public class ONeo4jImporterTest {

  @Ignore
  @Test
  public void shouldImportEmptyDb() throws Exception {

    ONeo4jImporterSettings settings = new ONeo4jImporterSettings();

    settings.setOrientDbDir("target/migrated_databases/graphdb_empty_db");
    settings.setOverwriteOrientDbDir(true);
    settings.setCreateIndexOnNeo4jRelID(true);

    ONeo4jImporter importer = new ONeo4jImporter(settings);

    importer.execute();

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:target/migrated_databases/graphdb_empty_db");
    db.open("admin", "admin");

    //no vertices classes, no edge classes, 0 vertices, 0 edges
    assertEquals(0, db.getMetadata().getSchema().getClass("V").getSubclasses().size());
    assertEquals(0, db.getMetadata().getSchema().getClass("E").getSubclasses().size());

    assertEquals(0, db.getMetadata().getSchema().getClass("V").count());
    assertEquals(0, db.getMetadata().getSchema().getClass("E").count());

    db.close();
  }

  @Ignore
  @Test
  public void shouldImportUniqueConstraintsOnlyDb() throws Exception {

    ONeo4jImporterSettings settings = new ONeo4jImporterSettings();

    settings.setOrientDbDir("target/migrated_databases/graphdb_unique_constraints_only");
    settings.setOverwriteOrientDbDir(true);
    settings.setCreateIndexOnNeo4jRelID(true);

    ONeo4jImporter importer = new ONeo4jImporter(settings);

    importer.execute();

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:target/migrated_databases/graphdb_unique_constraints_only");
    db.open("admin", "admin");

    //0 vertices, 0 edges, class NodeLabelA, class NodeLabelB, class NodeLabelC, 3 properties, 3 unique indexes
    assertEquals(0, db.getMetadata().getSchema().getClass("V").count());
    assertEquals(0, db.getMetadata().getSchema().getClass("E").count());

    Assertions.assertThat(db.getMetadata().getSchema().getClass("NodeLabelA")).isNotNull();
    Assertions.assertThat(db.getMetadata().getSchema().getClass("NodeLabelB")).isNotNull();
    Assertions.assertThat(db.getMetadata().getSchema().getClass("NodeLabelC")).isNotNull();

    assertEquals(2, db.getMetadata().getSchema().getClass("NodeLabelA").declaredProperties().size());
    assertEquals(2, db.getMetadata().getSchema().getClass("NodeLabelB").declaredProperties().size());
    assertEquals(2, db.getMetadata().getSchema().getClass("NodeLabelC").declaredProperties().size());

    assertEquals(true, db.getMetadata().getSchema().getClass("NodeLabelA").existsProperty("p_number"));
    assertEquals(true, db.getMetadata().getSchema().getClass("NodeLabelB").existsProperty("p_string"));
    assertEquals(true, db.getMetadata().getSchema().getClass("NodeLabelC").existsProperty("p_boolean"));

    assertEquals(true, db.getMetadata().getSchema().getClass("NodeLabelA").areIndexed("p_number"));
    assertEquals(true, db.getMetadata().getSchema().getClass("NodeLabelB").areIndexed("p_string"));
    assertEquals(true, db.getMetadata().getSchema().getClass("NodeLabelC").areIndexed("p_boolean"));
    //

    db.close();

  }

  @Ignore
  @Test
  public void shouldImportNodesOnlyDb() throws Exception {

    ONeo4jImporterSettings settings = new ONeo4jImporterSettings();

    settings.setOrientDbDir("target/migrated_databases/graphdb_nodes_only");
    settings.setOverwriteOrientDbDir(true);
    settings.setCreateIndexOnNeo4jRelID(true);

    ONeo4jImporter importer = new ONeo4jImporter(settings);

    importer.execute();

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:target/migrated_databases/graphdb_nodes_only");
    db.open("admin", "admin");

    Assertions.assertThat(db.getMetadata().getSchema().getClass("NodeLabelA")).isNotNull();
    Assertions.assertThat(db.getMetadata().getSchema().getClass("NodeLabelB")).isNotNull();
    Assertions.assertThat(db.getMetadata().getSchema().getClass("NodeLabelC")).isNotNull();

    assertEquals(2, db.getMetadata().getSchema().getClass("NodeLabelA").declaredProperties().size());
    assertEquals(2, db.getMetadata().getSchema().getClass("NodeLabelB").declaredProperties().size());
    assertEquals(2, db.getMetadata().getSchema().getClass("NodeLabelC").declaredProperties().size());

    assertEquals(10, db.getMetadata().getSchema().getClass("NodeLabelA").count());
    assertEquals(10, db.getMetadata().getSchema().getClass("NodeLabelB").count());
    assertEquals(10, db.getMetadata().getSchema().getClass("NodeLabelC").count());

    db.close();

  }

  @Ignore
  @Test
  public void shouldImportNodesOnlyNoLabelsDb() throws Exception {

    ONeo4jImporterSettings settings = new ONeo4jImporterSettings();

    settings.setOrientDbDir("target/migrated_databases/graphdb_nodes_only_no_labels");
    settings.setOverwriteOrientDbDir(true);
    settings.setCreateIndexOnNeo4jRelID(true);

    ONeo4jImporter importer = new ONeo4jImporter(settings);

    importer.execute();

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:target/migrated_databases/graphdb_nodes_only_no_labels");
    db.open("admin", "admin");

    assertEquals(1, db.getMetadata().getSchema().getClass("V").getSubclasses().size());
    assertEquals(0, db.getMetadata().getSchema().getClass("E").getSubclasses().size());

    Assertions.assertThat(db.getMetadata().getSchema().getClass("GenericClassNeo4jConversion")).isNotNull();
    assertEquals(2, db.getMetadata().getSchema().getClass("GenericClassNeo4jConversion").declaredProperties().size());

    assertEquals(30, db.getMetadata().getSchema().getClass("V").count());

    db.close();

  }

  @Ignore
  @Test
  public void shouldImportNodesOnlyMixedLabelsNoLabelsDb() throws Exception {

    ONeo4jImporterSettings settings = new ONeo4jImporterSettings();

    settings.setOrientDbDir("target/migrated_databases/graphdb_nodes_only_mixed_labels_and_no_labels");
    settings.setOverwriteOrientDbDir(true);
    settings.setCreateIndexOnNeo4jRelID(true);

    ONeo4jImporter importer = new ONeo4jImporter(settings);

    importer.execute();

    ODatabaseDocumentTx db = new ODatabaseDocumentTx(
        "plocal:target/migrated_databases/graphdb_nodes_only_mixed_labels_and_no_labels");
    db.open("admin", "admin");

    Assertions.assertThat(db.getMetadata().getSchema().getClass("NodeLabelA")).isNotNull();
    Assertions.assertThat(db.getMetadata().getSchema().getClass("NodeLabelC")).isNotNull();
    Assertions.assertThat(db.getMetadata().getSchema().getClass("GenericClassNeo4jConversion")).isNotNull();

    assertEquals(2, db.getMetadata().getSchema().getClass("NodeLabelA").declaredProperties().size());
    assertEquals(2, db.getMetadata().getSchema().getClass("NodeLabelC").declaredProperties().size());
    assertEquals(2, db.getMetadata().getSchema().getClass("GenericClassNeo4jConversion").declaredProperties().size());

    db.close();

  }

  @Ignore
  @Test
  public void shouldImportNodesOnlyLabelCaseDb() throws Exception {

    ONeo4jImporterSettings settings = new ONeo4jImporterSettings();

    settings.setOrientDbDir("target/migrated_databases/graphdb_nodes_only_label_case_test");
    settings.setOverwriteOrientDbDir(true);
    settings.setCreateIndexOnNeo4jRelID(true);

    ONeo4jImporter importer = new ONeo4jImporter(settings);

    importer.execute();

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:target/migrated_databases/graphdb_nodes_only_label_case_test");
    db.open("admin", "admin");

    Assertions.assertThat(db.getMetadata().getSchema().getClass("NodeLabelA")).isNotNull();
    Assertions.assertThat(db.getMetadata().getSchema().getClass("NodeLabelC")).isNotNull();
    Assertions.assertThat(db.getMetadata().getSchema().getClass("GenericClassNeo4jConversion")).isNotNull();

    assertEquals(2, db.getMetadata().getSchema().getClass("NodeLabelA").declaredProperties().size());
    assertEquals(2, db.getMetadata().getSchema().getClass("NodeLabelC").declaredProperties().size());
    assertEquals(2, db.getMetadata().getSchema().getClass("GenericClassNeo4jConversion").declaredProperties().size());

    db.close();

  }

  @Ignore
  @Test
  public void shouldImportNodesOnlyLabelCaseConstraintsDb() throws Exception {

    ONeo4jImporterSettings settings = new ONeo4jImporterSettings();

    settings.setOrientDbDir("target/migrated_databases/graphdb_nodes_only_label_case_test_constraints");
    settings.setOverwriteOrientDbDir(true);
    settings.setCreateIndexOnNeo4jRelID(true);

    ONeo4jImporter importer = new ONeo4jImporter(settings);

    importer.execute();

    ODatabaseDocumentTx db = new ODatabaseDocumentTx(
        "plocal:target/migrated_databases/graphdb_nodes_only_label_case_test_constraints");
    db.open("admin", "admin");

    Assertions.assertThat(db.getMetadata().getSchema().getClass("NodeLabelA")).isNotNull();
    Assertions.assertThat(db.getMetadata().getSchema().getClass("NodeLabelB")).isNotNull();

    assertEquals(3, db.getMetadata().getSchema().getClass("NodeLabelA").declaredProperties().size());
    assertEquals(3, db.getMetadata().getSchema().getClass("NodeLabelB").declaredProperties().size());

    assertEquals(20, db.getMetadata().getSchema().getClass("NodeLabelA").count());
    assertEquals(20, db.getMetadata().getSchema().getClass("NodeLabelB").count());

    assertEquals(true, db.getMetadata().getSchema().getClass("NodeLabelA").existsProperty("p_number"));
    assertEquals(true, db.getMetadata().getSchema().getClass("NodeLabelB").existsProperty("p_number"));

    db.close();

  }

  @Ignore
  @Test
  public void shouldImportNodesOnlyMultipleLabelsDb() throws Exception {

    ONeo4jImporterSettings settings = new ONeo4jImporterSettings();

    settings.setOrientDbDir("target/migrated_databases/graphdb_nodes_only_multiple_labels");
    settings.setOverwriteOrientDbDir(true);
    settings.setCreateIndexOnNeo4jRelID(true);

    ONeo4jImporter importer = new ONeo4jImporter(settings);

    importer.execute();

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:target/migrated_databases/graphdb_nodes_only_multiple_labels");
    db.open("admin", "admin");

    Assertions.assertThat(db.getMetadata().getSchema().getClass("MultipleLabelNeo4jConversion")).isNotNull();
    Assertions.assertThat(db.getMetadata().getSchema().getClass("NodeLabelA")).isNull();
    Assertions.assertThat(db.getMetadata().getSchema().getClass("NodeLabelB")).isNull();
    Assertions.assertThat(db.getMetadata().getSchema().getClass("NodeLabelC")).isNull();
    Assertions.assertThat(db.getMetadata().getSchema().getClass("NodeLabelD")).isNull();
    Assertions.assertThat(db.getMetadata().getSchema().getClass("NodeLabelE")).isNotNull();

    assertEquals(2, db.getMetadata().getSchema().getClass("MultipleLabelNeo4jConversion").declaredProperties().size());
    assertEquals(2, db.getMetadata().getSchema().getClass("NodeLabelE").declaredProperties().size());

    assertEquals(10, db.getMetadata().getSchema().getClass("NodeLabelE").count());
    assertEquals(20, db.getMetadata().getSchema().getClass("MultipleLabelNeo4jConversion").count());

    db.close();

  }

  @Ignore
  @Test
  public void shouldImportMultipleLabelsAndConstraintsDb() throws Exception {

    ONeo4jImporterSettings settings = new ONeo4jImporterSettings();

    settings.setOrientDbDir("target/migrated_databases/graphdb_multiple_labels_and_constraints");
    settings.setOverwriteOrientDbDir(true);
    settings.setCreateIndexOnNeo4jRelID(true);

    ONeo4jImporter importer = new ONeo4jImporter(settings);

    importer.execute();

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:target/migrated_databases/graphdb_multiple_labels_and_constraints");
    db.open("admin", "admin");

    Assertions.assertThat(db.getMetadata().getSchema().getClass("MultipleLabelNeo4jConversion")).isNotNull();
    Assertions.assertThat(db.getMetadata().getSchema().getClass("NodeLabelA")).isNull();
    Assertions.assertThat(db.getMetadata().getSchema().getClass("NodeLabelB")).isNotNull();
    Assertions.assertThat(db.getMetadata().getSchema().getClass("NodeLabelC")).isNull();
    Assertions.assertThat(db.getMetadata().getSchema().getClass("NodeLabelD")).isNull();
    Assertions.assertThat(db.getMetadata().getSchema().getClass("NodeLabelE")).isNotNull();

    assertEquals(true, db.getMetadata().getSchema().getClass("NodeLabelB").existsProperty("p_number"));
    assertEquals(true, db.getMetadata().getSchema().getClass("NodeLabelE").existsProperty("other_property"));
    assertEquals(true, db.getMetadata().getSchema().getClass("MultipleLabelNeo4jConversion").existsProperty("p_number"));
    assertEquals(true, db.getMetadata().getSchema().getClass("MultipleLabelNeo4jConversion").existsProperty("p_string"));

    assertEquals(4, db.getMetadata().getSchema().getClass("MultipleLabelNeo4jConversion").declaredProperties().size());
    assertEquals(3, db.getMetadata().getSchema().getClass("NodeLabelB").declaredProperties().size());
    assertEquals(3, db.getMetadata().getSchema().getClass("NodeLabelE").declaredProperties().size());

    assertEquals(10, db.getMetadata().getSchema().getClass("NodeLabelB").count());
    assertEquals(10, db.getMetadata().getSchema().getClass("NodeLabelE").count());
    assertEquals(20, db.getMetadata().getSchema().getClass("MultipleLabelNeo4jConversion").count());

    db.close();
  }


  @Test
  public void basicImportingTest() throws Exception {


    String neo4jUrl = "bolt://localhost:7687";
    String neo4jUsername = "neo4j";
    String neo4jPassword = "admin";
    String odbDir = "/Users/gabriele/orientdb-community-2.2.18-SNAPSHOT/databases/neo4jImport";
    String odbProtocol = "plocal";
    boolean overwriteDB = true;
    boolean createIndexOnNeo4jRelID = true;

    ONeo4jImporterSettings settings = new ONeo4jImporterSettings(neo4jUrl, neo4jUsername, neo4jPassword, odbDir, odbProtocol, overwriteDB, createIndexOnNeo4jRelID);

    try {
      ONeo4jImporterMain.executeJob(settings);
    } catch(Exception e) {
      fail();
      ONeo4jImporterContext.getInstance().getOutputManager().info("Exception message: " + e.getMessage());
      ONeo4jImporterContext.getInstance().getOutputManager().info("Stacktrace:\n" + e.getStackTrace());
    }
  }

}