# Neo4j to OrientDB Importer

## Documentation

http://orientdb.com/docs/last/OrientDB-Neo4j-Importer.html


## Internals

### Compile

```
mvn clean install
```

To skip tests:

```
mvn clean install -DskipTests
```

To run only a specific test, e.g. `shouldImportEmptyDb`:

```
mvn -Dtest=ONeo4jImporterTest#shouldImportEmptyDb test
```


### Tests

The test databases are created using the following queries:


#### graphdb\_empty\_db (test `shouldImportEmptyDb`)

Empty database


#### graphdb\_unique_constraints\_only (test `shouldImportUniqueConstraintsOnlyDb`)

```
CREATE CONSTRAINT ON (n:NodeLabelA) ASSERT n.p_number   IS UNIQUE
CREATE CONSTRAINT ON (n:NodeLabelB) ASSERT n.p_string   IS UNIQUE
CREATE CONSTRAINT ON (n:NodeLabelC) ASSERT n.p_boolean  IS UNIQUE
```

#### graphdb\_nodes\_only (test `shouldImportNodesOnlyDb`)

```
foreach(x in range(1,10) | create (:NodeLabelA {p_number:x, other_property: "NodeLabelA-"+x}))
foreach(x in range(1,10) | create (:NodeLabelB {p_string:"string_value_" + x, other_property: "NodeLabelB-"+x}))
foreach(x in range(1,5)  | create (:NodeLabelC {p_boolean:false, other_property: "NodeLabelC-"+x}))
foreach(x in range(6,10) | create (:NodeLabelC {p_boolean:true, other_property: "NodeLabelC-"+x}))
```

#### graphdb\_nodes\_only\_no\_labels (test `shouldImportNodesOnlyNoLabelsDb`)

```
foreach(x in range(1,10) | create ( {p_number:x, other_property: "string-"+x}))
foreach(x in range(1,10) | create ( {p_string:"string_value_" + x, other_property: "string-"+x}))
foreach(x in range(1,5)  | create ( {p_boolean:false, other_property: "string-"+x}))
foreach(x in range(6,10) | create ( {p_boolean:true, other_property: "string-"+x}))
```

#### graphdb\_nodes\_only\_mixed\_labels\_and\_no\_labels (test `shouldImportNodesOnlyMixedLabelsNoLabelsDb`)

```
foreach(x in range(1,10) | create (:NodeLabelA {p_number:x, other_property: "NodeLabelA-"+x}))
foreach(x in range(1,10) | create ( {p_string:"string_value_" + x, other_property: "string-"+x}))
foreach(x in range(1,5)  | create (:NodeLabelC {p_boolean:false, other_property: "NodeLabelC-"+x}))
foreach(x in range(6,10) | create ( {p_boolean:true, other_property: "string-"+x}))
```

#### graphdb\_nodes\_only\_label\_case\_test (test `shouldImportNodesOnlyLabelCaseDb`)

```
foreach(x in range(1,10) | create (:NodeLabelA {p_number:x, other_property: "NodeLabelA-"+x}))
foreach(x in range(1,10) | create (:NodeLABELA {p_string:"string_value_" + x, other_property: "NodeLABELA-"+x}))
foreach(x in range(1,5)  | create (:NodeLabelC {p_boolean:false, other_property: "NodeLabelC-"+x}))
foreach(x in range(6,10) | create ( {p_boolean:true, other_property: "string-"+x}))
```


#### graphdb\_nodes\_only\_label\_case\_test\_constraints (test `shouldImportNodesOnlyLabelCaseConstraintsDb`)

```
CREATE CONSTRAINT ON (n:NodeLabelA) ASSERT n.p_number        IS UNIQUE
CREATE CONSTRAINT ON (n:NodeLabelB) ASSERT n.p_number        IS UNIQUE
CREATE CONSTRAINT ON (n:NodeLABELB) ASSERT n.p_number        IS UNIQUE

foreach(x in range(1,10) | create (:NodeLabelA {p_number:x, other_property: "NodeLabelA-"+x}))
foreach(x in range(1,10) | create (:NodeLABELA {p_string:"string_value_" + x, other_property: "NodeLABELA-"+x}))
foreach(x in range(1,10) | create (:NodeLabelB {p_number:x, other_property: "NodeLabelB-"+x}))
foreach(x in range(1,10) | create (:NodeLABELB {p_number:x, other_property: "NodeLABELB-"+x}))
```


#### graphdb\_nodes\_only\_multiple\_labels (test `shouldImportNodesOnlyMultipleLabelsDb`)

```
foreach(x in range(1,10) | create (:NodeLabelA:NodeLabelB {p_number:x, other_property: "NodeLabelA-NodeLabelB-"+x}))
foreach(x in range(1,10) | create (:NodeLabelC:NodeLabelD {p_string:"string_value_" + x, other_property: "NodeLabelC-NodeLabelD"+x}))
foreach(x in range(1,10) | create (:NodeLabelE {p_boolean:true, other_property: "NodeLabelE-"+x}))
```

#### graphdb\_multiple\_labels\_and\_constraints (test `shouldImportMultipleLabelsAndConstraintsDb`)

```
CREATE CONSTRAINT ON (n:NodeLabelA) ASSERT n.p_number        IS UNIQUE
CREATE CONSTRAINT ON (n:NodeLabelB) ASSERT n.p_number        IS UNIQUE
CREATE CONSTRAINT ON (n:NodeLabelC) ASSERT n.p_string        IS UNIQUE
CREATE CONSTRAINT ON (n:NodeLabelE) ASSERT n.other_property  IS UNIQUE

foreach(x in range(1,10) | create (:NodeLabelA:NodeLabelB {p_number:x, other_property: "NodeLabelA-NodeLabelB-"+x}))
foreach(x in range(11,20) | create (:NodeLabelB {p_number:x, other_property: "NodeLabelB-"+x}))
foreach(x in range(1,10) | create (:NodeLabelC:NodeLabelD {p_string:"string_value_" + x, other_property: "NodeLabelC-NodeLabelD"+x}))
foreach(x in range(1,10) | create (:NodeLabelE {p_boolean:true, other_property: "NodeLabelE-"+x}))
```