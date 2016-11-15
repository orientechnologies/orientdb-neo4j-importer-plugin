# Neo4j to OrientDB Importer

## Documentation

http://orientdb.com/docs/last/OrientDB-Neo4j-Importer.html


## Internals

### Tests

_**Work in progress**_

The test databases are created using the following queries:

#### graphdb\_empty\_db

Empty database

#### graphdb\_unique_constraints\_only

```
CREATE CONSTRAINT ON (n:NodeLabelA) ASSERT n.p_number   IS UNIQUE
CREATE CONSTRAINT ON (n:NodeLabelB) ASSERT n.p_string   IS UNIQUE
CREATE CONSTRAINT ON (n:NodeLabelC) ASSERT n.p_boolean  IS UNIQUE
```

#### graphdb\_nodes\_only

```
foreach(x in range(1,10) | create (:NodeLabelA {p_number:x, other_property: "NodeLabelA-"+x}))
foreach(x in range(1,10) | create (:NodeLabelB {p_string:"string_value_" + x, other_property: "NodeLabelB-"+x}))
foreach(x in range(1,5)  | create (:NodeLabelC {p_boolean:false, other_property: "NodeLabelC-"+x}))
foreach(x in range(6,10) | create (:NodeLabelC {p_boolean:true, other_property: "NodeLabelC-"+x}))
```

#### graphdb\_nodes\_only\_no\_labels

```
foreach(x in range(1,10) | create ( {p_number:x, other_property: "string-"+x}))
foreach(x in range(1,10) | create ( {p_string:"string_value_" + x, other_property: "string-"+x}))
foreach(x in range(1,5)  | create ( {p_boolean:false, other_property: "string-"+x}))
foreach(x in range(6,10) | create ( {p_boolean:true, other_property: "string-"+x}))
```

#### graphdb\_nodes\_only\_mixed\_labels\_and\_no\_labels

```
foreach(x in range(1,10) | create (:NodeLabelA {p_number:x, other_property: "NodeLabelA-"+x}))
foreach(x in range(1,10) | create ( {p_string:"string_value_" + x, other_property: "string-"+x}))
foreach(x in range(1,5)  | create (:NodeLabelC {p_boolean:false, other_property: "NodeLabelC-"+x}))
foreach(x in range(6,10) | create ( {p_boolean:true, other_property: "string-"+x}))
```

#### graphdb_nodes_only_label_case_test

```
foreach(x in range(1,10) | create (:NodeLabelA {p_number:x, other_property: "NodeLabelA-"+x}))
foreach(x in range(1,10) | create (:NodeLABELA {p_string:"string_value_" + x, other_property: "NodeLabelB-"+x}))
foreach(x in range(1,5)  | create (:NodeLabelC {p_boolean:false, other_property: "NodeLabelC-"+x}))
foreach(x in range(6,10) | create ( {p_boolean:true, other_property: "string-"+x}))
```

