#!/usr/bin/env bash

# resolve links - $0 may be a softlink
PRG="$0"

while [ -h "$PRG" ]; do
  ls=`ls -ld "$PRG"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=`dirname "$PRG"`/"$link"
  fi
done

# Get standard environment variables
PRGDIR=`dirname "$PRG"`

# Only set ORIENTDB_HOME if not already set
[ -f "$ORIENTDB_HOME"/lib/orientdb-neo4j-importer-@VERSION@.jar ] || ORIENTDB_HOME=`cd "$PRGDIR/.." ; pwd`
export ORIENTDB_HOME

# Set JavaHome if it exists
if [ -f "${JAVA_HOME}/bin/java" ]; then
   JAVA=${JAVA_HOME}/bin/java
else
   JAVA=java
fi
export JAVA

LOG_FILE=$ORIENTDB_HOME/config/orientdb-neo4j-importer-log.properties
JAVA_MAX_DIRECT="-XX:MaxDirectMemorySize=512g" #raised in v.2.2.15 from 4g. It does not mean that it allocates 512g, but it can avoid OOM 
JAVA_OPTS_SCRIPT="$JAVA_MAX_DIRECT -Djava.util.logging.config.file=$LOG_FILE"

ARGS='';
for var in "$@"; 
	do ARGS="$ARGS $var"    
done

#gets value of neo4jlibdir, so that we can use it in the -classpath
while [[ $# -gt 1 ]]
do
key="$1"

case $key in
    -neo4jlibdir)
    NEO4JLIB="$2"
    shift 
    ;;
    
    *)
    # unknown option
    ;;
esac
shift  
done

#if [ -z ${NEO4JLIB+x} ]; then 
#	echo "Option -neo4jlibdir is mandatory" 
#	echo "Exiting"
#	exit 1
#else 
#fi

exec "$JAVA" -client -cp "$NEO4JLIB/*:$ORIENTDB_HOME/lib/*" $JAVA_OPTS_SCRIPT  \
    com.orientechnologies.orient.neo4jimporter.ONeo4jImporterMain $ARGS
