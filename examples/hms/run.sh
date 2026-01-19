#!/usr/bin/env bash
set -euxo pipefail

# Add AWS/S3 jars to classpath
export HADOOP_CLASSPATH="${HADOOP_CLASSPATH:-}:/opt/hadoop/share/hadoop/tools/lib/*"

# Run schematool
/opt/hive-metastore/bin/schematool --verbose -dbType derby -initSchema

# Start metastore (in foreground)
/opt/hive-metastore/bin/hive --service metastore --hiveconf hive.root.logger=${HMS_LOGLEVEL:-INFO},console
