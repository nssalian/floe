#!/usr/bin/env bash
#
# Copyright 2026 The Floe Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -euxo pipefail

# Add AWS/S3 jars to classpath
export HADOOP_CLASSPATH="${HADOOP_CLASSPATH:-}:/opt/hadoop/share/hadoop/tools/lib/*"

# Run schematool
/opt/hive-metastore/bin/schematool --verbose -dbType derby -initSchema

# Start metastore (in foreground)
/opt/hive-metastore/bin/hive --service metastore --hiveconf hive.root.logger=${HMS_LOGLEVEL:-INFO},console
