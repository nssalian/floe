g#!/bin/bash

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

# Configure Livy based on environment variables
if [[ -n "${SPARK_MASTER}" ]]; then
    echo "livy.spark.master=${SPARK_MASTER}" >> "${LIVY_CONF_DIR}/livy.conf"
fi

if [[ -n "${SPARK_DEPLOY_MODE}" ]]; then
    echo "livy.spark.deploy-mode=${SPARK_DEPLOY_MODE}" >> "${LIVY_CONF_DIR}/livy.conf"
fi

# Start Livy server
exec "$LIVY_HOME/bin/livy-server"