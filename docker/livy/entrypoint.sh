#!/bin/bash

# Configure Livy based on environment variables
if [[ -n "${SPARK_MASTER}" ]]; then
    echo "livy.spark.master=${SPARK_MASTER}" >> "${LIVY_CONF_DIR}/livy.conf"
fi

if [[ -n "${SPARK_DEPLOY_MODE}" ]]; then
    echo "livy.spark.deploy-mode=${SPARK_DEPLOY_MODE}" >> "${LIVY_CONF_DIR}/livy.conf"
fi

# Start Livy server
exec "$LIVY_HOME/bin/livy-server"