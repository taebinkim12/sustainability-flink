#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

FLINK_VERSION="1.18.0"
FLINK_DIR="flink-${FLINK_VERSION}"
FLINK_TAR="flink-${FLINK_VERSION}-bin-scala_2.12.tgz"
DOWNLOAD_URL="https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}/${FLINK_TAR}"

echo "=========================================="
echo "  Flink Local Cluster Setup & Execution   "
echo "=========================================="

# 1. Download and extract Flink if not present
if [ ! -d "$FLINK_DIR" ]; then
    echo "[INFO] Flink ${FLINK_VERSION} not found locally. Downloading..."
    if [ ! -f "$FLINK_TAR" ]; then
        wget -q "$DOWNLOAD_URL" -O "$FLINK_TAR"
    fi
    echo "[INFO] Extracting Flink..."
    tar -xzf "$FLINK_TAR"
    rm "$FLINK_TAR"
    echo "[INFO] Flink downloaded and extracted."
else
    echo "[INFO] Flink ${FLINK_VERSION} is already present locally."
fi

# 2. Configure the standalone cluster
echo "[INFO] Configuring Flink for high-performance standalone execution..."
CONF_FILE="${FLINK_DIR}/conf/flink-conf.yaml"

# Update flink-conf.yaml to support the remote machine's cores
# Use sed to safely replace or append configurations
if grep -q "^taskmanager.numberOfTaskSlots:" "$CONF_FILE"; then
    sed -i.bak 's/^taskmanager.numberOfTaskSlots:.*/taskmanager.numberOfTaskSlots: 128/' "$CONF_FILE"
else
    echo "taskmanager.numberOfTaskSlots: 128" >> "$CONF_FILE"
fi

if grep -q "^taskmanager.memory.process.size:" "$CONF_FILE"; then
    sed -i.bak 's/^taskmanager.memory.process.size:.*/taskmanager.memory.process.size: 16384m/' "$CONF_FILE"
else
    echo "taskmanager.memory.process.size: 16384m" >> "$CONF_FILE"
fi

# Ensure network buffers are properly scaled for 128 parallelism
if grep -q "^taskmanager.memory.network.max:" "$CONF_FILE"; then
    sed -i.bak 's/^taskmanager.memory.network.max:.*/taskmanager.memory.network.max: 2gb/' "$CONF_FILE"
else
    echo "taskmanager.memory.network.max: 2gb" >> "$CONF_FILE"
fi

# 3. Start the Flink cluster
echo "[INFO] Starting Flink local standalone cluster..."
./${FLINK_DIR}/bin/start-cluster.sh

# 4. Run the experiment
echo "[INFO] Submitting job to the cluster..."
export PATH="$PWD/${FLINK_DIR}/bin:$PATH"

# Trap to ensure the cluster shuts down even if the job fails
cleanup() {
    echo "[INFO] Shutting down Flink local standalone cluster..."
    ./${FLINK_DIR}/bin/stop-cluster.sh
}
trap cleanup EXIT

# Execute the existing run_experiment script in distributed mode, passing all arguments along
./run_experiment.sh --execution-mode distributed "$@"

echo "[INFO] Execution complete."
