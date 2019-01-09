#!/bin/bash

# Ensure loader is available
EXE_FILE_NAME=${EXE_FILE_NAME:-$(which tsbs_load_siridb)}
if [[ -z "$EXE_FILE_NAME" ]]; then
    echo "tsbs_load_siridb not available. It is not specified explicitly and not found in \$PATH"
    exit 1
fi

# Load parameters - common
DATA_FILE_NAME=${DATA_FILE_NAME:-siridb-data.gz}

# Load parameters - personal
DATABASE_USER=${DATABASE_USER:-iris}
DATABASE_PASS=${DATABASE_PASS:-siri}

EXE_DIR=${EXE_DIR:-$(dirname $0)}
source ${EXE_DIR}/load_common.sh

until nc -z ${DATABASE_HOST} ${DATABASE_PORT}; do
    xterm -e ~/workspace/siridb-server/Release/siridb-server -c  ~/workspace/dbtest/siridb0.conf &
    echo "Waiting for SiriDB"
    sleep 1
done

cat ${DATA_FILE} | gunzip | $EXE_FILE_NAME \
                                --db-name=${DATABASE_NAME} \
                                --host=${DATABASE_HOST} \
                                --port=${DATABASE_PORT} \
                                --dbuser=${DATABASE_USER} \
                                --dbpass=${DATABASE_PASS} \
                                --workers=${NUM_WORKERS} \
                                --batch-size=${BATCH_SIZE} \
                                --reporting-period=${REPORTING_PERIOD} \



