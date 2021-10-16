#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CLICKHOUSE_CLIENT=$(echo ${CLICKHOUSE_CLIENT} | sed 's/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/--send_logs_level=none/g')

$CLICKHOUSE_CLIENT --query="SELECT 1 INTO OUTFILE 'test.native.zst' FORMAT Native" 2>&1 | grep -q "Code: 76. DB::ErrnoException: Cannot open file test.native.zst, errno: 17, strerror: File exists." && echo 'OK' || echo 'FAIL' ||:
$CLICKHOUSE_LOCAL --query="SELECT 1 INTO OUTFILE 'test.native.zst' FORMAT Native" 2>&1 | grep -q "Code: 76. DB::ErrnoException: Cannot open file test.native.zst, errno: 17, strerror: File exists." && echo 'OK' || echo 'FAIL' ||:

