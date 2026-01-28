#!/bin/bash
set -e

# Git bisect script for testing information_schema table query bug
# This script builds TiDB, starts the server, runs a test query, and checks the result.

TIDB_PORT=4000
STATUS_PORT=10080
TIDB_LOG="/tmp/tidb-bisect.log"
TIDB_PID=""
MAIN_GO_FILE="cmd/tidb-server/main.go"

cleanup() {
    echo "Cleaning up..."
    if [ -n "$TIDB_PID" ] && kill -0 "$TIDB_PID" 2>/dev/null; then
        kill "$TIDB_PID" 2>/dev/null || true
        wait "$TIDB_PID" 2>/dev/null || true
    fi
    # Kill any remaining tidb-server processes on the port
    pkill -f "tidb-server.*-P $TIDB_PORT" 2>/dev/null || true
    sleep 1

    # Revert the change using git checkout
    echo "Reverting changes to $MAIN_GO_FILE..."
    git checkout -f "$MAIN_GO_FILE"
}

trap cleanup EXIT INT TERM

# Patch the file
echo "==> Patching $MAIN_GO_FILE..."
sed -i 's/err = memory.InitMemoryHook()/\_ = memory.InitMemoryHook()/g' "$MAIN_GO_FILE"

echo "==> Building TiDB..."
if ! make; then
    echo "Build failed, skipping this commit"
    exit 125  # Git bisect skip code
fi

echo "==> Starting TiDB server..."
./bin/tidb-server -P $TIDB_PORT --store unistore --log-file "$TIDB_LOG" \
    --host 127.0.0.1 --status $STATUS_PORT &
TIDB_PID=$!

echo "==> Waiting for TiDB to be ready..."
for i in {1..30}; do
    if mysql -h 127.0.0.1 -P $TIDB_PORT -u root -e "SELECT 1" &>/dev/null; then
        echo "TiDB is ready"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "TiDB failed to start in time"
        exit 125  # Skip this commit
    fi
    sleep 1
done

echo "==> Running test query..."
RESULT=$(mysql -h 127.0.0.1 -P $TIDB_PORT -u root -N -s test <<'EOF'
drop table if exists aa_log, bb_log, aa_log1;
create table aa_log(a int);
create table bb_log(b int);
create table aa_log1(a int);
select table_schema, table_name from information_schema.tables where table_schema = 'test' and table_name like '%g' order by table_name;
EOF
)

echo "Query result:"
echo "$RESULT"

# Check if the result matches the expected output (should contain aa_log and bb_log)
if echo "$RESULT" | grep -q "aa_log" && echo "$RESULT" | grep -q "bb_log"; then
    echo "==> GOOD: Query returned expected results"
    exit 0  # Good commit
else
    echo "==> BAD: Query did not return expected results"
    exit 1  # Bad commit
fi

