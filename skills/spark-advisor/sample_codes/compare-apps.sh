#!/bin/bash
# Compare two Spark applications side-by-side
# Usage: bash compare-apps.sh <app-id-1> <app-id-2> [server-url]

APP1="$1"
APP2="$2"
SERVER="${3:-http://localhost:18080}"

if [ -z "$APP1" ] || [ -z "$APP2" ]; then
    echo "Usage: bash compare-apps.sh <app-id-1> <app-id-2> [server-url]"
    exit 1
fi

echo "=== Application Comparison ==="
echo ""

for APP in "$APP1" "$APP2"; do
    echo "--- $APP ---"
    spark-history-cli --json --server "$SERVER" -a "$APP" summary | python -c "
import sys, json
data = json.load(sys.stdin)
app = data.get('application', {})
workload = data.get('workload', {})
print(f'  Name:       {app.get(\"name\", \"N/A\")}')
print(f'  Duration:   {app.get(\"duration\", \"N/A\")}')
print(f'  Jobs:       {workload.get(\"totalJobs\", \"N/A\")}')
print(f'  Stages:     {workload.get(\"totalStages\", \"N/A\")}')
print(f'  Tasks:      {workload.get(\"totalTasks\", \"N/A\")}')
print(f'  SQL Execs:  {workload.get(\"totalSqlExecutions\", \"N/A\")}')
"
    echo ""
done

echo "=== Config Diff ==="
diff <(spark-history-cli --json --server "$SERVER" -a "$APP1" env | python -c "
import sys, json
data = json.load(sys.stdin)
for item in sorted(data.get('sparkProperties', []), key=lambda x: x[0]):
    print(f'{item[0]} = {item[1]}')
") <(spark-history-cli --json --server "$SERVER" -a "$APP2" env | python -c "
import sys, json
data = json.load(sys.stdin)
for item in sorted(data.get('sparkProperties', []), key=lambda x: x[0]):
    print(f'{item[0]} = {item[1]}')
") || true
