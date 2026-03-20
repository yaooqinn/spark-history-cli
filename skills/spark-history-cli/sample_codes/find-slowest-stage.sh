#!/bin/bash
# Find the slowest stage in the latest Spark application
# Usage: bash find-slowest-stage.sh [server-url]

SERVER="${1:-http://localhost:18080}"

# Get latest app ID
APP_ID=$(spark-history-cli --json --server "$SERVER" apps --limit 1 | python -c "
import sys, json
apps = json.load(sys.stdin)
print(apps[0]['id'] if apps else '')
")

if [ -z "$APP_ID" ]; then
    echo "No applications found on $SERVER"
    exit 1
fi

echo "App: $APP_ID"
echo "---"

# Find slowest stage
spark-history-cli --json --server "$SERVER" -a "$APP_ID" stages | python -c "
import sys, json
stages = json.load(sys.stdin)
if not stages:
    print('No stages found')
    sys.exit(0)
completed = [s for s in stages if s.get('status') == 'COMPLETE']
if not completed:
    print('No completed stages')
    sys.exit(0)
slowest = max(completed, key=lambda s: s.get('executorRunTime', 0))
print(f'Slowest Stage: {slowest[\"stageId\"]}')
print(f'  Name:     {slowest.get(\"name\", \"N/A\")[:80]}')
print(f'  Duration: {slowest.get(\"executorRunTime\", 0) / 1000:.1f}s executor time')
print(f'  Tasks:    {slowest.get(\"numCompleteTasks\", 0)}')
print(f'  Input:    {slowest.get(\"inputBytes\", 0) / 1048576:.1f} MB')
print(f'  Shuffle:  {slowest.get(\"shuffleReadBytes\", 0) / 1048576:.1f} MB read, {slowest.get(\"shuffleWriteBytes\", 0) / 1048576:.1f} MB write')
"
