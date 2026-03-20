# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#!/usr/bin/env python3
"""Generate sample Spark event logs for CI testing.

Creates synthetic event log files in Spark's JSON event log format.
These are NOT real production logs — they contain only fabricated test data.
"""

import json
import os
import sys
import time

# Event log directory (matches SHS config)
LOG_DIR = os.environ.get("SPARK_EVENT_LOG_DIR", "/tmp/spark-events")


def _ts(offset_s: int = 0) -> int:
    """Epoch milliseconds with offset."""
    return int((time.time() + offset_s) * 1000)


def generate_app_event_log(
    app_id: str,
    app_name: str,
    num_jobs: int = 2,
    num_stages_per_job: int = 2,
    num_tasks_per_stage: int = 4,
    spark_user: str = "ci-test",
    attempt_id: str = "1",
):
    """Generate a complete event log file for one Spark application."""
    events = []
    base_ts = _ts(-3600)  # 1 hour ago

    # -- SparkListenerLogStart
    events.append({
        "Event": "SparkListenerLogStart",
        "Spark Version": "4.0.0",
    })

    # -- SparkListenerEnvironmentUpdate
    events.append({
        "Event": "SparkListenerEnvironmentUpdate",
        "JVM Information": {
            "Java Home": "/usr/lib/jvm/java-17",
            "Java Version": "17.0.10 (Eclipse Adoptium)",
            "Scala Version": "version 2.13.14",
        },
        "Spark Properties": {
            "spark.app.name": app_name,
            "spark.app.id": app_id,
            "spark.master": "local[4]",
            "spark.executor.memory": "1g",
            "spark.driver.memory": "1g",
            "spark.sql.shuffle.partitions": "4",
        },
        "Hadoop Properties": {},
        "System Properties": {
            "java.version": "17.0.10",
            "os.name": "Linux",
        },
        "Classpath Entries": {},
        "Metrics Properties": {},
    })

    # -- SparkListenerApplicationStart
    events.append({
        "Event": "SparkListenerApplicationStart",
        "App Name": app_name,
        "App ID": app_id,
        "Timestamp": base_ts,
        "User": spark_user,
        "App Attempt ID": attempt_id,
    })

    # -- SparkListenerResourceProfileAdded
    events.append({
        "Event": "SparkListenerResourceProfileAdded",
        "Resource Profile Id": 0,
        "Executor Resource Requests": {
            "cores": {"Resource Name": "cores", "Amount": 4, "Discovery Script": "", "Vendor": ""},
            "memory": {"Resource Name": "memory", "Amount": 1024, "Discovery Script": "", "Vendor": ""},
        },
        "Task Resource Requests": {
            "cpus": {"Resource Name": "cpus", "Amount": 1.0},
        },
    })

    # -- SparkListenerExecutorAdded (driver + 2 executors)
    for exec_id, host in [("driver", "localhost"), ("1", "worker-1"), ("2", "worker-2")]:
        events.append({
            "Event": "SparkListenerExecutorAdded",
            "Timestamp": base_ts + 1000,
            "Executor ID": exec_id,
            "Executor Info": {
                "Host": host,
                "Total Cores": 4,
                "Log Urls": {},
                "Attributes": {},
                "Resources": {},
                "Resource Profile Id": 0,
                "Registration Time": base_ts + 1000,
                "Request Time": base_ts + 500,
            },
        })

    stage_id_counter = 0
    task_id_counter = 0
    t = base_ts + 2000

    for job_id in range(num_jobs):
        stage_ids = list(range(stage_id_counter, stage_id_counter + num_stages_per_job))

        # -- SparkListenerJobStart
        events.append({
            "Event": "SparkListenerJobStart",
            "Job ID": job_id,
            "Submission Time": t,
            "Stage Infos": [
                {
                    "Stage ID": sid,
                    "Stage Attempt ID": 0,
                    "Stage Name": f"stage_{sid} at test.py:{sid + 10}",
                    "Number of Tasks": num_tasks_per_stage,
                    "RDD Info": [{
                        "RDD ID": sid,
                        "Name": f"MapPartitionsRDD[{sid}]",
                        "Scope": json.dumps({"id": str(sid), "name": "map"}),
                        "Callsite": f"test.py:{sid + 10}",
                        "Parent IDs": [sid - 1] if sid > 0 else [],
                        "Storage Level": {
                            "Use Disk": False, "Use Memory": False,
                            "Deserialized": False, "Replication": 1,
                        },
                        "Barrier": False,
                        "DeterministicLevel": "DETERMINATE",
                        "Number of Partitions": num_tasks_per_stage,
                    }],
                    "Parent IDs": [sid - 1] if sid > stage_ids[0] else [],
                    "Details": f"org.apache.spark.rdd.RDD.map(test.py:{sid + 10})",
                    "Resource Profile Id": 0,
                }
                for sid in stage_ids
            ],
            "Stage IDs": stage_ids,
            "Properties": {
                "spark.job.description": f"Test job {job_id}",
                "spark.jobGroup.id": f"group-{job_id}",
            },
        })

        for sid in stage_ids:
            # -- SparkListenerStageSubmitted
            events.append({
                "Event": "SparkListenerStageSubmitted",
                "Stage Info": {
                    "Stage ID": sid,
                    "Stage Attempt ID": 0,
                    "Stage Name": f"stage_{sid} at test.py:{sid + 10}",
                    "Number of Tasks": num_tasks_per_stage,
                    "RDD Info": [],
                    "Parent IDs": [],
                    "Details": "",
                    "Submission Time": t,
                    "Resource Profile Id": 0,
                },
            })

            # Tasks
            for task_idx in range(num_tasks_per_stage):
                tid = task_id_counter
                task_id_counter += 1
                executor = "1" if task_idx % 2 == 0 else "2"
                task_start = t + task_idx * 200

                # -- SparkListenerTaskStart
                events.append({
                    "Event": "SparkListenerTaskStart",
                    "Stage ID": sid,
                    "Stage Attempt ID": 0,
                    "Task Info": {
                        "Task ID": tid,
                        "Index": task_idx,
                        "Attempt": 0,
                        "Partition ID": task_idx,
                        "Launch Time": task_start,
                        "Executor ID": executor,
                        "Host": f"worker-{executor}",
                        "Locality": "PROCESS_LOCAL",
                        "Speculative": False,
                        "Getting Result Time": 0,
                        "Finish Time": 0,
                        "Failed": False,
                        "Killed": False,
                        "Accumulables": [],
                    },
                })

                task_end = task_start + 500 + task_idx * 100
                # -- SparkListenerTaskEnd
                events.append({
                    "Event": "SparkListenerTaskEnd",
                    "Stage ID": sid,
                    "Stage Attempt ID": 0,
                    "Task Type": "ResultTask" if sid == stage_ids[-1] else "ShuffleMapTask",
                    "Task End Reason": {"Reason": "Success"},
                    "Task Info": {
                        "Task ID": tid,
                        "Index": task_idx,
                        "Attempt": 0,
                        "Partition ID": task_idx,
                        "Launch Time": task_start,
                        "Executor ID": executor,
                        "Host": f"worker-{executor}",
                        "Locality": "PROCESS_LOCAL",
                        "Speculative": False,
                        "Getting Result Time": 0,
                        "Finish Time": task_end,
                        "Failed": False,
                        "Killed": False,
                        "Accumulables": [],
                    },
                    "Task Executor Metrics": {
                        "JVMHeapMemory": 50000000,
                        "JVMOffHeapMemory": 10000000,
                        "OnHeapExecutionMemory": 0,
                        "OffHeapExecutionMemory": 0,
                        "OnHeapStorageMemory": 5000000,
                        "OffHeapStorageMemory": 0,
                        "OnHeapUnifiedMemory": 5000000,
                        "OffHeapUnifiedMemory": 0,
                        "DirectPoolMemory": 1000000,
                        "MappedPoolMemory": 0,
                        "MinorGCCount": 5,
                        "MinorGCTime": 50,
                        "MajorGCCount": 0,
                        "MajorGCTime": 0,
                    },
                    "Task Metrics": {
                        "Executor Deserialize Time": 10,
                        "Executor Deserialize CPU Time": 8000000,
                        "Executor Run Time": task_end - task_start - 20,
                        "Executor CPU Time": (task_end - task_start - 30) * 1000000,
                        "Peak Execution Memory": 1048576,
                        "Result Size": 2048,
                        "JVM GC Time": 5,
                        "Result Serialization Time": 2,
                        "Memory Bytes Spilled": 0,
                        "Disk Bytes Spilled": 0,
                        "Shuffle Read Metrics": {
                            "Remote Blocks Fetched": 2 if sid > 0 else 0,
                            "Local Blocks Fetched": 2 if sid > 0 else 0,
                            "Fetch Wait Time": 3 if sid > 0 else 0,
                            "Remote Bytes Read": 4096 if sid > 0 else 0,
                            "Remote Bytes Read To Disk": 0,
                            "Local Bytes Read": 4096 if sid > 0 else 0,
                            "Total Records Read": 100 if sid > 0 else 0,
                            "Remote Requests Duration": 0,
                            "Push Based Shuffle": {
                                "Corrupt Merged Block Chunks": 0,
                                "Merged Fetch Fallback Count": 0,
                                "Remote Merged Blocks Fetched": 0,
                                "Local Merged Blocks Fetched": 0,
                                "Remote Merged Chunks Fetched": 0,
                                "Local Merged Chunks Fetched": 0,
                                "Remote Merged Bytes Read": 0,
                                "Local Merged Bytes Read": 0,
                                "Remote Merged Requests Duration": 0,
                            },
                        },
                        "Shuffle Write Metrics": {
                            "Shuffle Bytes Written": 8192 if sid < stage_ids[-1] else 0,
                            "Shuffle Write Time": 5000000 if sid < stage_ids[-1] else 0,
                            "Shuffle Records Written": 100 if sid < stage_ids[-1] else 0,
                        },
                        "Input Metrics": {
                            "Bytes Read": 65536 if sid == 0 else 0,
                            "Records Read": 1000 if sid == 0 else 0,
                        },
                        "Output Metrics": {
                            "Bytes Written": 32768 if sid == stage_ids[-1] else 0,
                            "Records Written": 500 if sid == stage_ids[-1] else 0,
                        },
                        "Updated Blocks": [],
                    },
                })

            t += num_tasks_per_stage * 300 + 500

            # -- SparkListenerStageCompleted
            events.append({
                "Event": "SparkListenerStageCompleted",
                "Stage Info": {
                    "Stage ID": sid,
                    "Stage Attempt ID": 0,
                    "Stage Name": f"stage_{sid} at test.py:{sid + 10}",
                    "Number of Tasks": num_tasks_per_stage,
                    "RDD Info": [],
                    "Parent IDs": [],
                    "Details": "",
                    "Submission Time": t - 2000,
                    "Completion Time": t,
                    "Resource Profile Id": 0,
                },
            })

            stage_id_counter += 1

        # -- SparkListenerJobEnd
        events.append({
            "Event": "SparkListenerJobEnd",
            "Job ID": job_id,
            "Completion Time": t,
            "Job Result": {"Result": "JobSucceeded"},
        })

        t += 1000

    # -- SparkListenerApplicationEnd
    events.append({
        "Event": "SparkListenerApplicationEnd",
        "Timestamp": t,
    })

    return events


def write_event_log(app_id: str, events: list[dict], attempt_id: str = "1"):
    """Write events to a Spark event log file."""
    app_dir = os.path.join(LOG_DIR, app_id)
    os.makedirs(app_dir, exist_ok=True)

    log_file = os.path.join(app_dir, f"application_{attempt_id}")
    with open(log_file, "w") as f:
        for event in events:
            f.write(json.dumps(event, separators=(",", ":")) + "\n")

    # Mark as completed by creating the .inprogress removal
    # SHS looks for files without .inprogress suffix as completed
    print(f"  Written: {log_file} ({len(events)} events)")
    return log_file


def main():
    os.makedirs(LOG_DIR, exist_ok=True)
    print(f"Generating sample event logs in {LOG_DIR}")

    # App 1: simple 2-job app
    events1 = generate_app_event_log(
        app_id="app-ci-test-0001",
        app_name="CI Test App - Simple",
        num_jobs=2,
        num_stages_per_job=2,
        num_tasks_per_stage=4,
    )
    write_event_log("app-ci-test-0001", events1)

    # App 2: larger app with more stages
    events2 = generate_app_event_log(
        app_id="app-ci-test-0002",
        app_name="CI Test App - Complex",
        num_jobs=3,
        num_stages_per_job=3,
        num_tasks_per_stage=8,
    )
    write_event_log("app-ci-test-0002", events2)

    print(f"\nGenerated 2 sample applications in {LOG_DIR}")
    print("Event log files:")
    for root, dirs, files in os.walk(LOG_DIR):
        for f in files:
            path = os.path.join(root, f)
            size = os.path.getsize(path)
            print(f"  {path} ({size:,} bytes)")


if __name__ == "__main__":
    main()
