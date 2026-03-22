# TEST.md — spark-history-cli Test Plan & Results

## Test Inventory

| File | Type | Planned Tests |
|------|------|---------------|
| `test_core.py` | Unit tests | ~60 tests |
| `test_full_e2e.py` | E2E tests (requires running SHS) | ~13 tests |

## Unit Test Plan (`test_core.py`)

### Client Tests (mocked HTTP)
- `test_get_version` — GET /api/v1/version
- `test_list_applications` — GET with status/limit params
- `test_get_application` — GET single app
- `test_list_jobs` — GET with status filter
- `test_get_job` — GET single job
- `test_list_stages` — GET with status filter
- `test_list_executors` — active executors
- `test_list_all_executors` — all executors
- `test_list_sql` — GET SQL executions
- `test_get_sql` — GET single SQL execution
- `test_list_rdds` — GET cached RDDs
- `test_get_environment` — GET environment
- `test_connection_error` — raises HistoryServerError
- `test_http_404` — raises HistoryServerError
- `test_check_health` — returns True/False

### Formatter Tests
- `test_format_app_list` — table output
- `test_format_app_detail` — status block
- `test_format_job_list` — table output
- `test_format_stage_list` — table output
- `test_format_executor_list` — table output
- `test_format_sql_list` — table output
- `test_parse_plan_sections_non_adaptive_uses_full_plan` — non-AQE fallback
- `test_parse_plan_sections_adaptive_splits_initial_and_final` — AQE section splitting
- `test_plan_to_dot_escapes_and_truncates_labels` — DOT export shape and escaping
- `test_duration_formatting` — ms, s, m, h
- `test_bytes_formatting` — B, KB, MB, GB

### Session Tests
- `test_session_set_app` — set/clear context
- `test_session_require_app` — raises ValueError when none
- `test_session_save_load` — JSON round-trip
- `test_session_context_label` — truncation

### CLI Subprocess Tests
- `test_help` — --help returns 0
- `test_version_subcommand` — version subcommand
- `test_apps_no_server` — graceful error when no server
- `test_sql_plan_json_returns_selected_view` — one-shot sql-plan JSON output
- `test_sql_plan_dot_writes_output_file` — one-shot sql-plan DOT file output
- `test_sql_jobs_json_outputs_matched_jobs` — one-shot sql-jobs JSON output
- `test_sql_jobs_without_referenced_jobs_prints_message` — no-job branch

### SQL Helper Tests
- `test_collect_sql_job_ids_deduplicates_and_sorts` — merges success/failed/running IDs
- `test_fetch_sql_jobs_filters_bulk_job_list` — bulk fetch + filter behavior

## E2E Test Plan (`test_full_e2e.py`)

Requires a running Spark History Server at `SPARK_HISTORY_SERVER` env var.

### Scenarios
- List all applications
- Get specific application details
- List jobs for an application
- List stages with metrics
- List executors
- List SQL executions
- Get environment info
- Download event logs as ZIP and verify format
- Full workflow: apps → pick first → jobs → stages → executors → env

## Realistic Workflow Scenarios

1. **Application investigation**: List apps → pick one → examine jobs → drill into failed stages → check task metrics
2. **Performance audit**: List executors → check memory/disk usage → examine shuffle metrics across stages
3. **SQL debugging**: List SQL executions → find slow query → examine plan nodes and metrics
