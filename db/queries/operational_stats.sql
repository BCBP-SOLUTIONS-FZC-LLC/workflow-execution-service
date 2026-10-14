-- name: OperationalStats :one
SELECT
    (SELECT count(*) FROM workflow_instance WHERE status = 'DEGRADED')::bigint AS degraded_instances,
    COALESCE((SELECT EXTRACT(EPOCH FROM (now() - min(updated_at))) FROM workflow_instance WHERE status = 'DEGRADED'), 0)::float8 AS oldest_degraded_age_seconds,
    COALESCE((SELECT EXTRACT(EPOCH FROM (now() - min(created_at))) FROM workflow_task WHERE status = 'READY'), 0)::float8 AS oldest_ready_task_age_seconds;
