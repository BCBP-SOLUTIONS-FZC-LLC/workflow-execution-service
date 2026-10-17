-- name: ListActiveTaskQueues :many
SELECT * FROM active_task_queues ORDER BY queue_name;

-- name: GetActiveTaskQueueByName :one
SELECT * FROM active_task_queues WHERE queue_name = $1;

-- name: RegisterActiveTaskQueue :one
INSERT INTO active_task_queues (id, tenant_id, queue_name, registered_at, updated_at)
VALUES ($1, $2, $3, now(), now())
ON CONFLICT (queue_name) DO UPDATE SET routable = true, updated_at = now()
RETURNING *;

-- name: StopActiveTaskQueueRouting :exec
UPDATE active_task_queues SET routable = false, updated_at = now() WHERE queue_name = $1;
