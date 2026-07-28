-- name: GetProcessedEvent :one
SELECT * FROM processed_event WHERE event_id = $1 AND consumer = $2;

-- name: InsertProcessedEvent :exec
INSERT INTO processed_event (event_id, consumer, event_type)
VALUES ($1, $2, $3)
ON CONFLICT (event_id, consumer) DO NOTHING;
