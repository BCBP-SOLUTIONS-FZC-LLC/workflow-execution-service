-- name: GetProcessedEvent :one
SELECT * FROM processed_event WHERE event_id = $1 AND consumer = $2;

-- name: InsertProcessedEvent :execrows
INSERT INTO processed_event (event_id, consumer, event_type)
VALUES ($1, $2, $3)
ON CONFLICT (event_id, consumer) DO NOTHING;

-- name: PruneProcessedEventsOlderThan :execrows
DELETE FROM processed_event WHERE processed_at < $1;
