-- name: LatestRLSViolationID :one
SELECT COALESCE(max(id), 0)::bigint AS latest_id FROM rls_violation_log;

-- name: CountRLSViolationsAfter :many
SELECT violation_type, count(*)::bigint AS total, max(id)::bigint AS newest_id
FROM rls_violation_log
WHERE id > $1
GROUP BY violation_type;

-- name: PruneRLSViolationsBefore :execrows
DELETE FROM rls_violation_log WHERE occurred_at < $1;
