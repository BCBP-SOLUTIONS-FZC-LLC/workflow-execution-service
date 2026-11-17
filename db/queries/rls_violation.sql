-- name: RLSViolationHorizon :one
SELECT (now() - make_interval(secs => @lag_seconds::double precision))::timestamptz AS horizon;

-- name: CountRLSViolationsBetween :many
SELECT violation_type, count(*)::bigint AS total
FROM rls_violation_log
WHERE occurred_at > @after AND occurred_at <= @up_to
GROUP BY violation_type;

-- name: PruneRLSViolationsBefore :execrows
DELETE FROM rls_violation_log WHERE occurred_at < $1;
