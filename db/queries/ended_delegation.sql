-- name: MarkDelegationEnded :exec
-- Backs EndedDelegationRepo.MarkEnded -- repeated for the same delegation, it
-- keeps the first end.
INSERT INTO ended_delegation (tenant_id, delegation_id)
VALUES ($1, $2)
ON CONFLICT (tenant_id, delegation_id) DO NOTHING;

-- name: DelegationIsEnded :one
SELECT EXISTS (
    SELECT 1 FROM ended_delegation WHERE tenant_id = $1 AND delegation_id = $2
)::boolean AS ended;
