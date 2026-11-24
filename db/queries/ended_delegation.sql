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

-- name: MarkDelegateEnded :exec
-- Backs EndedDelegationRepo.MarkDelegateEnded -- a later cancel moves the time
-- forward, an older one never moves it back.
INSERT INTO ended_delegate (tenant_id, delegate_user_id, ended_at)
VALUES ($1, $2, $3)
ON CONFLICT (tenant_id, delegate_user_id) DO UPDATE
    SET ended_at = GREATEST(ended_delegate.ended_at, EXCLUDED.ended_at);

-- name: GetDelegateEndedAt :one
SELECT ended_at FROM ended_delegate WHERE tenant_id = $1 AND delegate_user_id = $2;
