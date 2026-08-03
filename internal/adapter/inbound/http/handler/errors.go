package handler

import (
	"context"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	adapterhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// ErrCode is this task's RFC-9457 `code` vocabulary.
type ErrCode string

const (
	CodeBadRequest               ErrCode = "BAD_REQUEST"
	CodeUnauthorized             ErrCode = "UNAUTHORIZED"
	CodeForbidden                ErrCode = "FORBIDDEN"
	CodeNotAssignee              ErrCode = "NOT_ASSIGNEE"
	CodeNotAuthorizedForResource ErrCode = "NOT_AUTHORIZED_FOR_RESOURCE"
	CodeTaskNotFound             ErrCode = "TASK_NOT_FOUND"
	CodeOverrideNoOp             ErrCode = "OVERRIDE_NO_OP"
	CodeRecordVersionConflict    ErrCode = "RECORD_VERSION_CONFLICT"
	CodeTaskAlreadyClaimed       ErrCode = "TASK_ALREADY_CLAIMED"
	CodeClaimNotApplicable       ErrCode = "CLAIM_NOT_APPLICABLE"
	CodeInvalidTaskState         ErrCode = "INVALID_TASK_STATE"
	CodeNodeAlreadyResolved      ErrCode = "NODE_ALREADY_RESOLVED"
	CodeAssigneeIneligible       ErrCode = "ASSIGNEE_INELIGIBLE"
	CodeUpstreamUnavailable      ErrCode = "UPSTREAM_UNAVAILABLE"
	CodePayloadTooLarge          ErrCode = "PAYLOAD_TOO_LARGE"
	CodeUnsupportedMediaType     ErrCode = "UNSUPPORTED_MEDIA_TYPE"
	CodeInternal                 ErrCode = "INTERNAL_ERROR"
	CodeTenantMismatch           ErrCode = "TENANT_MISMATCH"
	CodeIdempotencyReplay        ErrCode = "IDEMPOTENCY_KEY_REPLAY"
)

var codeTitles = map[ErrCode]string{
	CodeBadRequest:               "Bad Request",
	CodeUnauthorized:             "Unauthorized",
	CodeForbidden:                "Forbidden",
	CodeNotAssignee:              "Not Assigned",
	CodeNotAuthorizedForResource: "Not Authorized For Resource",
	CodeTaskNotFound:             "Task Not Found",
	CodeOverrideNoOp:             "Override No-Op",
	CodeRecordVersionConflict:    "Record Version Conflict",
	CodeTaskAlreadyClaimed:       "Task Already Claimed",
	CodeClaimNotApplicable:       "Claim Not Applicable",
	CodeInvalidTaskState:         "Invalid Task State",
	CodeNodeAlreadyResolved:      "Node Already Resolved",
	CodeAssigneeIneligible:       "Assignee Ineligible",
	CodeUpstreamUnavailable:      "Service Unavailable",
	CodePayloadTooLarge:          "Payload Too Large",
	CodeUnsupportedMediaType:     "Unsupported Media Type",
	CodeInternal:                 "Internal Server Error",
	CodeTenantMismatch:           "Tenant Mismatch",
	CodeIdempotencyReplay:        "Idempotency Key Replay",
}

const errBase = "https://errors.bcbp.io/execution/"

var problemTypes = map[ErrCode]string{
	CodeBadRequest:               errBase + "bad-request",
	CodeUnauthorized:             errBase + "unauthorized",
	CodeForbidden:                errBase + "forbidden",
	CodeNotAssignee:              errBase + "assignee-not-assigned",
	CodeNotAuthorizedForResource: errBase + "not-authorized-for-resource",
	CodeTaskNotFound:             errBase + "task-not-found",
	CodeOverrideNoOp:             errBase + "override-no-op",
	CodeRecordVersionConflict:    errBase + "record-version-conflict",
	CodeTaskAlreadyClaimed:       errBase + "task-already-claimed",
	CodeClaimNotApplicable:       errBase + "claim-not-applicable",
	CodeInvalidTaskState:         errBase + "invalid-task-state",
	CodeNodeAlreadyResolved:      errBase + "node-already-resolved",
	CodeAssigneeIneligible:       errBase + "assignee-ineligible",
	CodeUpstreamUnavailable:      errBase + "temporal-unavailable",
	CodePayloadTooLarge:          errBase + "payload-too-large",
	CodeUnsupportedMediaType:     errBase + "unsupported-media-type",
	CodeInternal:                 errBase + "internal-error",
	CodeTenantMismatch:           errBase + "tenant-mismatch",
	CodeIdempotencyReplay:        errBase + "idempotency-key-replay",
}

type problemDetails struct {
	Type     string  `json:"type"`
	Title    string  `json:"title"`
	Status   int     `json:"status"`
	Detail   string  `json:"detail"`
	Instance string  `json:"instance"`
	Code     ErrCode `json:"code"`
}

func writeProblem(c *gin.Context, status int, code ErrCode, detail string, _ any) {
	typeURI, ok := problemTypes[code]
	if !ok {
		typeURI = errBase + "internal-error"
	}
	c.Header("Content-Type", "application/problem+json")
	c.JSON(status, problemDetails{
		Type:     typeURI,
		Title:    codeTitles[code],
		Status:   status,
		Detail:   detail,
		Instance: c.Request.URL.Path,
		Code:     code,
	})
}

func bindErrResponse(c *gin.Context, err error) {
	if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
		writeProblem(c, http.StatusRequestEntityTooLarge, CodePayloadTooLarge, "request body exceeds the 10 MB limit", nil)
		return
	}
	writeProblem(c, http.StatusBadRequest, CodeBadRequest, err.Error(), nil)
}

func errResponse(c *gin.Context, err error) {
	status, code, detail := mapErr(err)
	writeProblem(c, status, code, detail, nil)
}

func mapErr(err error) (status int, code ErrCode, detail string) {
	switch {
	case errors.Is(err, port.ErrTaskNotFound):
		return http.StatusNotFound, CodeTaskNotFound, err.Error()
	case errors.Is(err, port.ErrNotAssignee):
		return http.StatusForbidden, CodeNotAssignee, err.Error()
	case errors.Is(err, port.ErrNotAuthorizedForRead):
		return http.StatusForbidden, CodeNotAuthorizedForResource, err.Error()
	case errors.Is(err, port.ErrRecordVersionConflict):
		return http.StatusConflict, CodeRecordVersionConflict, err.Error()
	case errors.Is(err, port.ErrTaskAlreadyClaimed):
		return http.StatusConflict, CodeTaskAlreadyClaimed, err.Error()
	case errors.Is(err, port.ErrClaimNotApplicable):
		return http.StatusConflict, CodeClaimNotApplicable, err.Error()
	case errors.Is(err, port.ErrInvalidTaskState):
		return http.StatusConflict, CodeInvalidTaskState, err.Error()
	case errors.Is(err, port.ErrNodeAlreadyResolved):
		return http.StatusConflict, CodeNodeAlreadyResolved, err.Error()
	case errors.Is(err, port.ErrTenantMismatch):
		return http.StatusForbidden, CodeTenantMismatch, err.Error()
	case errors.Is(err, port.ErrIdempotencyKeyReplay):
		return http.StatusConflict, CodeIdempotencyReplay, err.Error()
	case errors.Is(err, port.ErrOverrideNoOp):
		return http.StatusBadRequest, CodeOverrideNoOp, err.Error()
	case errors.Is(err, adapterhttp.ErrUpstreamUnavailable):
		return http.StatusServiceUnavailable, CodeUpstreamUnavailable, err.Error()
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusServiceUnavailable, CodeUpstreamUnavailable, "request timed out"
	default:
		// Catch-all for anything TaskService returns that isn't one of the
		// sentinels above. Extend this switch with a new port.Err* sentinel
		// and ErrCode as narrower failure modes are identified — don't route
		// new cases through the generic 500 by default.
		return http.StatusInternalServerError, CodeInternal, "an unexpected error occurred"
	}
}
