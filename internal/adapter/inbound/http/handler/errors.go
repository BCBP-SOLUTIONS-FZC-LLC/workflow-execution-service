package handler

import (
	"context"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	adapterhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
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
	CodeEventDecodeFailed        ErrCode = "EVENT_DECODE_FAILED"

	CodeInstanceNotFound        ErrCode = "INSTANCE_NOT_FOUND"
	CodeTargetNodeNotFound      ErrCode = "TARGET_NODE_NOT_FOUND"
	CodeWaitNotFound            ErrCode = "WAIT_NOT_FOUND"
	CodeNotWaiting              ErrCode = "NOT_WAITING"
	CodeDuplicateBusinessKey    ErrCode = "DUPLICATE_BUSINESS_KEY"
	CodeTenantNotActive         ErrCode = "TENANT_NOT_ACTIVE"
	CodeVersionNotPublished     ErrCode = "VERSION_NOT_PUBLISHED"
	CodeVersionInvalid          ErrCode = "VERSION_INVALID"
	CodeInstanceAlreadyTerminal ErrCode = "INSTANCE_ALREADY_TERMINAL"
	CodeInvalidInstanceState    ErrCode = "INVALID_INSTANCE_STATE"
	CodeForceBackNoSavedBranch  ErrCode = "FORCE_BACK_NO_SAVED_BRANCH"
	CodeOverrideMapInvalid      ErrCode = "OVERRIDE_MAP_INVALID"
	CodeTaskNotHumanActionable  ErrCode = "TASK_NOT_HUMAN_ACTIONABLE"
	CodeAssigneeUnavailable     ErrCode = "ASSIGNEE_UNAVAILABLE"
	CodeTaskNotConnectorTyped   ErrCode = "TASK_NOT_CONNECTOR_TYPED"
	CodeNoAutomationPrincipal   ErrCode = "NO_AUTOMATION_PRINCIPAL"
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
	CodeEventDecodeFailed:        "Event Decode Failed",

	CodeInstanceNotFound:        "Instance Not Found",
	CodeTargetNodeNotFound:      "Target Node Not Found",
	CodeWaitNotFound:            "Wait Not Found",
	CodeNotWaiting:              "Not Waiting",
	CodeDuplicateBusinessKey:    "Duplicate Business Key",
	CodeTenantNotActive:         "Tenant Not Active",
	CodeVersionNotPublished:     "Version Not Published",
	CodeVersionInvalid:          "Version Invalid",
	CodeInstanceAlreadyTerminal: "Instance Already Terminal",
	CodeInvalidInstanceState:    "Invalid Instance State",
	CodeForceBackNoSavedBranch:  "Force Back No Saved Branch",
	CodeOverrideMapInvalid:      "Override Map Invalid",
	CodeTaskNotHumanActionable:  "Task Not Human Actionable",
	CodeAssigneeUnavailable:     "Assignee Unavailable",
	CodeTaskNotConnectorTyped:   "Task Not Connector Typed",
	CodeNoAutomationPrincipal:   "No Automation Principal",
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
	CodeEventDecodeFailed:        errBase + "event-decode-failed",

	CodeInstanceNotFound:        errBase + "instance-not-found",
	CodeTargetNodeNotFound:      errBase + "target-node-not-found",
	CodeWaitNotFound:            errBase + "wait-not-found",
	CodeNotWaiting:              errBase + "not-waiting",
	CodeDuplicateBusinessKey:    errBase + "duplicate-business-key",
	CodeTenantNotActive:         errBase + "tenant-not-active",
	CodeVersionNotPublished:     errBase + "version-not-published",
	CodeVersionInvalid:          errBase + "version-invalid",
	CodeInstanceAlreadyTerminal: errBase + "instance-already-terminal",
	CodeInvalidInstanceState:    errBase + "invalid-instance-state",
	CodeForceBackNoSavedBranch:  errBase + "force-back-no-saved-branch",
	CodeOverrideMapInvalid:      errBase + "override-map-invalid",
	CodeTaskNotHumanActionable:  errBase + "task-not-human-actionable",
	CodeAssigneeUnavailable:     errBase + "assignee-unavailable",
	CodeTaskNotConnectorTyped:   errBase + "task-not-connector-typed",
	CodeNoAutomationPrincipal:   errBase + "no-automation-principal",
}

type invalidParam struct {
	Name   string  `json:"name"`
	Reason string  `json:"reason"`
	Code   ErrCode `json:"code,omitempty"`
}

type problemDetails struct {
	Type          string         `json:"type"`
	Title         string         `json:"title"`
	Status        int            `json:"status"`
	Detail        string         `json:"detail"`
	Instance      string         `json:"instance"`
	Code          ErrCode        `json:"code"`
	InvalidParams []invalidParam `json:"invalid_params,omitempty"`
}

func writeProblem(c *gin.Context, status int, code ErrCode, detail string, invalidParams []invalidParam) {
	typeURI, ok := problemTypes[code]
	if !ok {
		typeURI = errBase + "internal-error"
	}
	c.Header("Content-Type", "application/problem+json")
	c.JSON(status, problemDetails{
		Type:          typeURI,
		Title:         codeTitles[code],
		Status:        status,
		Detail:        detail,
		Instance:      c.Request.URL.Path,
		Code:          code,
		InvalidParams: invalidParams,
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
	if dependency := upstreamDependency(err); dependency != "" {
		observability.IncCounterVec(observability.UpstreamDependencyErrorsTotal, dependency)
	}
	writeProblem(c, status, code, detail, invalidParamsFor(err))
}

// invalidParamsFor extracts the invalid_params list (LLD §5.5 step 3: "naming
// every offending node in one payload") from error types that carry one —
// today, only *port.AssigneeIneligibleError. Every other error returns nil,
// which problemDetails.InvalidParams (omitempty) simply omits.
func invalidParamsFor(err error) []invalidParam {
	var ineligible *port.AssigneeIneligibleError
	if errors.As(err, &ineligible) {
		params := make([]invalidParam, len(ineligible.Nodes))
		for i, node := range ineligible.Nodes {
			params[i] = invalidParam{Name: node, Reason: "assignee ineligible", Code: CodeAssigneeIneligible}
		}
		return params
	}
	return nil
}

func upstreamDependency(err error) string {
	if errors.Is(err, port.ErrDefinitionUnavailable) || errors.Is(err, port.ErrDefinitionRejected) {
		return "definition_service"
	}
	if errors.Is(err, adapterhttp.ErrUpstreamUnavailable) {
		return "iam"
	}
	return ""
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
	case errors.Is(err, port.ErrInstanceNotFound):
		return http.StatusNotFound, CodeInstanceNotFound, err.Error()
	case errors.Is(err, port.ErrTargetNodeNotFound):
		return http.StatusConflict, CodeTargetNodeNotFound, err.Error()
	case errors.Is(err, port.ErrWaitNotFound):
		return http.StatusNotFound, CodeWaitNotFound, err.Error()
	case errors.Is(err, port.ErrNotWaiting):
		return http.StatusConflict, CodeNotWaiting, err.Error()
	case errors.Is(err, port.ErrDuplicateBusinessKey):
		return http.StatusConflict, CodeDuplicateBusinessKey, err.Error()
	case errors.Is(err, port.ErrTenantNotActive):
		return http.StatusConflict, CodeTenantNotActive, err.Error()
	case errors.Is(err, port.ErrVersionNotPublished):
		return http.StatusConflict, CodeVersionNotPublished, err.Error()
	case errors.Is(err, port.ErrVersionInvalid):
		return http.StatusConflict, CodeVersionInvalid, err.Error()
	case errors.Is(err, port.ErrInstanceAlreadyTerminal):
		return http.StatusConflict, CodeInstanceAlreadyTerminal, err.Error()
	case errors.Is(err, port.ErrInvalidInstanceState):
		return http.StatusConflict, CodeInvalidInstanceState, err.Error()
	case errors.Is(err, port.ErrForceBackNoSavedBranch):
		return http.StatusConflict, CodeForceBackNoSavedBranch, err.Error()
	case errors.Is(err, port.ErrOverrideMapInvalid):
		return http.StatusUnprocessableEntity, CodeOverrideMapInvalid, err.Error()
	case errors.Is(err, port.ErrAssigneeIneligible):
		return http.StatusUnprocessableEntity, CodeAssigneeIneligible, err.Error()
	case errors.Is(err, port.ErrTaskNotHumanActionable):
		return http.StatusConflict, CodeTaskNotHumanActionable, err.Error()
	case errors.Is(err, port.ErrAssigneeUnavailable):
		return http.StatusConflict, CodeAssigneeUnavailable, err.Error()
	case errors.Is(err, port.ErrTaskNotConnectorTyped):
		return http.StatusConflict, CodeTaskNotConnectorTyped, err.Error()
	case errors.Is(err, port.ErrNoAutomationPrincipal):
		return http.StatusConflict, CodeNoAutomationPrincipal, err.Error()
	case errors.Is(err, adapterhttp.ErrUpstreamUnavailable):
		return http.StatusServiceUnavailable, CodeUpstreamUnavailable, err.Error()
	// This catalogue has no narrower code than UPSTREAM_UNAVAILABLE for a
	// Definition Service rejection (LLD §5.10), so both of its outcomes map here.
	// The cause carries the Definition Service's internal URL and the tenant,
	// so the detail is fixed; the service that read the plan logs the cause.
	case errors.Is(err, port.ErrDefinitionUnavailable), errors.Is(err, port.ErrDefinitionRejected):
		return http.StatusServiceUnavailable, CodeUpstreamUnavailable, "the Definition Service could not serve the workflow version"
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
