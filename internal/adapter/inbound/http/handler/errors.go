package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/go-playground/validator/v10"

	adapterhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

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
	CodeVersionNotFound         ErrCode = "VERSION_NOT_FOUND"
	CodeInstanceAlreadyTerminal ErrCode = "INSTANCE_ALREADY_TERMINAL"
	CodeInvalidInstanceState    ErrCode = "INVALID_INSTANCE_STATE"
	CodeForceBackNoSavedBranch  ErrCode = "FORCE_BACK_NO_SAVED_BRANCH"
	CodeOverrideMapInvalid      ErrCode = "OVERRIDE_MAP_INVALID"
	CodeTaskNotHumanActionable  ErrCode = "TASK_NOT_HUMAN_ACTIONABLE"
	CodeTaskNotConnectorTyped   ErrCode = "TASK_NOT_CONNECTOR_TYPED"
	CodeNoAutomationPrincipal   ErrCode = "NO_AUTOMATION_PRINCIPAL"
	CodeNotFound                ErrCode = "NOT_FOUND"
)

var codeTitles = map[ErrCode]string{
	CodeBadRequest:               "Bad Request",
	CodeUnauthorized:             "Unauthorized",
	CodeForbidden:                "Forbidden",
	CodeNotAssignee:              "Not Assigned",
	CodeNotAuthorizedForResource: "Not Authorized For Resource",
	CodeTaskNotFound:             "Task Not Found",
	CodeNotFound:                 "Not Found",
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
	CodeVersionNotFound:         "Version Not Found",
	CodeInstanceAlreadyTerminal: "Instance Already Terminal",
	CodeInvalidInstanceState:    "Invalid Instance State",
	CodeForceBackNoSavedBranch:  "Force Back No Saved Branch",
	CodeOverrideMapInvalid:      "Override Map Invalid",
	CodeTaskNotHumanActionable:  "Task Not Human Actionable",
	CodeTaskNotConnectorTyped:   "Task Not Connector Typed",
	CodeNoAutomationPrincipal:   "No Automation Principal",
}

const errBase = "https://errors.bcbp.io/execution/"

var problemTypes = map[ErrCode]string{
	CodeBadRequest:               errBase + "bad-request",
	CodeUnauthorized:             errBase + "unauthorized",
	CodeForbidden:                errBase + "forbidden",
	CodeNotAssignee:              errBase + "not-assignee",
	CodeNotAuthorizedForResource: errBase + "not-authorized-for-resource",
	CodeTaskNotFound:             errBase + "task-not-found",
	CodeNotFound:                 errBase + "not-found",
	CodeOverrideNoOp:             errBase + "override-no-op",
	CodeRecordVersionConflict:    errBase + "record-version-conflict",
	CodeTaskAlreadyClaimed:       errBase + "task-already-claimed",
	CodeClaimNotApplicable:       errBase + "claim-not-applicable",
	CodeInvalidTaskState:         errBase + "invalid-task-state",
	CodeNodeAlreadyResolved:      errBase + "node-already-resolved",
	CodeAssigneeIneligible:       errBase + "assignee-ineligible",
	CodeUpstreamUnavailable:      errBase + "upstream-unavailable",
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
	CodeVersionNotFound:         errBase + "version-not-found",
	CodeInstanceAlreadyTerminal: errBase + "instance-already-terminal",
	CodeInvalidInstanceState:    errBase + "invalid-instance-state",
	CodeForceBackNoSavedBranch:  errBase + "force-back-no-saved-branch",
	CodeOverrideMapInvalid:      errBase + "override-map-invalid",
	CodeTaskNotHumanActionable:  errBase + "task-not-human-actionable",
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
	TraceID       string         `json:"trace_id,omitempty"`
	RequestID     string         `json:"request_id,omitempty"`
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
		TraceID:       c.Writer.Header().Get("X-Trace-ID"),
		RequestID:     c.Writer.Header().Get("X-Request-ID"),
		InvalidParams: invalidParams,
	})
}

func bindErrResponse(c *gin.Context, err error) {
	if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
		writeProblem(c, http.StatusRequestEntityTooLarge, CodePayloadTooLarge, "request body exceeds the 10 MB limit", nil)
		return
	}
	detail, params := describeBindError(err)
	writeProblem(c, http.StatusBadRequest, CodeBadRequest, detail, params)
}

func describeBindError(err error) (string, []invalidParam) {
	var invalid validator.ValidationErrors
	var wrongType *json.UnmarshalTypeError
	var unknown *unknownFieldsError
	switch {
	case errors.As(err, &unknown):
		params := make([]invalidParam, 0, len(unknown.names))
		for _, name := range unknown.names {
			params = append(params, invalidParam{Name: name, Reason: "is not a known field"})
		}
		return "request body has fields this endpoint does not define", params
	case errors.As(err, &invalid):
		params := make([]invalidParam, 0, len(invalid))
		for _, fe := range invalid {
			reason := "is not valid"
			if fe.Tag() == "required" {
				reason = "is required"
			}
			params = append(params, invalidParam{Name: fe.Field(), Reason: reason})
		}
		return "request body has missing or invalid fields", params
	case errors.As(err, &wrongType):
		return "request body has a field of the wrong type", []invalidParam{{Name: wrongType.Field, Reason: "has the wrong type"}}
	case errors.Is(err, errTrailingData):
		return errTrailingData.Error(), nil
	default:
		return "request body is not valid JSON", nil
	}
}

func SpecUnavailable(c *gin.Context) {
	writeProblem(c, http.StatusInternalServerError, CodeInternal, "the AsyncAPI document is unavailable", nil)
}

func NotFound(c *gin.Context) {
	writeProblem(c, http.StatusNotFound, CodeNotFound, "no route matches this method and path", nil)
}

var errTrailingData = errors.New("request body has data after its JSON value")

const maxEchoedFieldName = 64

func bindStrictJSON(c *gin.Context, obj any) error {
	body, err := readBindBody(c)
	if err != nil {
		return err
	}
	if err := checkKnownFields(body, obj); err != nil {
		return err
	}
	return decodeOneValue(body, obj, true)
}

func bindInternalStrictJSON(c *gin.Context, obj any) error {
	body, err := readBindBody(c)
	if err != nil {
		return err
	}
	if err := checkFieldCase(body, obj); err != nil {
		return err
	}
	return decodeOneValue(body, obj, false)
}

func readBindBody(c *gin.Context) ([]byte, error) {
	if c.Request == nil || c.Request.Body == nil {
		return nil, errors.New("invalid request")
	}
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		return nil, fmt.Errorf("read request body: %w", err)
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return nil, fmt.Errorf("decode request body: %w", io.EOF)
	}
	return body, nil
}

func decodeOneValue(body []byte, obj any, disallowUnknown bool) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	if disallowUnknown {
		dec.DisallowUnknownFields()
	}
	if err := dec.Decode(obj); err != nil {
		return fmt.Errorf("decode request body: %w", err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return errTrailingData
	}
	if err := binding.Validator.ValidateStruct(obj); err != nil {
		return fmt.Errorf("validate request body: %w", err)
	}
	return nil
}

func errResponse(c *gin.Context, err error) {
	_ = c.Error(err)
	writeMappedProblem(c, err)
}

// writeMappedProblem answers err without attaching it to the request, for a caller that logs the cause itself.
func writeMappedProblem(c *gin.Context, err error) {
	status, code, detail := mapErr(err)
	if dependency := upstreamDependency(err); dependency != "" {
		observability.IncCounterVec(observability.UpstreamDependencyErrorsTotal, dependency)
	}
	writeProblem(c, status, code, detail, invalidParamsFor(err))
}

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
	if errors.Is(err, adapterhttp.ErrUpstreamUnavailable) || errors.Is(err, port.ErrEligibilityAnswerCount) {
		return "iam"
	}
	return ""
}

type refusal struct {
	sentinel error
	status   int
	code     ErrCode
}

// refusals answer with the sentinel's own text as the detail; a wrapped cause (SDK text, channel names) stays out of the body.
var refusals = []refusal{
	{port.ErrTaskNotFound, http.StatusNotFound, CodeTaskNotFound},
	{port.ErrNotAssignee, http.StatusForbidden, CodeNotAssignee},
	{port.ErrNotAuthorizedForRead, http.StatusForbidden, CodeNotAuthorizedForResource},
	{port.ErrRecordVersionConflict, http.StatusConflict, CodeRecordVersionConflict},
	{port.ErrTaskAlreadyClaimed, http.StatusConflict, CodeTaskAlreadyClaimed},
	{port.ErrClaimNotApplicable, http.StatusConflict, CodeClaimNotApplicable},
	{port.ErrInvalidTaskState, http.StatusConflict, CodeInvalidTaskState},
	{port.ErrNodeAlreadyResolved, http.StatusConflict, CodeNodeAlreadyResolved},
	{port.ErrTenantMismatch, http.StatusForbidden, CodeTenantMismatch},
	{port.ErrIdempotencyKeyReplay, http.StatusConflict, CodeIdempotencyReplay},
	{port.ErrOverrideNoOp, http.StatusBadRequest, CodeOverrideNoOp},
	{port.ErrInstanceNotFound, http.StatusNotFound, CodeInstanceNotFound},
	{port.ErrTargetNodeNotFound, http.StatusConflict, CodeTargetNodeNotFound},
	{port.ErrWaitNotFound, http.StatusNotFound, CodeWaitNotFound},
	{port.ErrNotWaiting, http.StatusConflict, CodeNotWaiting},
	{port.ErrDuplicateBusinessKey, http.StatusConflict, CodeDuplicateBusinessKey},
	{port.ErrTenantNotActive, http.StatusConflict, CodeTenantNotActive},
	{port.ErrVersionNotPublished, http.StatusConflict, CodeVersionNotPublished},
	{port.ErrVersionInvalid, http.StatusConflict, CodeVersionInvalid},
	{port.ErrVersionNotFound, http.StatusNotFound, CodeVersionNotFound},
	{port.ErrInstanceAlreadyTerminal, http.StatusConflict, CodeInstanceAlreadyTerminal},
	{port.ErrInvalidInstanceState, http.StatusConflict, CodeInvalidInstanceState},
	{port.ErrWorkflowNotFound, http.StatusConflict, CodeInvalidInstanceState},
	{port.ErrForceBackNoSavedBranch, http.StatusConflict, CodeForceBackNoSavedBranch},
	{port.ErrOverrideMapInvalid, http.StatusUnprocessableEntity, CodeOverrideMapInvalid},
	{port.ErrAssigneeIneligible, http.StatusUnprocessableEntity, CodeAssigneeIneligible},
	{port.ErrTaskNotHumanActionable, http.StatusConflict, CodeTaskNotHumanActionable},
	{port.ErrTaskNotConnectorTyped, http.StatusConflict, CodeTaskNotConnectorTyped},
	{port.ErrNoAutomationPrincipal, http.StatusConflict, CodeNoAutomationPrincipal},
}

// actionableRefusals are wrapped by the service with text built from the request (why the target was refused,
// which node key is invalid), so the caller gets that text to act on.
var actionableRefusals = map[error]bool{
	port.ErrTargetNodeNotFound: true,
	port.ErrOverrideMapInvalid: true,
}

func mapRefusal(err error) (status int, code ErrCode, detail string, ok bool) {
	for _, r := range refusals {
		if errors.Is(err, r.sentinel) {
			detail = r.sentinel.Error()
			if actionableRefusals[r.sentinel] {
				detail = err.Error()
			}
			if ineligible, typed := errors.AsType[*port.AssigneeIneligibleError](err); typed {
				detail = ineligible.Error()
			}
			return r.status, r.code, detail, true
		}
	}
	return 0, "", "", false
}

func mapErr(err error) (status int, code ErrCode, detail string) {
	if errors.Is(err, port.ErrUserPauseIncomplete) {
		return http.StatusServiceUnavailable, CodeUpstreamUnavailable, "some of the user's instances could not be paused; retry the call"
	}
	if status, code, detail, ok := mapRefusal(err); ok {
		return status, code, detail
	}
	switch {
	case errors.Is(err, port.ErrCallbackInFlight):
		return http.StatusServiceUnavailable, CodeUpstreamUnavailable, "an earlier attempt is still in progress; retry later"
	case errors.Is(err, port.ErrEligibilityAnswerCount):
		return http.StatusServiceUnavailable, CodeUpstreamUnavailable, "the eligibility check did not answer every assignee; retry the call"
	case errors.Is(err, adapterhttp.ErrUpstreamUnavailable):
		return http.StatusServiceUnavailable, CodeUpstreamUnavailable, "an upstream service could not serve the request"
	case errors.Is(err, port.ErrDefinitionUnavailable), errors.Is(err, port.ErrDefinitionRejected):
		return http.StatusServiceUnavailable, CodeUpstreamUnavailable, "the Definition Service could not serve the workflow version"
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusServiceUnavailable, CodeUpstreamUnavailable, "request timed out"
	case errors.Is(err, port.ErrTemporalUnavailable):
		return http.StatusServiceUnavailable, CodeUpstreamUnavailable, "the workflow engine could not take the request; retry it"
	default:
		return http.StatusInternalServerError, CodeInternal, "an unexpected error occurred"
	}
}
