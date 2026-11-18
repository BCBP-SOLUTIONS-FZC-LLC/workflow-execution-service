package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const reasonRequired = "is required"

// internalIDQuery reads a required id from the query of an internal call. An
// absent parameter is "is required"; a present one that is empty, malformed or
// the all-zero id is "is not valid".
func internalIDQuery(c *gin.Context, name string) (uuid.UUID, bool) {
	raw, present := c.GetQuery(name)
	if !present {
		writeParamProblem(c, name, name+" query parameter is required", reasonRequired)
		return uuid.Nil, false
	}
	id, ok := parseNonZeroUUID(raw)
	if !ok {
		writeInvalidParam(c, name, "invalid "+name+" query parameter")
		return uuid.Nil, false
	}
	return id, true
}

// internalOptionalIDQuery is internalIDQuery for a parameter that may be left
// out or empty; one that is given must be a non-zero UUID.
func internalOptionalIDQuery(c *gin.Context, name string) (*uuid.UUID, bool) {
	raw := c.Query(name)
	if raw == "" {
		return nil, true
	}
	id, ok := parseNonZeroUUID(raw)
	if !ok {
		writeInvalidParam(c, name, "invalid "+name+" query parameter")
		return nil, false
	}
	return &id, true
}

func parseNonZeroUUID(raw string) (uuid.UUID, bool) {
	id, err := uuid.Parse(raw)
	if err != nil || id == uuid.Nil {
		return uuid.Nil, false
	}
	return id, true
}

func writeParamProblem(c *gin.Context, name, detail, reason string) {
	writeProblem(c, http.StatusBadRequest, CodeBadRequest, detail, []invalidParam{{Name: name, Reason: reason}})
}
