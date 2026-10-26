package http

import (
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

const (
	internalPrefix  = "/api/v1/internal"
	apiPrefix       = "/api/v1"
	publicSpecPath  = "../../../../docs/swagger/openapi.yaml"
	internalSpec    = "../../../../api/internal-openapi.yaml"
	asyncAPISpec    = "../../../../api/asyncapi.yaml"
	pathParamSyntax = `\{([^}]+)\}`
)

var operationalRoutes = map[string]bool{"/asyncapi": true}

func specOperations(t *testing.T, path string) []string {
	t.Helper()
	doc, err := openapi3.NewLoader().LoadFromFile(path)
	require.NoError(t, err)
	param := regexp.MustCompile(pathParamSyntax)
	var ops []string
	for p, item := range doc.Paths.Map() {
		for method := range item.Operations() {
			ops = append(ops, method+" "+param.ReplaceAllString(p, ":$1"))
		}
	}
	require.NotEmpty(t, ops)
	sort.Strings(ops)
	return ops
}

func routerOperations(t *testing.T, internal bool) []string {
	t.Helper()
	engine := testRouter(fakeDBPinger{health: DBHealth{Healthy: true}}, fakePinger{}, fakePinger{})
	var ops []string
	for _, r := range engine.Routes() {
		if operationalRoutes[r.Path] {
			continue
		}
		if strings.HasPrefix(r.Path, internalPrefix) != internal {
			continue
		}
		ops = append(ops, r.Method+" "+r.Path)
	}
	sort.Strings(ops)
	return ops
}

func TestPublicOpenAPIMatchesRoutes(t *testing.T) {
	assert.Equal(t, specOperations(t, publicSpecPath), routerOperations(t, false),
		"docs/swagger/openapi.yaml and the public routes must list the same operations")
}

func TestInternalOpenAPIMatchesRoutes(t *testing.T) {
	assert.Equal(t, specOperations(t, internalSpec), routerOperations(t, true),
		"api/internal-openapi.yaml and the internal routes must list the same operations")
}

type asyncMessages struct {
	Components struct {
		Messages map[string]struct {
			Name string `yaml:"name"`
		} `yaml:"messages"`
	} `yaml:"components"`
}

func asyncAPIMessageNames(t *testing.T) (inbound, outbound []string) {
	t.Helper()
	raw, err := os.ReadFile(asyncAPISpec)
	require.NoError(t, err)
	var doc asyncMessages
	require.NoError(t, yaml.Unmarshal(raw, &doc))
	for key, m := range doc.Components.Messages {
		if strings.HasSuffix(key, "Inbound") {
			inbound = append(inbound, m.Name)
			continue
		}
		outbound = append(outbound, key)
	}
	sort.Strings(inbound)
	sort.Strings(outbound)
	return inbound, outbound
}

func TestAsyncAPIInboundMessagesMatchHandledEvents(t *testing.T) {
	inbound, _ := asyncAPIMessageNames(t)
	handled := append([]string(nil), handler.HandledEventTypes()...)
	sort.Strings(handled)
	selfDispatched := "WorkflowTaskCreated"
	var documented []string
	documented = append(documented, inbound...)
	documented = append(documented, selfDispatched)
	sort.Strings(documented)
	assert.Equal(t, handled, documented, "api/asyncapi.yaml must document exactly the event types the inbound handler accepts")
}

func TestAsyncAPIOutboundMessagesMatchProducedEvents(t *testing.T) {
	_, outbound := asyncAPIMessageNames(t)
	produced := append([]string(nil), domain.OutboundEventTypes()...)
	sort.Strings(produced)
	assert.Equal(t, produced, outbound, "api/asyncapi.yaml must document exactly the event types the service produces")
}

func TestAsyncAPIDeadLetterQueuesAreNamedAfterTheirQueue(t *testing.T) {
	raw, err := os.ReadFile(asyncAPISpec)
	require.NoError(t, err)
	var doc struct {
		Channels map[string]struct {
			Bindings struct {
				SQS *struct {
					Queue struct {
						Name          string `yaml:"name"`
						RedrivePolicy struct {
							DeadLetterQueue struct {
								Name string `yaml:"name"`
							} `yaml:"deadLetterQueue"`
						} `yaml:"redrivePolicy"`
					} `yaml:"queue"`
				} `yaml:"sqs"`
			} `yaml:"bindings"`
		} `yaml:"channels"`
	}
	require.NoError(t, yaml.Unmarshal(raw, &doc))
	checked := 0
	for channel, c := range doc.Channels {
		if c.Bindings.SQS == nil {
			continue
		}
		q := c.Bindings.SQS.Queue
		assert.Equal(t, q.Name+"-dlq", q.RedrivePolicy.DeadLetterQueue.Name, "channel %s: the dead-letter queue is named <queue>-dlq in IAM's specs and the consumer's topology", channel)
		checked++
	}
	assert.NotZero(t, checked)
}
