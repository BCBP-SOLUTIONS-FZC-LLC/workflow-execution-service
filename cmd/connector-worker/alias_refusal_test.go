package main

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors/aliasconfig"
)

func TestRefreshAliases_AnEmptyAnswerOverAHeldRegistryWithoutRestCallAliasesIsAppliedAtOnce(t *testing.T) {
	var b atomic.Value
	b.Store(`{"version":1,"restCall":[]}`)
	d, cfg := aliasRefreshFixture(t, &b)
	d.aliases = aliasconfig.Config{SQLQuery: []aliasconfig.Query{
		{Alias: "lookup", BaseURL: "http://db.example", Path: "/q", QueryID: "q1"},
	}}

	refreshAliases(context.Background(), d, cfg, noopLogger{})

	assert.Zero(t, aliasCount(d.currentAliases()))
	assert.Empty(t, d.currentAliases().SQLQuery, "no rest-call alias was held, so there is nothing to protect")
}

func TestValidAliasRows_TheFailureKindFollowsTheRefusedFieldNotTheAliasText(t *testing.T) {
	const host = "http://host.example"
	cases := []struct {
		name string
		row  aliasconfig.Endpoint
		want string
	}{
		{"method under an alias that reads like a base URL", aliasconfig.Endpoint{Alias: "baseURLx", Method: "NOPE", BaseURL: host, PathTemplate: "/y"}, "method"},
		{"base URL under an alias that reads like a method", aliasconfig.Endpoint{Alias: "method-a", Method: "GET", BaseURL: "ftp://host.example", PathTemplate: "/y"}, "base URL"},
		{"path under an alias that reads like a base URL", aliasconfig.Endpoint{Alias: "baseURL-b", Method: "GET", BaseURL: host, PathTemplate: "y"}, "path"},
		{"path template fragment", aliasconfig.Endpoint{Alias: "frag", Method: "GET", BaseURL: host, PathTemplate: "/y#z"}, "path"},
		{"missing alias", aliasconfig.Endpoint{Method: "GET", BaseURL: host, PathTemplate: "/y"}, "invalid row"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			log := &w27bFieldLogger{}

			out := validAliasRows(aliasconfig.Config{RestCall: []aliasconfig.Endpoint{tc.row}}, log)

			assert.Empty(t, out.RestCall)
			require.Len(t, log.warns, 1)
			assert.Equal(t, tc.want, log.warns[0]["failure"])
		})
	}
}

func TestValidAliasRows_TheFailureKindOfASQLRowFollowsTheRefusedField(t *testing.T) {
	cases := []struct {
		name string
		row  aliasconfig.Query
		want string
	}{
		{"base URL", aliasconfig.Query{Alias: "pathTemplate-q", BaseURL: "ftp://db.example", Path: "/q", QueryID: "q1"}, "base URL"},
		{"path", aliasconfig.Query{Alias: "baseURL-q", BaseURL: "http://db.example", Path: "q", QueryID: "q1"}, "path"},
		{"query id", aliasconfig.Query{Alias: "baseURL-r", BaseURL: "http://db.example", Path: "/q"}, "invalid row"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			log := &w27bFieldLogger{}

			out := validAliasRows(aliasconfig.Config{SQLQuery: []aliasconfig.Query{tc.row}}, log)

			assert.Empty(t, out.SQLQuery)
			require.Len(t, log.warns, 1)
			assert.Equal(t, tc.want, log.warns[0]["failure"])
		})
	}
}
