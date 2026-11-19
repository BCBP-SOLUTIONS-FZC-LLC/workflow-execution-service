package main

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors/aliasconfig"
)

func TestFetchAliases_OneUndecodableRow_IsSkippedAndTheRestAreKept(t *testing.T) {
	body := `{"version":2,
	  "restCall":[
	    {"alias":"good","method":"GET","baseURL":"http://x.example","pathTemplate":"/y","timeout":2000000000},
	    {"alias":"stringy","method":"GET","baseURL":"http://x.example","pathTemplate":"/y","timeout":"2s"},
	    {"alias":"after","method":"POST","baseURL":"http://x.example","pathTemplate":"/z"}],
	  "sqlQuery":[
	    {"alias":"badcount","baseURL":"http://db.example","path":"/q","queryId":"q1","paramCount":"two"},
	    {"alias":"lookup","baseURL":"http://db.example","path":"/q","queryId":"q2","paramCount":1}]}`
	srv, _ := w27bAliasServer(t, http.StatusOK, body)
	log := &w27bFieldLogger{}

	cfg, err := fetchAliases(context.Background(), srv.URL, time.Second, log)

	require.NoError(t, err, "one bad row must not fail the whole registry fetch")
	assert.Equal(t, 2, cfg.Version)
	require.Len(t, cfg.RestCall, 2)
	assert.Equal(t, "good", cfg.RestCall[0].Alias)
	assert.Equal(t, 2*time.Second, cfg.RestCall[0].Timeout)
	assert.Equal(t, "after", cfg.RestCall[1].Alias, "rows after the bad one are still read")
	require.Len(t, cfg.SQLQuery, 1)
	assert.Equal(t, aliasconfig.Query{Alias: "lookup", BaseURL: "http://db.example", Path: "/q", QueryID: "q2", ParamCount: 1}, cfg.SQLQuery[0])
	assert.Equal(t, []map[string]any{
		{"kind": "restCall", "index": 1, "failure": "undecodable row"},
		{"kind": "sqlQuery", "index": 0, "failure": "undecodable row"},
	}, log.warns)
}

func TestFetchAliases_ABodyThatIsNotARegistry_IsStillAnError(t *testing.T) {
	cases := []struct{ name, body string }{
		{"not json", `{"version":`},
		{"rows are not a list", `{"version":1,"restCall":{"alias":"a"}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := w27bAliasServer(t, http.StatusOK, tc.body)

			_, err := fetchAliases(context.Background(), srv.URL, time.Second, &w27bFieldLogger{})

			require.Error(t, err)
			assert.Contains(t, err.Error(), "decode alias registry response")
		})
	}
}
