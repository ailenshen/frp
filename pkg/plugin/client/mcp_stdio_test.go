// Copyright 2026 The frp Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !frps

package client

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v1 "github.com/fatedier/frp/pkg/config/v1"
)

func TestMCPStdioClassifyJSONRPC(t *testing.T) {
	cases := []struct {
		name               string
		body               string
		hasID              bool
		reqID              string
		isInit, isInitNote bool
	}{
		{"request", `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`, true, "1", false, false},
		{"initialize", `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`, true, "1", true, false},
		{"initialized note", `{"jsonrpc":"2.0","method":"notifications/initialized"}`, false, "", false, true},
		{"plain notification", `{"jsonrpc":"2.0","method":"notifications/cancelled"}`, false, "", false, false},
		{"explicit null id", `{"jsonrpc":"2.0","id":null,"method":"x"}`, false, "", false, false},
		{"string id", `{"jsonrpc":"2.0","id":"abc","method":"ping"}`, true, `"abc"`, false, false},
		{"malformed JSON", `not json`, true, "", false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotID, gotReqID, gotInit, gotInitNote := classifyJSONRPC([]byte(tc.body))
			require.Equal(t, tc.hasID, gotID, "hasID")
			require.Equal(t, tc.reqID, gotReqID, "reqID")
			require.Equal(t, tc.isInit, gotInit, "isInit")
			require.Equal(t, tc.isInitNote, gotInitNote, "isInitNote")
		})
	}
}

// fakeMCPScript is a tiny stdio program loaded via `/bin/sh script`. It
// counts every input line and echoes only those that look like JSON-RPC
// requests (have an "id" field), echoing the original id back so that
// readResponse can match it. The call count is embedded in the result so
// tests can detect when a new child instance has been spawned.
const fakeMCPScript = `n=0
while IFS= read -r line; do
  n=$((n+1))
  case "$line" in
    *'"id"'*)
      id=$(printf '%s' "$line" | grep -o '"id":[^,}]*' | sed 's/"id"://')
      printf '%s\n' "{\"jsonrpc\":\"2.0\",\"result\":\"call=${n}\",\"id\":${id}}"
      ;;
  esac
done
`

func TestMCPStdioDispatchAndReplay(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("uses /bin/sh stub")
	}

	dir := t.TempDir()
	scriptPath := dir + "/fake.sh"
	require.NoError(t, os.WriteFile(scriptPath, []byte(fakeMCPScript), 0o600))

	plugin, err := NewMCPStdioPlugin(PluginContext{}, &v1.MCPStdioPluginOptions{
		Type:    v1.PluginMCPStdio,
		Command: []string{"/bin/sh", scriptPath},
	})
	require.NoError(t, err)
	defer plugin.Close()

	p := plugin.(*MCPStdioPlugin)
	ctx := context.Background()

	// Initialize handshake (call=1 in child).
	resp, err := p.dispatch(ctx, []byte(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`))
	require.NoError(t, err)
	require.Contains(t, string(resp), `"call=1"`)

	// Notification: no response, but the plugin must cache it for replay (call=2 in child).
	resp, err = p.dispatch(ctx, []byte(`{"jsonrpc":"2.0","method":"notifications/initialized"}`))
	require.NoError(t, err)
	require.Nil(t, resp)

	// Regular request goes to the same child (call=3 because init=1, note=2).
	resp, err = p.dispatch(ctx, []byte(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`))
	require.NoError(t, err)
	require.Contains(t, string(resp), `"call=3"`)

	// Simulate an idle reap; the next request must respawn and replay
	// initialize + initialized-note before processing, so this request will
	// be call=3 in the new child (init=1, note=2, request=3).
	p.killChildNow()

	resp, err = p.dispatch(ctx, []byte(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`))
	require.NoError(t, err)
	require.Contains(t, string(resp), `"call=3"`)
}

// rootsMCPScript behaves like server-filesystem with a roots-capable client:
// after "notifications/initialized" it asks the client for roots/list (id 0)
// and holds back every response until that request is answered.
const rootsMCPScript = `pending=0; deferred=
while IFS= read -r line; do
  case "$line" in
    *'"method":"notifications/initialized"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":0,"method":"roots/list"}'
      pending=1
      ;;
    *'"error"'*)
      pending=0
      if [ -n "$deferred" ]; then
        printf '%s\n' "{\"jsonrpc\":\"2.0\",\"result\":\"ok\",\"id\":${deferred}}"
        deferred=
      fi
      ;;
    *'"id"'*)
      id=$(printf '%s' "$line" | grep -o '"id":[^,}]*' | sed 's/"id"://')
      if [ "$pending" = 1 ]; then
        deferred=$id
      else
        printf '%s\n' "{\"jsonrpc\":\"2.0\",\"result\":\"ok\",\"id\":${id}}"
      fi
      ;;
  esac
done
`

func TestMCPStdioRefusesServerRequests(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("uses /bin/sh stub")
	}

	dir := t.TempDir()
	scriptPath := dir + "/roots.sh"
	require.NoError(t, os.WriteFile(scriptPath, []byte(rootsMCPScript), 0o600))

	plugin, err := NewMCPStdioPlugin(PluginContext{}, &v1.MCPStdioPluginOptions{
		Type:    v1.PluginMCPStdio,
		Command: []string{"/bin/sh", scriptPath},
	})
	require.NoError(t, err)
	defer plugin.Close()

	p := plugin.(*MCPStdioPlugin)
	ctx := context.Background()

	_, err = p.dispatch(ctx, []byte(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`))
	require.NoError(t, err)
	_, err = p.dispatch(ctx, []byte(`{"jsonrpc":"2.0","method":"notifications/initialized"}`))
	require.NoError(t, err)

	// The client request reuses id 0, the same id as the child's roots/list:
	// the bridge must not mistake that request for the response, must answer
	// it so the child unblocks, and must then return the real response.
	resp, err := p.dispatch(ctx, []byte(`{"jsonrpc":"2.0","id":0,"method":"tools/list"}`))
	require.NoError(t, err)
	require.JSONEq(t, `{"jsonrpc":"2.0","result":"ok","id":0}`, string(resp))
}

func TestMCPStdioTimeoutRepliesWithJSONRPCError(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("uses /bin/sh stub")
	}

	saved := mcpStdioReadTimeout
	mcpStdioReadTimeout = 200 * time.Millisecond
	defer func() { mcpStdioReadTimeout = saved }()

	dir := t.TempDir()
	scriptPath := dir + "/silent.sh"
	// A child that reads requests but never answers.
	require.NoError(t, os.WriteFile(scriptPath, []byte("while IFS= read -r line; do :; done\n"), 0o600))

	plugin, err := NewMCPStdioPlugin(PluginContext{}, &v1.MCPStdioPluginOptions{
		Type:    v1.PluginMCPStdio,
		Command: []string{"/bin/sh", scriptPath},
	})
	require.NoError(t, err)
	defer plugin.Close()

	p := plugin.(*MCPStdioPlugin)
	resp, err := p.dispatch(context.Background(), []byte(`{"jsonrpc":"2.0","id":7,"method":"tools/call"}`))
	require.NoError(t, err)

	var got struct {
		ID    int `json:"id"`
		Error struct {
			Code int `json:"code"`
		} `json:"error"`
	}
	require.NoError(t, json.Unmarshal(resp, &got))
	require.Equal(t, 7, got.ID)
	require.Equal(t, mcpStdioTimeoutCode, got.Error.Code)
}

// concurrentMCPScript answers "slow" after 0.3s in the background, never
// answers "never", exits on "die", answers a notifications/cancelled by
// resolving the cancelled request, and answers anything else at once with its
// line count (which restarts at 1 in a respawned child).
const concurrentMCPScript = `n=0
while IFS= read -r line; do
  n=$((n+1))
  id=$(printf '%s' "$line" | grep -o '"id":[^,}]*' | head -1 | sed 's/"id"://')
  case "$line" in
    *'"method":"notifications/cancelled"'*)
      rid=$(printf '%s' "$line" | grep -o '"requestId":[^,}]*' | sed 's/"requestId"://')
      printf '%s\n' "{\"jsonrpc\":\"2.0\",\"result\":\"cancelled\",\"id\":${rid}}"
      ;;
    *'"method":"slow"'*)
      ( sleep 0.3; printf '%s\n' "{\"jsonrpc\":\"2.0\",\"result\":\"slow\",\"id\":${id}}" ) &
      ;;
    *'"method":"never"'*) ;;
    *'"method":"die"'*) exit 0 ;;
    *)
      if [ -n "$id" ]; then
        printf '%s\n' "{\"jsonrpc\":\"2.0\",\"result\":\"call=${n}\",\"id\":${id}}"
      fi
      ;;
  esac
done
`

func newConcurrentTestPlugin(t *testing.T) *MCPStdioPlugin {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("uses /bin/sh stub")
	}
	scriptPath := t.TempDir() + "/concurrent.sh"
	require.NoError(t, os.WriteFile(scriptPath, []byte(concurrentMCPScript), 0o600))
	plugin, err := NewMCPStdioPlugin(PluginContext{}, &v1.MCPStdioPluginOptions{
		Type:    v1.PluginMCPStdio,
		Command: []string{"/bin/sh", scriptPath},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = plugin.Close() })
	return plugin.(*MCPStdioPlugin)
}

type testRPCResponse struct {
	ID     int    `json:"id"`
	Result string `json:"result"`
	Error  struct {
		Code int `json:"code"`
	} `json:"error"`
}

func dispatchAsync(p *MCPStdioPlugin, body string) <-chan testRPCResponse {
	ch := make(chan testRPCResponse, 1)
	go func() {
		var r testRPCResponse
		if resp, err := p.dispatch(context.Background(), []byte(body)); err == nil {
			_ = json.Unmarshal(resp, &r)
		}
		ch <- r
	}()
	return ch
}

func TestMCPStdioConcurrentRequestsShareIDs(t *testing.T) {
	p := newConcurrentTestPlugin(t)

	// Two callers both use id 1. The fast call must not wait behind the slow
	// one, and each caller must get its own answer under its own id.
	slow := dispatchAsync(p, `{"jsonrpc":"2.0","id":1,"method":"slow"}`)
	time.Sleep(50 * time.Millisecond)
	fast := dispatchAsync(p, `{"jsonrpc":"2.0","id":1,"method":"ping"}`)

	select {
	case r := <-fast:
		require.Equal(t, 1, r.ID)
		require.Equal(t, "call=2", r.Result)
	case <-slow:
		t.Fatal("slow call answered before the fast one")
	}
	r := <-slow
	require.Equal(t, 1, r.ID)
	require.Equal(t, "slow", r.Result)
}

func TestMCPStdioTimeoutKeepsResponsiveChild(t *testing.T) {
	saved := mcpStdioReadTimeout
	mcpStdioReadTimeout = 300 * time.Millisecond
	defer func() { mcpStdioReadTimeout = saved }()
	p := newConcurrentTestPlugin(t)

	never := dispatchAsync(p, `{"jsonrpc":"2.0","id":9,"method":"never"}`)
	// Keep the child talking while "never" waits out its timeout.
	for i := 0; i < 5; i++ {
		time.Sleep(100 * time.Millisecond)
		r := <-dispatchAsync(p, `{"jsonrpc":"2.0","id":2,"method":"ping"}`)
		require.NotEmpty(t, r.Result)
	}
	r := <-never
	require.Equal(t, 9, r.ID)
	require.Equal(t, mcpStdioTimeoutCode, r.Error.Code)

	// Same child: the line count keeps going (the bridge's cancellation of
	// "never" counts as a line too) instead of restarting at 1.
	r = <-dispatchAsync(p, `{"jsonrpc":"2.0","id":3,"method":"ping"}`)
	require.NotEqual(t, "call=1", r.Result)
	require.NotEmpty(t, r.Result)
}

func TestMCPStdioChildExitFailsFastAndRespawns(t *testing.T) {
	p := newConcurrentTestPlugin(t)

	start := time.Now()
	r := <-dispatchAsync(p, `{"jsonrpc":"2.0","id":4,"method":"die"}`)
	require.Equal(t, 4, r.ID)
	require.Equal(t, mcpStdioClosedCode, r.Error.Code)
	require.Less(t, time.Since(start), 5*time.Second, "an exited child must not wait out the timeout")

	r = <-dispatchAsync(p, `{"jsonrpc":"2.0","id":5,"method":"ping"}`)
	require.Equal(t, 5, r.ID)
	require.Equal(t, "call=1", r.Result)
}

func TestMCPStdioCancelledNotificationMapsID(t *testing.T) {
	p := newConcurrentTestPlugin(t)

	// Use up a few bridge ids so the client's id differs from the bridge's.
	for i := 0; i < 3; i++ {
		<-dispatchAsync(p, `{"jsonrpc":"2.0","id":1,"method":"ping"}`)
	}
	never := dispatchAsync(p, `{"jsonrpc":"2.0","id":1,"method":"never"}`)
	time.Sleep(50 * time.Millisecond)
	_, err := p.dispatch(context.Background(),
		[]byte(`{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":1}}`))
	require.NoError(t, err)

	// The fake resolves whatever requestId it was told to cancel; it only
	// reaches the "never" caller if the bridge mapped the id.
	select {
	case r := <-never:
		require.Equal(t, 1, r.ID)
		require.Equal(t, "cancelled", r.Result)
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation did not reach the child under the bridge id")
	}
}

func TestMCPStdioHTTPHandler(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("uses /bin/sh stub")
	}

	dir := t.TempDir()
	scriptPath := dir + "/fake.sh"
	require.NoError(t, os.WriteFile(scriptPath, []byte(fakeMCPScript), 0o600))

	plugin, err := NewMCPStdioPlugin(PluginContext{}, &v1.MCPStdioPluginOptions{
		Type:    v1.PluginMCPStdio,
		Command: []string{"/bin/sh", scriptPath},
	})
	require.NoError(t, err)
	defer plugin.Close()

	p := plugin.(*MCPStdioPlugin)

	// Wrong method
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	p.handle(w, r)
	require.Equal(t, http.StatusMethodNotAllowed, w.Code)

	// Empty body
	r = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(""))
	w = httptest.NewRecorder()
	p.handle(w, r)
	require.Equal(t, http.StatusBadRequest, w.Code)

	// Notification → 202 with empty body
	r = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"jsonrpc":"2.0","method":"x"}`))
	w = httptest.NewRecorder()
	p.handle(w, r)
	require.Equal(t, http.StatusAccepted, w.Code)
	require.Empty(t, w.Body.String())

	// Request with id → 200 + body
	r = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"jsonrpc":"2.0","id":7,"method":"ping"}`))
	w = httptest.NewRecorder()
	p.handle(w, r)
	require.Equal(t, http.StatusOK, w.Code)
	require.True(t, bytes.Contains(w.Body.Bytes(), []byte(`"call=`)))
	require.Equal(t, "application/json", w.Header().Get("Content-Type"))
}
