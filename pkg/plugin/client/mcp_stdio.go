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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"time"

	v1 "github.com/fatedier/frp/pkg/config/v1"
	"github.com/fatedier/frp/pkg/util/log"
	netpkg "github.com/fatedier/frp/pkg/util/net"
)

const (
	// mcpStdioReapInterval is how often the worker checks for an idle child.
	mcpStdioReapInterval = 30 * time.Second
	// mcpStdioMaxBodySize caps inbound HTTP request bodies to guard against
	// memory exhaustion; JSON-RPC frames are small in practice.
	mcpStdioMaxBodySize = 1 << 20 // 1 MiB
	// mcpStdioTimeoutCode is the JSON-RPC error code for a request the child
	// did not answer in time (the MCP SDKs' RequestTimeout code).
	mcpStdioTimeoutCode = -32001
	// mcpStdioClosedCode is the JSON-RPC error code for a request lost because
	// the child exited or was restarted (the MCP SDKs' ConnectionClosed code).
	mcpStdioClosedCode = -32000
)

// mcpStdioReadTimeout is the per-request deadline for a child response. It
// must stay above every child's own worst case, so a slow tool call fails
// inside the child with its own error first. mcp-safari waits up to 30s per
// extension call and caps run_steps at 60s, plus post-action waits; a 30s
// deadline here used to tie with it and get mcp-safari killed, whose new auth
// token then locked the Safari extension out for ~24s.
// It is a var so tests can shorten it.
// Refactor: move to a per-proxy option like IdleTimeoutSeconds once a child
// needs a materially different limit.
var mcpStdioReadTimeout = 120 * time.Second

var errMCPStdioChildTimeout = errors.New("child response timeout")

func init() {
	Register(v1.PluginMCPStdio, NewMCPStdioPlugin)
}

// MCPStdioPlugin exposes a stdio-based MCP server (a child process that speaks
// newline-delimited JSON-RPC on stdin/stdout) over the Streamable HTTP
// transport. Each inbound HTTP POST body is forwarded as one JSON-RPC line;
// the child's response with the matching id is returned as the HTTP response.
// Requests from concurrent HTTP calls are in flight at the same time.
//
// The child is spawned lazily on the first request and is killed when idle
// (when IdleTimeoutSeconds > 0). To keep MCP sessions intact across respawns,
// the plugin caches the most recent "initialize" request and the
// "notifications/initialized" notification, replaying them whenever a new
// child is started.
//
// Because a stdio MCP server only supports a single "initialize" per
// process lifetime, a client "initialize" sent while the current child is
// already initialized is not forwarded: the cached "initialize" response is
// replayed back to the client (with its id rewritten to match), and a
// repeat "notifications/initialized" is swallowed. This lets HTTP clients
// re-initialize (e.g. after losing their session) without breaking a
// long-lived child that never gets reaped.
//
// A single worker goroutine owns the child process and its stdin, and a
// reader goroutine per child feeds it stdout lines, so no mutexes are needed.
// HTTP handlers communicate with the worker via channels.
type MCPStdioPlugin struct {
	opts    *v1.MCPStdioPluginOptions
	idleTTL time.Duration

	l *Listener
	s *http.Server

	reqCh      chan dispatchReq
	closeCh    chan struct{}
	workerDone chan struct{}
	killCh     chan chan struct{} // signals worker to kill the child and ack
}

type dispatchReq struct {
	body    []byte
	replyCh chan dispatchResp
}

type dispatchResp struct {
	data []byte
	err  error
}

// NewMCPStdioPlugin constructs the plugin from validated options.
func NewMCPStdioPlugin(_ PluginContext, options v1.ClientPluginOptions) (Plugin, error) {
	opts := options.(*v1.MCPStdioPluginOptions)
	if len(opts.Command) == 0 {
		return nil, errors.New("mcp_stdio: command is required")
	}

	listener := NewProxyListener()

	p := &MCPStdioPlugin{
		opts:       opts,
		idleTTL:    time.Duration(opts.IdleTimeoutSeconds) * time.Second,
		l:          listener,
		reqCh:      make(chan dispatchReq),
		closeCh:    make(chan struct{}),
		workerDone: make(chan struct{}),
		killCh:     make(chan chan struct{}),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", p.handle)
	p.s = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 60 * time.Second,
	}

	go func() { _ = p.s.Serve(listener) }()
	go p.worker()
	return p, nil
}

func (p *MCPStdioPlugin) handle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, mcpStdioMaxBodySize)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	body = bytes.TrimRight(body, "\r\n")
	if len(body) == 0 {
		http.Error(w, "empty body", http.StatusBadRequest)
		return
	}
	// Compact to a single line so multi-line pretty-printed JSON does not
	// split into multiple stdio frames when forwarded to the child.
	if compact := bytes.NewBuffer(make([]byte, 0, len(body))); json.Compact(compact, body) == nil {
		body = compact.Bytes()
	}

	resp, err := p.dispatch(r.Context(), body)
	if err != nil {
		log.Warnf("mcp_stdio: dispatch error: %v", err)
		http.Error(w, "upstream error: "+err.Error(), http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if resp == nil {
		// Notifications carry no id and produce no response on stdio either;
		// per MCP HTTP spec we return 202 Accepted with an empty body.
		w.WriteHeader(http.StatusAccepted)
		return
	}
	_, _ = w.Write(resp)
}

// dispatch sends a JSON-RPC frame to the worker and waits for the response.
// It returns nil data for notifications (no id field), respecting ctx for
// cancellation throughout.
func (p *MCPStdioPlugin) dispatch(ctx context.Context, body []byte) ([]byte, error) {
	replyCh := make(chan dispatchResp, 1)
	select {
	case p.reqCh <- dispatchReq{body: body, replyCh: replyCh}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.closeCh:
		return nil, errors.New("plugin closing")
	}
	select {
	case resp := <-replyCh:
		return resp.data, resp.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.closeCh:
		return nil, errors.New("plugin closing")
	}
}

// worker is the single goroutine that owns the child process, its stdin and
// all request bookkeeping. A reader goroutine per child turns stdout into
// lines; the worker routes each response to its caller by id. Several
// requests can therefore be in flight at once, and a slow call no longer
// holds up the ones behind it (Claude Code talks to the same servers this way
// over plain stdio). Because all state lives here, no mutexes are needed.
//
// Client ids are rewritten to bridge-assigned ids on the way in and restored
// on the way out, since separate HTTP sessions reuse the same small ids.
func (p *MCPStdioPlugin) worker() {
	defer close(p.workerDone)

	type pendingReq struct {
		clientID json.RawMessage
		replyCh  chan dispatchResp
		sentAt   time.Time
		isInit   bool
	}

	name := p.opts.Command[0]
	var (
		child      *exec.Cmd
		childIn    io.WriteCloser
		childLines <-chan []byte
		childDone  chan struct{} // closed to stop the current reader goroutine
		lastUsedAt time.Time
		lastOutput time.Time // when the current child last wrote to stdout

		pending = map[int64]*pendingReq{}
		nextID  int64

		cachedInitReq  []byte
		cachedInitNote []byte
		cachedInitResp []byte

		// childInitialized and childInitNoteSent track the current child's
		// handshake state; they are reset whenever the child is killed.
		childInitialized  bool
		childInitNoteSent bool

		deadline   *time.Timer
		deadlineCh <-chan time.Time
	)

	send := func(frame []byte) error {
		_, err := fmt.Fprintf(childIn, "%s\n", frame)
		return err
	}

	// failPending answers every in-flight request with a JSON-RPC error.
	failPending := func(code int, msg string) {
		for id, pr := range pending {
			pr.replyCh <- dispatchResp{data: errorResponse(pr.clientID, code, msg)}
			delete(pending, id)
		}
	}

	killChild := func() {
		if child == nil {
			return
		}
		close(childDone)
		_ = childIn.Close()
		if child.Process != nil {
			_ = child.Process.Kill()
		}
		_ = child.Wait()
		child, childIn, childLines, childDone = nil, nil, nil, nil
		childInitialized = false
		childInitNoteSent = false
	}

	// refuse answers a server-to-client request (e.g. roots/list). It has no
	// way back over one-shot HTTP, and servers such as server-filesystem stall
	// every request until it is answered, so refuse it and let them fall back.
	// Refactor: forward it instead once mcp_stdio can relay server requests to
	// the HTTP client (e.g. over an SSE stream).
	refuse := func(id json.RawMessage, method string) {
		if reply, err := refuseServerRequest(id, method); err == nil {
			_ = send(reply)
		}
	}

	// awaitInit waits for the child's answer to a replayed "initialize",
	// before any client request is in flight on the new child.
	awaitInit := func(id int64) ([]byte, error) {
		timer := time.NewTimer(mcpStdioReadTimeout)
		defer timer.Stop()
		for {
			select {
			case line, ok := <-childLines:
				if !ok {
					return nil, errors.New("child exited during initialize")
				}
				head := parseHead(line)
				if head.Method != "" && head.hasID() {
					refuse(head.ID, head.Method)
				} else if head.Method == "" && string(head.ID) == strconv.FormatInt(id, 10) {
					return line, nil
				}
			case <-timer.C:
				return nil, errMCPStdioChildTimeout
			case <-p.closeCh:
				return nil, errors.New("plugin closing")
			}
		}
	}

	ensureChild := func() error {
		if child != nil {
			return nil
		}
		cmd := exec.Command(p.opts.Command[0], p.opts.Command[1:]...)
		cmd.Env = os.Environ()
		for k, v := range p.opts.Env {
			cmd.Env = append(cmd.Env, k+"="+v)
		}
		stdin, err := cmd.StdinPipe()
		if err != nil {
			return err
		}
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			return err
		}
		stderr, err := cmd.StderrPipe()
		if err != nil {
			return err
		}
		if err := cmd.Start(); err != nil {
			return err
		}
		go pipeStderrToLog(stderr, name)
		lines, done := make(chan []byte), make(chan struct{})
		go readChildLines(stdout, lines, done)

		child, childIn, childLines, childDone = cmd, stdin, lines, done
		lastUsedAt, lastOutput = time.Now(), time.Now()

		if cachedInitReq != nil {
			nextID++
			frame, err := withReplacedID(cachedInitReq, strconv.FormatInt(nextID, 10))
			if err == nil {
				err = send(frame)
			}
			var resp []byte
			if err == nil {
				resp, err = awaitInit(nextID)
			}
			if err != nil {
				killChild()
				return fmt.Errorf("replay initialize: %w", err)
			}
			cachedInitResp = append(cachedInitResp[:0], resp...)
			childInitialized = true
		}
		if cachedInitNote != nil {
			if err := send(cachedInitNote); err != nil {
				killChild()
				return fmt.Errorf("replay initialized notification: %w", err)
			}
			childInitNoteSent = true
		}
		return nil
	}

	// route hands one stdout line from the child to whoever is waiting for it.
	route := func(line []byte) {
		lastOutput = time.Now()
		head := parseHead(line)
		switch {
		case head.Method != "" && head.hasID():
			refuse(head.ID, head.Method)
		case head.Method == "" && head.hasID():
			id, err := strconv.ParseInt(string(head.ID), 10, 64)
			pr, ok := pending[id]
			if err != nil || !ok {
				// A late answer to a request that already timed out.
				return
			}
			delete(pending, id)
			lastUsedAt = time.Now()
			resp, err := withReplacedID(line, string(pr.clientID))
			if err != nil {
				pr.replyCh <- dispatchResp{err: err}
				return
			}
			if pr.isInit {
				cachedInitResp = append(cachedInitResp[:0], line...)
				childInitialized = true
			}
			pr.replyCh <- dispatchResp{data: resp}
		}
		// Otherwise a server notification (progress, logging) or a line that
		// is not JSON at all; neither has a caller to go to.
	}

	// expire answers requests that ran past mcpStdioReadTimeout and asks the
	// child to cancel them, but keeps the child: it may just be busy with one
	// slow call. Only a child that wrote nothing at all while a request waited
	// out the whole timeout is treated as hung and killed.
	expire := func() {
		hung := false
		for id, pr := range pending {
			if time.Since(pr.sentAt) < mcpStdioReadTimeout {
				continue
			}
			delete(pending, id)
			log.Warnf("mcp_stdio: %s: child response timeout after %s", name, mcpStdioReadTimeout)
			pr.replyCh <- dispatchResp{data: errorResponse(pr.clientID, mcpStdioTimeoutCode, fmt.Sprintf(
				"No response from the MCP server within %s. Do not repeat the same call; narrow it or try another approach.",
				mcpStdioReadTimeout))}
			_ = send(cancelledNotification(id))
			if !lastOutput.After(pr.sentAt) {
				hung = true
			}
		}
		if hung {
			log.Warnf("mcp_stdio: %s: no output for %s, killing child", name, mcpStdioReadTimeout)
			failPending(mcpStdioClosedCode, "The MCP server stopped responding and was restarted; this call may not have finished.")
			killChild()
		}
	}

	handle := func(req dispatchReq) {
		hasID, reqID, isInit, isInitNote := classifyJSONRPC(req.body)

		if err := ensureChild(); err != nil {
			req.replyCh <- dispatchResp{err: fmt.Errorf("spawn child: %w", err)}
			return
		}

		// A child supports only one "initialize" per process lifetime.
		// If it's already initialized, virtualize a repeat "initialize"
		// (and swallow a repeat "notifications/initialized") instead of
		// forwarding to the child, so HTTP clients can re-initialize
		// without disturbing a long-lived child.
		if isInit && childInitialized && cachedInitResp != nil {
			resp, err := withReplacedID(cachedInitResp, reqID)
			if err != nil {
				req.replyCh <- dispatchResp{err: fmt.Errorf("rewrite cached initialize response: %w", err)}
				return
			}
			req.replyCh <- dispatchResp{data: resp}
			return
		}
		if isInitNote && childInitNoteSent {
			req.replyCh <- dispatchResp{}
			return
		}

		if isInit {
			cachedInitReq = append(cachedInitReq[:0], req.body...)
		}
		if isInitNote {
			cachedInitNote = append(cachedInitNote[:0], req.body...)
		}

		frame := req.body
		var id int64
		if hasID {
			nextID++
			id = nextID
			f, err := withReplacedID(req.body, strconv.FormatInt(id, 10))
			if err != nil {
				req.replyCh <- dispatchResp{err: fmt.Errorf("rewrite request id: %w", err)}
				return
			}
			frame = f
		} else if f, ok := rewriteCancelled(req.body, func(clientID json.RawMessage) (int64, bool) {
			for bid, pr := range pending {
				if bytes.Equal(pr.clientID, clientID) {
					return bid, true
				}
			}
			return 0, false
		}); ok {
			if f == nil {
				// Cancels nothing we know of; forwarding it could cancel an
				// unrelated request that happens to carry the same bridge id.
				req.replyCh <- dispatchResp{}
				return
			}
			frame = f
		}

		if err := send(frame); err != nil {
			// The child died since the last request; start a fresh one and
			// try once more.
			failPending(mcpStdioClosedCode, "The MCP server exited before answering. A new one starts with the next call.")
			killChild()
			if err = ensureChild(); err == nil {
				err = send(frame)
			}
			if err != nil {
				req.replyCh <- dispatchResp{err: fmt.Errorf("write stdin: %w", err)}
				return
			}
		}
		lastUsedAt = time.Now()

		if !hasID {
			if isInitNote {
				childInitNoteSent = true
			}
			req.replyCh <- dispatchResp{}
			return
		}
		pending[id] = &pendingReq{
			clientID: json.RawMessage(reqID),
			replyCh:  req.replyCh,
			sentAt:   time.Now(),
			isInit:   isInit,
		}
	}

	// armDeadline points deadlineCh at the oldest in-flight request's deadline.
	armDeadline := func() {
		if deadline != nil {
			deadline.Stop()
		}
		deadline, deadlineCh = nil, nil
		var oldest time.Time
		for _, pr := range pending {
			if oldest.IsZero() || pr.sentAt.Before(oldest) {
				oldest = pr.sentAt
			}
		}
		if !oldest.IsZero() {
			deadline = time.NewTimer(time.Until(oldest.Add(mcpStdioReadTimeout)))
			deadlineCh = deadline.C
		}
	}

	var reaperCh <-chan time.Time
	if p.idleTTL > 0 {
		t := time.NewTicker(mcpStdioReapInterval)
		defer t.Stop()
		reaperCh = t.C
	}

	for {
		select {
		case <-p.closeCh:
			failPending(mcpStdioClosedCode, "The bridge is shutting down.")
			killChild()
			return

		case ack := <-p.killCh:
			failPending(mcpStdioClosedCode, "The MCP server was restarted; this call may not have finished.")
			killChild()
			close(ack)

		case <-reaperCh:
			if child != nil && len(pending) == 0 && time.Since(lastUsedAt) > p.idleTTL {
				log.Infof("mcp_stdio: reaping idle child %s", name)
				killChild()
			}

		case req := <-p.reqCh:
			handle(req)

		case line, ok := <-childLines:
			if !ok {
				log.Warnf("mcp_stdio: %s: child exited", name)
				failPending(mcpStdioClosedCode, "The MCP server exited before answering. A new one starts with the next call.")
				killChild()
				break
			}
			route(line)

		case <-deadlineCh:
			expire()
		}
		armDeadline()
	}
}

// readChildLines sends each non-empty stdout line of a child to lines and
// closes lines when stdout ends. done stops it early once the worker has
// dropped this child, so a send can never block forever.
func readChildLines(out io.Reader, lines chan<- []byte, done <-chan struct{}) {
	defer close(lines)
	br := bufio.NewReader(out)
	for {
		line, err := br.ReadBytes('\n')
		if line = bytes.TrimRight(line, "\r\n"); len(line) > 0 {
			select {
			case lines <- line:
			case <-done:
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// jsonRPCHead holds the fields the bridge routes on.
type jsonRPCHead struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
}

func (h jsonRPCHead) hasID() bool { return len(h.ID) > 0 && string(h.ID) != "null" }

// parseHead reads a frame's id and method; a non-JSON line yields neither.
func parseHead(line []byte) jsonRPCHead {
	var h jsonRPCHead
	_ = json.Unmarshal(line, &h)
	return h
}

// rewriteCancelled handles a client "notifications/cancelled". ok is false
// for any other frame. Otherwise frame is the notification with requestId
// mapped to the bridge id, or nil when lookup knows no such request.
func rewriteCancelled(body []byte, lookup func(clientID json.RawMessage) (int64, bool)) (frame []byte, ok bool) {
	var m struct {
		Method string                     `json:"method"`
		Params map[string]json.RawMessage `json:"params"`
	}
	if json.Unmarshal(body, &m) != nil || m.Method != "notifications/cancelled" {
		return nil, false
	}
	bid, found := lookup(m.Params["requestId"])
	if !found {
		return nil, true
	}
	m.Params["requestId"] = json.RawMessage(strconv.FormatInt(bid, 10))
	out, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": m.Method, "params": m.Params})
	if err != nil {
		return nil, true
	}
	return out, true
}

// classifyJSONRPC inspects a JSON-RPC frame's "method" and "id" fields.
// hasID is true when the frame has a non-null id (and therefore expects a
// response); reqID is its raw string value used to match the child's response;
// isInit/isInitNote flag the MCP handshake messages so they can be cached for
// replay.
func classifyJSONRPC(body []byte) (hasID bool, reqID string, isInit, isInitNote bool) {
	var head struct {
		Method string          `json:"method"`
		ID     json.RawMessage `json:"id"`
	}
	if err := json.Unmarshal(body, &head); err != nil {
		// Be conservative: assume the peer expects a response so we surface
		// any framing error back to the client instead of hanging.
		return true, "", false, false
	}
	hasID = len(head.ID) > 0 && string(head.ID) != "null"
	if hasID {
		reqID = string(head.ID)
	}
	isInit = head.Method == "initialize"
	isInitNote = head.Method == "notifications/initialized"
	return
}

// refuseServerRequest builds the JSON-RPC "method not found" error sent back
// to the child for a server-to-client request the bridge cannot deliver.
func refuseServerRequest(id json.RawMessage, method string) ([]byte, error) {
	return json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"error": map[string]any{
			"code":    -32601,
			"message": method + " is not supported by the mcp_stdio bridge",
		},
	})
}

// errorResponse builds a JSON-RPC error the bridge answers a client request
// with on the child's behalf. clientID is the request's raw id.
func errorResponse(clientID json.RawMessage, code int, msg string) []byte {
	if len(clientID) == 0 {
		clientID = json.RawMessage("null")
	}
	out, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      clientID,
		"error":   map[string]any{"code": code, "message": msg},
	})
	return out
}

// cancelledNotification asks the child to stop working on bridge request id.
func cancelledNotification(id int64) []byte {
	out, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"method":  "notifications/cancelled",
		"params":  map[string]any{"requestId": id, "reason": "timed out in the mcp_stdio bridge"},
	})
	return out
}

// withReplacedID returns a copy of a JSON-RPC response line with its "id"
// field replaced by the raw JSON value reqID, so a cached response can be
// replayed to a client under its own request id.
func withReplacedID(line []byte, reqID string) ([]byte, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(line, &m); err != nil {
		return nil, fmt.Errorf("unmarshal cached response: %w", err)
	}
	if reqID == "" {
		reqID = "null"
	}
	m["id"] = json.RawMessage(reqID)
	out, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("marshal cached response: %w", err)
	}
	return out, nil
}

func pipeStderrToLog(r io.Reader, name string) {
	br := bufio.NewReader(r)
	for {
		line, err := br.ReadString('\n')
		if line != "" {
			log.Infof("mcp_stdio[%s]: %s", name, bytes.TrimRight([]byte(line), "\r\n"))
		}
		if err != nil {
			return
		}
	}
}

// killChildNow asks the worker to kill the current child process and waits for
// it to complete. Useful in tests to simulate an idle reap without waiting for
// the TTL.
func (p *MCPStdioPlugin) killChildNow() {
	ack := make(chan struct{})
	select {
	case p.killCh <- ack:
		<-ack
	case <-p.closeCh:
	}
}

func (p *MCPStdioPlugin) Handle(_ context.Context, connInfo *ConnectionInfo) {
	wrapConn := netpkg.WrapReadWriteCloserToConn(connInfo.Conn, connInfo.UnderlyingConn)
	_ = p.l.PutConn(wrapConn)
}

func (p *MCPStdioPlugin) Name() string {
	return v1.PluginMCPStdio
}

func (p *MCPStdioPlugin) Close() error {
	_ = p.s.Close()
	_ = p.l.Close()
	close(p.closeCh)
	<-p.workerDone
	return nil
}
