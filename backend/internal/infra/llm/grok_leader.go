package llm

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	grokLeaderDefaultSocket = ".grok/leader.sock"
	grokLeaderClientType    = "deeix-chat"
	grokLeaderMaxFrameSize  = 64 * 1024 * 1024
)

// grokLeaderSession owns one ACP connection and serializes prompts for one Deeix conversation.
type grokLeaderSession struct {
	mu        sync.Mutex
	conn      net.Conn
	sessionID string
	nextID    int
}

// grokLeaderAdapter forwards Deeix turns through the Grok Build leader ACP socket.
type grokLeaderAdapter struct {
	mu       sync.Mutex
	sessions map[string]*grokLeaderSession
}

func (a *grokLeaderAdapter) Name() string { return AdapterGrokLeader }

func (a *grokLeaderAdapter) Generate(ctx context.Context, route RouteConfig, input GenerateInput) (*GenerateOutput, error) {
	return a.run(ctx, route, input, nil)
}

func (a *grokLeaderAdapter) GenerateStream(ctx context.Context, route RouteConfig, input GenerateInput, onEvent func(GenerateStreamEvent) error) (*GenerateOutput, error) {
	return a.run(ctx, route, input, onEvent)
}

func (a *grokLeaderAdapter) ListModels(ctx context.Context, route RouteConfig) ([]ModelItem, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	model := strings.TrimSpace(route.UpstreamModel)
	if model == "" {
		return nil, errors.New("grok leader model is empty")
	}
	return []ModelItem{{ID: model, OwnedBy: "grok"}}, nil
}

type grokLeaderRosterResponse struct {
	Sessions []grokLeaderRosterEntry `json:"sessions"`
}

type grokLeaderRosterEntry struct {
	SessionID        string                 `json:"sessionId"`
	Title            string                 `json:"title"`
	CWD              string                 `json:"cwd"`
	IsWorktree       bool                   `json:"isWorktree"`
	ModelID          string                 `json:"modelId"`
	ReasoningEffort  string                 `json:"reasoningEffort"`
	Yolo             bool                   `json:"yolo"`
	Activity         string                 `json:"activity"`
	LastTurnSummary  string                 `json:"lastTurnSummary"`
	Resident         bool                   `json:"resident"`
	LastChangeUnixMS int64                  `json:"lastChangeUnixMs"`
	Origin           grokLeaderRosterOrigin `json:"origin"`
}

type grokLeaderRosterOrigin struct {
	Kind string `json:"kind"`
	Host string `json:"host"`
}

func (a *grokLeaderAdapter) listSessions(ctx context.Context, route RouteConfig) ([]GrokLeaderSession, error) {
	requestCtx, cancel := grokLeaderRequestContext(ctx, route)
	defer cancel()
	conn, err := openGrokLeaderConnection(requestCtx, route)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	stopWatching := watchGrokLeaderContext(requestCtx, conn)
	defer stopWatching()
	return requestGrokLeaderSessions(conn, 2)
}

func (a *grokLeaderAdapter) bindSession(ctx context.Context, route RouteConfig, conversationSessionKey string, sessionID string) (*GrokLeaderSession, error) {
	key := strings.TrimSpace(conversationSessionKey)
	targetID := strings.TrimSpace(sessionID)
	if key == "" || targetID == "" {
		return nil, ErrGrokLeaderSessionNotFound
	}
	requestCtx, cancel := grokLeaderRequestContext(ctx, route)
	defer cancel()
	session := a.sessionFor(key)
	session.mu.Lock()
	defer session.mu.Unlock()

	conn, err := openGrokLeaderConnection(requestCtx, route)
	if err != nil {
		return nil, err
	}
	stopWatching := watchGrokLeaderContext(requestCtx, conn)
	defer stopWatching()
	sessions, err := requestGrokLeaderSessions(conn, 2)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	entry, ok := findGrokLeaderSession(sessions, targetID)
	if !ok {
		_ = conn.Close()
		return nil, fmt.Errorf("%w: %s", ErrGrokLeaderSessionNotFound, targetID)
	}
	history, err := loadGrokLeaderSession(conn, 3, entry.SessionID, entry.CWD)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	entry.History = history
	if session.conn != nil {
		_ = session.conn.Close()
	}
	session.conn = conn
	session.sessionID = entry.SessionID
	session.nextID = 4
	return &entry, nil
}

func grokLeaderRequestContext(ctx context.Context, route RouteConfig) (context.Context, context.CancelFunc) {
	if timeout := resolveReadTimeout(route.ReadTimeoutMS); timeout > 0 {
		return context.WithTimeout(ctx, timeout)
	}
	return ctx, func() {}
}

func watchGrokLeaderContext(ctx context.Context, conn net.Conn) func() {
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()
	return func() {
		close(done)
		<-stopped
	}
}

func requestGrokLeaderSessions(conn net.Conn, requestID int) ([]GrokLeaderSession, error) {
	if err := sendGrokACPRequest(conn, requestID, "_x.ai/sessions/list", map[string]interface{}{}); err != nil {
		return nil, err
	}
	response, err := readGrokACPResponse(conn, requestID)
	if err != nil {
		return nil, fmt.Errorf("list Grok leader sessions: %w", err)
	}
	result, ok := grokMapAt(response, "result", "result")
	if !ok {
		return nil, errors.New("Grok leader sessions response did not contain a result")
	}
	payload, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("encode Grok leader sessions result: %w", err)
	}
	var roster grokLeaderRosterResponse
	if err = json.Unmarshal(payload, &roster); err != nil {
		return nil, fmt.Errorf("decode Grok leader sessions result: %w", err)
	}
	sessions := make([]GrokLeaderSession, 0, len(roster.Sessions))
	for _, entry := range roster.Sessions {
		entry.SessionID = strings.TrimSpace(entry.SessionID)
		entry.CWD = strings.TrimSpace(entry.CWD)
		entry.Activity = strings.TrimSpace(entry.Activity)
		if entry.SessionID == "" || entry.CWD == "" || entry.Activity == "" {
			return nil, errors.New("Grok leader sessions response contained an invalid session")
		}
		sessions = append(sessions, GrokLeaderSession{
			SessionID: entry.SessionID, Title: strings.TrimSpace(entry.Title), CWD: entry.CWD,
			IsWorktree: entry.IsWorktree, ModelID: strings.TrimSpace(entry.ModelID), ReasoningEffort: strings.TrimSpace(entry.ReasoningEffort),
			Yolo: entry.Yolo, Activity: entry.Activity, LastTurnSummary: strings.TrimSpace(entry.LastTurnSummary), Resident: entry.Resident,
			LastChangeUnixMS: entry.LastChangeUnixMS, Origin: strings.TrimSpace(entry.Origin.Kind), OriginHost: strings.TrimSpace(entry.Origin.Host),
		})
	}
	return sessions, nil
}

func findGrokLeaderSession(sessions []GrokLeaderSession, sessionID string) (GrokLeaderSession, bool) {
	for _, session := range sessions {
		if session.SessionID == sessionID {
			return session, true
		}
	}
	return GrokLeaderSession{}, false
}

func loadGrokLeaderSession(conn net.Conn, requestID int, sessionID string, cwd string) ([]GrokLeaderHistoryMessage, error) {
	if err := sendGrokACPRequest(conn, requestID, "session/load", map[string]interface{}{
		"sessionId": sessionID, "cwd": cwd, "mcpServers": []interface{}{},
	}); err != nil {
		return nil, err
	}
	history := make([]GrokLeaderHistoryMessage, 0)
	skipTurn := false
	for {
		message, err := readGrokLeaderMessage(conn)
		if err != nil {
			return nil, fmt.Errorf("load Grok leader session %q: %w", sessionID, err)
		}
		if grokString(message, "type") == "error" {
			return nil, errors.New(grokMessageError(message))
		}
		if grokString(message, "type") != "acp" {
			continue
		}
		payload := make(map[string]interface{})
		if err = json.Unmarshal([]byte(grokString(message, "payload")), &payload); err != nil {
			return nil, fmt.Errorf("decode Grok ACP session/load message: %w", err)
		}
		if payload["id"] != nil && grokJSONID(payload["id"]) == requestID {
			if payload["error"] != nil {
				return nil, fmt.Errorf("load Grok leader session %q: Grok ACP %s", sessionID, grokACPError(payload["error"]))
			}
			return history, nil
		}
		if grokString(payload, "method") != "session/update" {
			continue
		}
		params, ok := grokMapAt(payload, "params")
		if !ok {
			continue
		}
		meta, ok := grokMapAt(params, "_meta")
		if !ok {
			continue
		}
		isReplay, _ := meta["isReplay"].(bool)
		if !isReplay {
			continue
		}
		update, ok := grokMapAt(params, "update")
		if !ok {
			continue
		}
		if grokFirstString(update, "sessionUpdate", "session_update") == "user_message_chunk" {
			updateMeta, _ := grokMapAt(update, "_meta")
			skipTurn, _ = updateMeta["hideFromScrollback"].(bool)
		}
		if skipTurn {
			continue
		}
		history = appendGrokLeaderHistoryUpdate(history, update, grokInt64(meta, "agentTimestampMs"))
	}
}

func appendGrokLeaderHistoryUpdate(history []GrokLeaderHistoryMessage, update map[string]interface{}, createdAtUnixMS int64) []GrokLeaderHistoryMessage {
	kind := grokFirstString(update, "sessionUpdate", "session_update")
	text := grokReplayTextAt(update, "content")
	if text == "" {
		return history
	}
	switch kind {
	case "user_message_chunk":
		if len(history) > 0 && history[len(history)-1].Role == "user" {
			history[len(history)-1].Content = appendGrokLeaderHistoryText(history[len(history)-1].Content, text)
			return history
		}
		return append(history, GrokLeaderHistoryMessage{Role: "user", Content: text, CreatedAtUnixMS: createdAtUnixMS})
	case "agent_message_chunk", "agent_thought_chunk":
		if len(history) == 0 || history[len(history)-1].Role != "assistant" {
			history = append(history, GrokLeaderHistoryMessage{Role: "assistant", CreatedAtUnixMS: createdAtUnixMS})
		}
		current := &history[len(history)-1]
		if kind == "agent_thought_chunk" {
			current.ReasoningContent = appendGrokLeaderHistoryText(current.ReasoningContent, text)
		} else {
			current.Content = appendGrokLeaderHistoryText(current.Content, text)
		}
	}
	return history
}

func appendGrokLeaderHistoryText(current string, next string) string {
	return current + next
}

func grokReplayTextAt(value map[string]interface{}, key string) string {
	if text, ok := value[key].(string); ok {
		return text
	}
	content, ok := value[key].(map[string]interface{})
	if !ok {
		return ""
	}
	text, _ := content["text"].(string)
	return text
}

func (a *grokLeaderAdapter) run(ctx context.Context, route RouteConfig, input GenerateInput, onEvent func(GenerateStreamEvent) error) (*GenerateOutput, error) {
	requestCtx := ctx
	var cancel context.CancelFunc = func() {}
	if timeout := resolveReadTimeout(route.ReadTimeoutMS); timeout > 0 {
		requestCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()

	key := strings.TrimSpace(input.ConversationSessionKey)
	if key == "" {
		key = strings.TrimSpace(input.ConversationPublicID)
	}
	var session *grokLeaderSession
	if key != "" {
		session = a.sessionFor(key)
		session.mu.Lock()
		defer session.mu.Unlock()
	}

	conn, sessionID, requestID, err := a.acquireConnection(requestCtx, route, input, session)
	if err != nil {
		return nil, err
	}
	stopWatching := watchGrokLeaderContext(requestCtx, conn)
	defer stopWatching()

	output := &GenerateOutput{ResponseID: sessionID}
	if err := sendGrokACPRequest(conn, requestID, "session/prompt", map[string]interface{}{
		"sessionId": sessionID,
		"prompt": []map[string]string{{
			"type": "text",
			"text": grokPromptText(input),
		}},
		"_meta": map[string]interface{}{"screenMode": "headless"},
	}); err != nil {
		a.dropConnection(session, conn)
		return nil, err
	}
	if err := consumeGrokPrompt(conn, requestID, output, onEvent); err != nil {
		a.dropConnection(session, conn)
		return nil, err
	}

	if session != nil {
		session.conn = conn
		session.sessionID = sessionID
		session.nextID = requestID + 1
	}
	return output, nil
}

func (a *grokLeaderAdapter) acquireConnection(ctx context.Context, route RouteConfig, input GenerateInput, session *grokLeaderSession) (net.Conn, string, int, error) {
	if session != nil && session.conn != nil {
		return session.conn, session.sessionID, session.nextID, nil
	}
	conn, err := openGrokLeaderConnection(ctx, route)
	if err != nil {
		return nil, "", 0, err
	}
	persistedID := strings.TrimSpace(input.PreviousResponseID)
	if persistedID == "" && session != nil {
		persistedID = strings.TrimSpace(session.sessionID)
	}
	if persistedID != "" {
		sessions, listErr := requestGrokLeaderSessions(conn, 2)
		if listErr != nil {
			_ = conn.Close()
			return nil, "", 0, listErr
		}
		entry, ok := findGrokLeaderSession(sessions, persistedID)
		if !ok {
			_ = conn.Close()
			return nil, "", 0, fmt.Errorf("%w: %s", ErrGrokLeaderSessionNotFound, persistedID)
		}
		if _, err = loadGrokLeaderSession(conn, 3, entry.SessionID, entry.CWD); err != nil {
			_ = conn.Close()
			return nil, "", 0, err
		}
		return conn, entry.SessionID, 4, nil
	}
	sessionID, err := createGrokSession(conn, route)
	if err != nil {
		_ = conn.Close()
		return nil, "", 0, err
	}
	return conn, sessionID, 3, nil
}

func (a *grokLeaderAdapter) sessionFor(key string) *grokLeaderSession {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.sessions == nil {
		a.sessions = make(map[string]*grokLeaderSession)
	}
	if session := a.sessions[key]; session != nil {
		return session
	}
	session := &grokLeaderSession{nextID: 3}
	a.sessions[key] = session
	return session
}

func (a *grokLeaderAdapter) dropConnection(session *grokLeaderSession, conn net.Conn) {
	_ = conn.Close()
	if session != nil && session.conn == conn {
		session.conn = nil
	}
}

func openGrokLeaderConnection(ctx context.Context, route RouteConfig) (net.Conn, error) {
	socketPath, err := grokLeaderSocketPath()
	if err != nil {
		return nil, err
	}
	dialer := net.Dialer{Timeout: resolveConnectTimeout(route.ConnectTimeoutMS)}
	conn, err := dialer.DialContext(ctx, "unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("connect to Grok leader %q: %w", socketPath, err)
	}
	stopWatching := watchGrokLeaderContext(ctx, conn)
	defer stopWatching()

	if err := writeGrokLeaderMessage(conn, map[string]interface{}{
		"type": "register", "client_type": grokLeaderClientType, "mode": "stdio",
		"capabilities": map[string]interface{}{"auto_mode": true, "terminal": false, "fs_read": false, "fs_write": false},
	}); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("register with Grok leader: %w", err)
	}
	if err := waitForGrokLeaderReady(conn); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := sendGrokACPRequest(conn, 1, "initialize", map[string]interface{}{
		"protocolVersion":    1,
		"clientInfo":         map[string]interface{}{"name": grokLeaderClientType, "version": "1"},
		"clientCapabilities": map[string]interface{}{},
	}); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if _, err := readGrokACPResponse(conn, 1); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("initialize Grok ACP session: %w", err)
	}
	return conn, nil
}

func createGrokSession(conn net.Conn, route RouteConfig) (string, error) {
	cwd, err := grokLeaderWorkingDirectory()
	if err != nil {
		return "", err
	}
	params := map[string]interface{}{
		"cwd": cwd, "mcpServers": []interface{}{}, "_meta": map[string]interface{}{"sessionKind": "headless"},
	}
	if model := strings.TrimSpace(route.UpstreamModel); model != "" {
		params["modelId"] = model
	}
	if err := sendGrokACPRequest(conn, 2, "session/new", params); err != nil {
		return "", err
	}
	response, err := readGrokACPResponse(conn, 2)
	if err != nil {
		return "", fmt.Errorf("create Grok ACP session: %w", err)
	}
	sessionID := grokStringAt(response, "result", "sessionId")
	if sessionID == "" {
		sessionID = grokStringAt(response, "result", "session_id")
	}
	if sessionID == "" {
		return "", errors.New("Grok ACP session/new response did not contain a session id")
	}
	return sessionID, nil
}

func (a *grokLeaderAdapter) Close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, session := range a.sessions {
		session.mu.Lock()
		if session.conn != nil {
			_ = session.conn.Close()
			session.conn = nil
		}
		session.mu.Unlock()
	}
	a.sessions = nil
}

func grokLeaderSocketPath() (string, error) {
	if value := strings.TrimSpace(os.Getenv("GROK_LEADER_SOCKET")); value != "" {
		return value, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve Grok leader home directory: %w", err)
	}
	return filepath.Join(home, grokLeaderDefaultSocket), nil
}

func grokLeaderWorkingDirectory() (string, error) {
	if value := strings.TrimSpace(os.Getenv("DEEIX_GROK_CWD")); value != "" {
		return value, nil
	}
	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("resolve Grok working directory: %w", err)
	}
	return cwd, nil
}

func writeGrokLeaderMessage(conn net.Conn, message map[string]interface{}) error {
	payload, err := json.Marshal(message)
	if err != nil {
		return err
	}
	if len(payload) > grokLeaderMaxFrameSize {
		return fmt.Errorf("Grok leader message exceeds %d bytes", grokLeaderMaxFrameSize)
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	if err := writeGrokLeaderBytes(conn, header[:]); err != nil {
		return err
	}
	return writeGrokLeaderBytes(conn, payload)
}

func writeGrokLeaderBytes(conn net.Conn, payload []byte) error {
	for len(payload) > 0 {
		written, err := conn.Write(payload)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		payload = payload[written:]
	}
	return nil
}

func readGrokLeaderMessage(conn net.Conn) (map[string]interface{}, error) {
	var header [4]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(header[:])
	if length > grokLeaderMaxFrameSize {
		return nil, fmt.Errorf("Grok leader message exceeds %d bytes", grokLeaderMaxFrameSize)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return nil, err
	}
	message := make(map[string]interface{})
	if err := json.Unmarshal(payload, &message); err != nil {
		return nil, fmt.Errorf("decode Grok leader message: %w", err)
	}
	return message, nil
}

func waitForGrokLeaderReady(conn net.Conn) error {
	for {
		message, err := readGrokLeaderMessage(conn)
		if err != nil {
			return fmt.Errorf("wait for Grok leader registration: %w", err)
		}
		switch grokString(message, "type") {
		case "registered":
			if ready, ok := message["ready"].(bool); !ok || ready {
				return nil
			}
		case "leader_ready":
			return nil
		case "error":
			return fmt.Errorf("Grok leader registration failed: %s", grokMessageError(message))
		}
	}
}

func sendGrokACPRequest(conn net.Conn, id int, method string, params map[string]interface{}) error {
	payload, err := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      id,
		"method":  method,
		"params":  params,
	})
	if err != nil {
		return err
	}
	return writeGrokLeaderMessage(conn, map[string]interface{}{
		"type":    "acp",
		"payload": string(payload),
	})
}

func readGrokACPResponse(conn net.Conn, id int) (map[string]interface{}, error) {
	for {
		message, err := readGrokLeaderMessage(conn)
		if err != nil {
			return nil, err
		}
		if grokString(message, "type") == "error" {
			return nil, errors.New(grokMessageError(message))
		}
		if grokString(message, "type") != "acp" {
			continue
		}
		payload := make(map[string]interface{})
		if err := json.Unmarshal([]byte(grokString(message, "payload")), &payload); err != nil {
			return nil, fmt.Errorf("decode Grok ACP response: %w", err)
		}
		if grokJSONID(payload["id"]) != id {
			continue
		}
		if payload["error"] != nil {
			return nil, fmt.Errorf("Grok ACP %s", grokACPError(payload["error"]))
		}
		return payload, nil
	}
}

func consumeGrokPrompt(conn net.Conn, id int, output *GenerateOutput, onEvent func(GenerateStreamEvent) error) error {
	for {
		message, err := readGrokLeaderMessage(conn)
		if err != nil {
			return fmt.Errorf("read Grok ACP prompt: %w", err)
		}
		if grokString(message, "type") == "error" {
			return errors.New(grokMessageError(message))
		}
		if grokString(message, "type") != "acp" {
			continue
		}
		payload := make(map[string]interface{})
		if err := json.Unmarshal([]byte(grokString(message, "payload")), &payload); err != nil {
			return fmt.Errorf("decode Grok ACP prompt message: %w", err)
		}
		if payload["id"] != nil && grokJSONID(payload["id"]) == id {
			if payload["error"] != nil {
				return fmt.Errorf("Grok ACP prompt: %s", grokACPError(payload["error"]))
			}
			if usage := grokUsage(payload, "result", "usage"); usage != (Usage{}) {
				output.Usage = usage
			}
			if output.Text == "" {
				output.Text = grokStringAt(payload, "result", "text")
				if output.Text != "" && onEvent != nil {
					if err := onEvent(GenerateStreamEvent{Delta: output.Text, ResponseID: output.ResponseID}); err != nil {
						return err
					}
				}
			}
			return nil
		}
		if grokString(payload, "method") == "x.ai/session/prompt_complete" {
			if usage := grokUsage(payload, "params", "usage"); usage != (Usage{}) {
				output.Usage = usage
			}
			continue
		}
		if grokString(payload, "method") != "session/update" {
			continue
		}
		update, ok := grokMapAt(payload, "params", "update")
		if !ok {
			continue
		}
		if err := emitGrokUpdate(update, output, onEvent); err != nil {
			return err
		}
	}
}

func emitGrokUpdate(update map[string]interface{}, output *GenerateOutput, onEvent func(GenerateStreamEvent) error) error {
	kind := grokString(update, "sessionUpdate")
	if kind == "" {
		kind = grokString(update, "session_update")
	}
	event := GenerateStreamEvent{ResponseID: output.ResponseID}
	switch kind {
	case "agent_message_chunk":
		text := grokTextAt(update, "content")
		output.Text += text
		event.Delta = text
	case "agent_thought_chunk":
		text := grokTextAt(update, "content")
		event.Reasoning = &ReasoningDelta{Kind: "text", Text: text}
		if output.Reasoning == nil {
			output.Reasoning = &ReasoningOutput{}
		}
		output.Reasoning.Text += text
	case "tool_call", "tool_call_update":
		call := grokToolCall(update)
		if call.ToolCallID == "" && call.ToolName == "" {
			return nil
		}
		output.ServerToolCalls = append(output.ServerToolCalls, call)
		event.ServerToolCall = &call
	default:
		return nil
	}
	if onEvent == nil {
		return nil
	}
	if event.Delta == "" && event.Reasoning == nil && event.ServerToolCall == nil {
		return nil
	}
	return onEvent(event)
}

func grokPromptText(input GenerateInput) string {
	var builder strings.Builder
	if instructions := strings.TrimSpace(input.Instructions); instructions != "" {
		builder.WriteString("System instructions:\n")
		builder.WriteString(instructions)
		builder.WriteString("\n\n")
	}
	for _, message := range input.Messages {
		role := strings.TrimSpace(message.Role)
		if role == "" {
			role = "user"
		}
		builder.WriteString(role)
		builder.WriteString(":\n")
		if len(message.Parts) == 0 {
			builder.WriteString(message.Content)
		} else {
			for _, part := range message.Parts {
				if part.Kind == ContentPartText || part.Kind == ContentPartFile {
					builder.WriteString(part.Text)
				}
			}
		}
		for _, call := range message.ToolCalls {
			builder.WriteString("\n[tool call ")
			builder.WriteString(call.ToolName)
			builder.WriteString("] ")
			builder.WriteString(call.ArgumentsJSON)
		}
		for _, result := range message.ToolResults {
			builder.WriteString("\n[tool result] ")
			builder.WriteString(result.OutputJSON)
		}
		builder.WriteString("\n\n")
	}
	return strings.TrimSpace(builder.String())
}

func grokToolCall(update map[string]interface{}) ToolCall {
	call, _ := grokMapAt(update, "toolCall")
	if len(call) == 0 {
		call, _ = grokMapAt(update, "tool_call")
	}
	return ToolCall{
		ToolCallID:    grokFirstString(call, "toolCallId", "tool_call_id", "id"),
		ToolType:      "function",
		ToolName:      grokFirstString(call, "title", "name", "kind"),
		ArgumentsJSON: grokJSONField(call, "rawInput", "raw_input", "arguments", "input"),
		Status:        grokFirstString(call, "status"),
		OutputJSON:    grokJSONField(call, "rawOutput", "raw_output", "output"),
	}
}

func grokUsage(payload map[string]interface{}, path ...string) Usage {
	usage, ok := grokMapAt(payload, path...)
	if !ok {
		return Usage{}
	}
	return Usage{
		InputTokens:     grokInt64(usage, "inputTokens", "input_tokens"),
		OutputTokens:    grokInt64(usage, "outputTokens", "output_tokens"),
		ReasoningTokens: grokInt64(usage, "reasoningTokens", "reasoning_tokens"),
		CacheReadTokens: grokInt64(usage, "cacheReadTokens", "cache_read_tokens"),
	}
}

func grokMapAt(value map[string]interface{}, path ...string) (map[string]interface{}, bool) {
	current := value
	for _, key := range path {
		next, ok := current[key].(map[string]interface{})
		if !ok {
			return nil, false
		}
		current = next
	}
	return current, true
}

func grokString(value map[string]interface{}, key string) string {
	text, _ := value[key].(string)
	return strings.TrimSpace(text)
}

func grokFirstString(value map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if text := grokString(value, key); text != "" {
			return text
		}
	}
	return ""
}

func grokTextAt(value map[string]interface{}, key string) string {
	if text := grokString(value, key); text != "" {
		return text
	}
	content, ok := value[key].(map[string]interface{})
	if !ok {
		return ""
	}
	return grokString(content, "text")
}

func grokJSONField(value map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		raw, ok := value[key]
		if !ok || raw == nil {
			continue
		}
		if text, ok := raw.(string); ok {
			return text
		}
		encoded, err := json.Marshal(raw)
		if err == nil {
			return string(encoded)
		}
	}
	return ""
}

func grokInt64(value map[string]interface{}, keys ...string) int64 {
	for _, key := range keys {
		switch number := value[key].(type) {
		case float64:
			return int64(number)
		case int64:
			return number
		case json.Number:
			parsed, err := number.Int64()
			if err == nil {
				return parsed
			}
		}
	}
	return 0
}

func grokJSONID(value interface{}) int {
	switch number := value.(type) {
	case float64:
		return int(number)
	case int:
		return number
	case json.Number:
		parsed, _ := number.Int64()
		return int(parsed)
	default:
		return 0
	}
}

func grokStringAt(value map[string]interface{}, path ...string) string {
	if len(path) == 0 {
		return ""
	}
	parent, ok := grokMapAt(value, path[:len(path)-1]...)
	if !ok {
		return ""
	}
	return grokString(parent, path[len(path)-1])
}

func grokMessageError(message map[string]interface{}) string {
	if text := grokString(message, "message"); text != "" {
		return text
	}
	return "unknown leader error"
}

func grokACPError(value interface{}) string {
	if payload, ok := value.(map[string]interface{}); ok {
		if message := grokString(payload, "message"); message != "" {
			return message
		}
		encoded, _ := json.Marshal(payload)
		return string(encoded)
	}
	if text, ok := value.(string); ok && strings.TrimSpace(text) != "" {
		return text
	}
	return "unknown ACP error"
}
