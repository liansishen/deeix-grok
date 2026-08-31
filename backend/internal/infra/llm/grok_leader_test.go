package llm

import (
	"encoding/json"
	"fmt"
	"net"
	"testing"
)

func TestRequestGrokLeaderSessionsUsesWireExtensionAndUnwrapsResult(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	serverErr := make(chan error, 1)
	go func() {
		message, err := readGrokLeaderMessage(server)
		if err != nil {
			serverErr <- err
			return
		}
		if got := grokString(message, "type"); got != "acp" {
			serverErr <- fmt.Errorf("unexpected leader message type %q", got)
			return
		}
		request := make(map[string]interface{})
		if err = json.Unmarshal([]byte(grokString(message, "payload")), &request); err != nil {
			serverErr <- err
			return
		}
		if got := grokString(request, "method"); got != "_x.ai/sessions/list" {
			serverErr <- fmt.Errorf("unexpected roster method %q", got)
			return
		}
		if got := grokJSONID(request["id"]); got != 7 {
			serverErr <- fmt.Errorf("unexpected request id %d", got)
			return
		}

		payload, err := json.Marshal(map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      7,
			"result": map[string]interface{}{
				"result": map[string]interface{}{
					"sessions": []map[string]interface{}{{
						"sessionId": "session-1",
						"title": "Shared session",
						"cwd": "/workspace",
						"modelId": "gpt-sol",
						"reasoningEffort": "high",
						"activity": "working",
						"lastTurnSummary": "Inspecting the roster",
						"resident": true,
						"lastChangeUnixMs": int64(1234),
						"origin": map[string]interface{}{"kind": "local", "host": "host-1"},
					}},
				},
			},
		})
		if err == nil {
			err = writeGrokLeaderMessage(server, map[string]interface{}{"type": "acp", "payload": string(payload)})
		}
		serverErr <- err
	}()

	sessions, err := requestGrokLeaderSessions(client, 7)
	if err != nil {
		t.Fatalf("request sessions: %v", err)
	}
	if err = <-serverErr; err != nil {
		t.Fatalf("serve sessions response: %v", err)
	}
	if len(sessions) != 1 {
		t.Fatalf("expected one session, got %d", len(sessions))
	}
	got := sessions[0]
	if got.SessionID != "session-1" || got.Title != "Shared session" || got.CWD != "/workspace" {
		t.Fatalf("unexpected session identity: %#v", got)
	}
	if got.Activity != "working" || !got.Resident || got.LastChangeUnixMS != 1234 {
		t.Fatalf("unexpected session activity: %#v", got)
	}
	if got.Origin != "local" || got.OriginHost != "host-1" {
		t.Fatalf("unexpected session origin: %#v", got)
	}
}
