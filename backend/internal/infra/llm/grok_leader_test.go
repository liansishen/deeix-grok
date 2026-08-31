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

func TestLoadGrokLeaderSessionCollectsVisibleReplayHistory(t *testing.T) {
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
		request := make(map[string]interface{})
		if err = json.Unmarshal([]byte(grokString(message, "payload")), &request); err != nil {
			serverErr <- err
			return
		}
		if got := grokString(request, "method"); got != "session/load" {
			serverErr <- fmt.Errorf("unexpected load method %q", got)
			return
		}

		sendACP := func(payload map[string]interface{}) error {
			raw, marshalErr := json.Marshal(payload)
			if marshalErr != nil {
				return marshalErr
			}
			return writeGrokLeaderMessage(server, map[string]interface{}{"type": "acp", "payload": string(raw)})
		}
		replayUpdate := func(isReplay bool, timestamp int64, kind string, text string, hidden bool) map[string]interface{} {
			meta := map[string]interface{}{"isReplay": isReplay}
			if timestamp > 0 {
				meta["agentTimestampMs"] = timestamp
			}
			update := map[string]interface{}{
				"sessionUpdate": kind,
				"content":       map[string]interface{}{"type": "text", "text": text},
			}
			if hidden {
				update["_meta"] = map[string]interface{}{"hideFromScrollback": true}
			}
			return map[string]interface{}{
				"jsonrpc": "2.0",
				"method":  "session/update",
				"params":  map[string]interface{}{"_meta": meta, "update": update},
			}
		}
		updates := []map[string]interface{}{
			replayUpdate(false, 0, "user_message_chunk", "ignored", false),
			replayUpdate(true, 100, "user_message_chunk", "Hello", false),
			replayUpdate(true, 110, "agent_thought_chunk", "Think ", false),
			replayUpdate(true, 111, "agent_thought_chunk", "carefully", false),
			replayUpdate(true, 120, "agent_message_chunk", "Answer ", false),
			replayUpdate(true, 121, "agent_message_chunk", "one.", false),
			replayUpdate(true, 0, "user_message_chunk", "hidden", true),
			replayUpdate(true, 0, "agent_message_chunk", "hidden answer", false),
			replayUpdate(true, 200, "user_message_chunk", "Again", false),
			replayUpdate(true, 210, "agent_message_chunk", "Done", false),
		}
		for _, update := range updates {
			if err = sendACP(update); err != nil {
				serverErr <- err
				return
			}
		}
		err = sendACP(map[string]interface{}{"jsonrpc": "2.0", "id": 3, "result": map[string]interface{}{}})
		serverErr <- err
	}()

	history, err := loadGrokLeaderSession(client, 3, "session-1", "/workspace")
	if err != nil {
		t.Fatalf("load session: %v", err)
	}
	if err = <-serverErr; err != nil {
		t.Fatalf("serve load response: %v", err)
	}
	if len(history) != 4 {
		t.Fatalf("expected four visible messages, got %d: %#v", len(history), history)
	}
	if history[0].Role != "user" || history[0].Content != "Hello" || history[0].CreatedAtUnixMS != 100 {
		t.Fatalf("unexpected first user message: %#v", history[0])
	}
	if history[1].Role != "assistant" || history[1].ReasoningContent != "Think carefully" || history[1].Content != "Answer one." || history[1].CreatedAtUnixMS != 110 {
		t.Fatalf("unexpected first assistant message: %#v", history[1])
	}
	if history[2].Role != "user" || history[2].Content != "Again" || history[3].Role != "assistant" || history[3].Content != "Done" {
		t.Fatalf("unexpected second turn: %#v", history[2:])
	}
}
