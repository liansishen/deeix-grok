package conversation

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/DEEIX-AI/DEEIX-Chat/backend/internal/application/channel"
	model "github.com/DEEIX-AI/DEEIX-Chat/backend/internal/domain/conversation"
	"github.com/DEEIX-AI/DEEIX-Chat/backend/internal/ports/llm"
	"github.com/DEEIX-AI/DEEIX-Chat/backend/internal/repository"
)

type grokLeaderGateway interface {
	ListGrokLeaderSessions(ctx context.Context, route llm.RouteConfig) ([]llm.GrokLeaderSession, error)
	BindGrokLeaderSession(ctx context.Context, route llm.RouteConfig, conversationSessionKey string, sessionID string) (*llm.GrokLeaderSession, error)
	ObserveGrokLeaderSession(ctx context.Context, route llm.RouteConfig, sessionID string, onHistory func([]llm.GrokLeaderHistoryMessage) error, onEvent func(llm.GrokLeaderEvent) error) error
}

const grokLeaderLiveFlushInterval = 200 * time.Millisecond

type grokLeaderObservedUpdate struct {
	history []llm.GrokLeaderHistoryMessage
	event   *llm.GrokLeaderEvent
}

// GrokLeaderSessionBinding 返回绑定后的 Deeix 会话和 Grok roster 摘要。
type GrokLeaderSessionBinding struct {
	Conversation *model.Conversation
	Session      llm.GrokLeaderSession
}

// ListGrokLeaderSessions 查询当前共享 Grok leader 可发现的会话。
func (s *Service) ListGrokLeaderSessions(ctx context.Context, userID uint, platformModelName string, requestID string) ([]llm.GrokLeaderSession, error) {
	route, err := s.resolveGrokLeaderRoute(ctx, userID, 0, platformModelName, requestID)
	if err != nil {
		return nil, err
	}
	client, ok := s.llmClient.(grokLeaderGateway)
	if !ok {
		return nil, ErrModelRouteNotConfigured
	}
	return client.ListGrokLeaderSessions(ctx, route)
}

// BindGrokLeaderSession 加载已有 Grok 会话并绑定到当前用户的 Deeix 会话。
func (s *Service) BindGrokLeaderSession(ctx context.Context, userID uint, conversationPublicID string, sessionID string, platformModelName string, requestID string) (*GrokLeaderSessionBinding, error) {
	conversation, err := s.GetConversationByPublicID(ctx, userID, strings.TrimSpace(conversationPublicID))
	if err != nil {
		return nil, err
	}
	route, err := s.resolveGrokLeaderRoute(ctx, userID, conversation.ID, platformModelName, requestID)
	if err != nil {
		return nil, err
	}
	client, ok := s.llmClient.(grokLeaderGateway)
	if !ok {
		return nil, ErrModelRouteNotConfigured
	}
	boundSession, err := client.BindGrokLeaderSession(ctx, route, conversation.SessionKey, strings.TrimSpace(sessionID))
	if err != nil {
		if errors.Is(err, llm.ErrGrokLeaderSessionNotFound) {
			return nil, ErrGrokLeaderSessionNotFound
		}
		return nil, err
	}

	modelName := strings.TrimSpace(platformModelName)
	provider := inferProvider(modelName)
	if conversation.Model != modelName || conversation.Provider != provider {
		if err = s.repo.UpdateConversationModel(ctx, conversation.ID, modelName, provider); err != nil {
			return nil, err
		}
		conversation.Model = modelName
		conversation.Provider = provider
	}
	if len(boundSession.History) > 0 {
		conversation.MessageCount, err = s.syncGrokLeaderHistory(ctx, conversation, boundSession.SessionID, boundSession.History)
		if err != nil {
			return nil, err
		}
	}
	if err = s.repo.UpdateConversationLastResponseID(ctx, conversation.ID, boundSession.SessionID); err != nil {
		return nil, err
	}
	conversation.LastResponseID = boundSession.SessionID
	return &GrokLeaderSessionBinding{Conversation: conversation, Session: *boundSession}, nil
}

// ObserveGrokLeaderConversation 对账完整 replay，并将 live 更新合并写入当前会话。
func (s *Service) ObserveGrokLeaderConversation(ctx context.Context, userID uint, conversationPublicID string, requestID string, onChanged func() error) error {
	conversation, err := s.GetConversationByPublicID(ctx, userID, strings.TrimSpace(conversationPublicID))
	if err != nil {
		return err
	}
	sessionID := strings.TrimSpace(conversation.LastResponseID)
	if sessionID == "" {
		return ErrGrokLeaderSessionNotFound
	}
	route, err := s.resolveGrokLeaderRoute(ctx, userID, conversation.ID, conversation.Model, requestID)
	if err != nil {
		return err
	}
	client, ok := s.llmClient.(grokLeaderGateway)
	if !ok {
		return ErrModelRouteNotConfigured
	}

	updates := make(chan grokLeaderObservedUpdate, 64)
	observerErr := make(chan error, 1)
	deliver := func(update grokLeaderObservedUpdate) error {
		select {
		case updates <- update:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	go func() {
		observerErr <- client.ObserveGrokLeaderSession(
			ctx,
			route,
			sessionID,
			func(history []llm.GrokLeaderHistoryMessage) error {
				return deliver(grokLeaderObservedUpdate{history: history})
			},
			func(event llm.GrokLeaderEvent) error {
				value := event
				return deliver(grokLeaderObservedUpdate{event: &value})
			},
		)
	}()

	ticker := time.NewTicker(grokLeaderLiveFlushInterval)
	defer ticker.Stop()
	var history []llm.GrokLeaderHistoryMessage
	pendingEvents := make([]llm.GrokLeaderEvent, 0)
	dirty := false
	flush := func() error {
		if !dirty || len(history) == 0 {
			return nil
		}
		last := len(history) - 1
		eventFrom := len(history[last].Events) - len(pendingEvents)
		count, syncErr := s.syncGrokLeaderHistoryFrom(ctx, conversation, sessionID, history, last, eventFrom)
		if syncErr != nil {
			return syncErr
		}
		conversation.MessageCount = count
		pendingEvents = pendingEvents[:0]
		dirty = false
		if onChanged != nil {
			return onChanged()
		}
		return nil
	}

	for {
		select {
		case update := <-updates:
			if update.event == nil {
				if err = flush(); err != nil {
					return err
				}
				history = update.history
				conversation.MessageCount, err = s.syncGrokLeaderHistory(ctx, conversation, sessionID, history)
				if err != nil {
					return err
				}
				if onChanged != nil {
					if err = onChanged(); err != nil {
						return err
					}
				}
				continue
			}
			if grokLeaderLiveStartsMessage(history, *update.event) && dirty {
				if err = flush(); err != nil {
					return err
				}
			}
			history = appendGrokLeaderLiveEvent(history, *update.event)
			pendingEvents = append(pendingEvents, *update.event)
			dirty = true
		case <-ticker.C:
			if err = flush(); err != nil {
				return err
			}
		case err = <-observerErr:
			if flushErr := flush(); flushErr != nil {
				return flushErr
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *Service) syncGrokLeaderHistory(ctx context.Context, conversation *model.Conversation, sessionID string, history []llm.GrokLeaderHistoryMessage) (int, error) {
	return s.syncGrokLeaderHistoryFrom(ctx, conversation, sessionID, history, 0, 0)
}

func (s *Service) syncGrokLeaderHistoryFrom(ctx context.Context, conversation *model.Conversation, sessionID string, history []llm.GrokLeaderHistoryMessage, traceFrom int, traceEventFrom int) (int, error) {
	if conversation == nil || len(history) == 0 {
		return 0, nil
	}
	existing, err := s.repo.ListAllMessages(ctx, conversation.ID)
	if err != nil {
		return 0, err
	}
	byPublicID := make(map[string]*model.Message, len(existing))
	for index := range existing {
		byPublicID[existing[index].PublicID] = &existing[index]
	}

	parentID := uint(0)
	createdCount := 0
	for index, item := range history {
		role := strings.ToLower(strings.TrimSpace(item.Role))
		content := strings.TrimSpace(item.Content)
		reasoning := strings.TrimSpace(item.ReasoningContent)
		if role != "user" && role != "assistant" {
			continue
		}
		if role == "user" && content == "" {
			continue
		}
		if role == "assistant" && content == "" && reasoning == "" && len(item.Events) == 0 {
			continue
		}

		publicID := grokLeaderStableHistoryPublicID(conversation.PublicID, sessionID, item.StableID, index)
		message := byPublicID[publicID]
		if message == nil {
			legacyID := grokLeaderHistoryPublicID(conversation.PublicID, sessionID, index)
			message = byPublicID[legacyID]
		}
		if message == nil {
			message = &model.Message{
				ConversationID:   conversation.ID,
				UserID:           conversation.UserID,
				PublicID:         publicID,
				RunID:            strings.TrimSpace(item.RunID),
				Role:             role,
				ContentType:      "text",
				Content:          content,
				ReasoningContent: reasoning,
				BranchReason:     "default",
				Status:           "success",
			}
			if role == "assistant" {
				message.ContentType = "markdown"
			}
			if parentID > 0 {
				value := parentID
				message.ParentMessageID = &value
			}
			if item.CreatedAtUnixMS > 0 {
				message.CreatedAt = time.UnixMilli(item.CreatedAtUnixMS).UTC()
				message.UpdatedAt = message.CreatedAt
			}
			if err = s.repo.CreateMessage(ctx, message); err != nil {
				return 0, err
			}
			byPublicID[publicID] = message
			createdCount++
		} else if message.Content != content || message.ReasoningContent != reasoning || message.Status != "success" {
			if err = s.repo.UpdateAssistantMessageCompletion(ctx, message.ID, repository.AssistantMessageCompletionUpdate{
				ContentType:      message.ContentType,
				Content:          content,
				ReasoningContent: reasoning,
				Status:           "success",
			}); err != nil {
				return 0, err
			}
			message.Content = content
			message.ReasoningContent = reasoning
			message.Status = "success"
		}
		parentID = message.ID
		if role == "assistant" && index >= traceFrom {
			eventFrom := 0
			if index == traceFrom {
				eventFrom = traceEventFrom
			}
			if err = s.syncGrokLeaderTrace(ctx, conversation, message, item, eventFrom); err != nil {
				return 0, err
			}
		}
	}
	if createdCount > 0 {
		if err = s.repo.IncrementMessageCount(ctx, conversation.ID, createdCount); err != nil {
			return 0, err
		}
	}
	return len(existing) + createdCount, nil
}

func grokLeaderLiveStartsMessage(history []llm.GrokLeaderHistoryMessage, event llm.GrokLeaderEvent) bool {
	if len(history) == 0 {
		return true
	}
	lastRole := history[len(history)-1].Role
	if event.Kind == "user_message_chunk" {
		return lastRole != "user"
	}
	return lastRole != "assistant"
}

func appendGrokLeaderLiveEvent(history []llm.GrokLeaderHistoryMessage, event llm.GrokLeaderEvent) []llm.GrokLeaderHistoryMessage {
	role := "assistant"
	if event.Kind == "user_message_chunk" {
		role = "user"
	}
	if len(history) == 0 || history[len(history)-1].Role != role {
		history = append(history, llm.GrokLeaderHistoryMessage{Role: role})
	}
	current := &history[len(history)-1]
	current.Events = append(current.Events, event)
	if current.CreatedAtUnixMS == 0 {
		current.CreatedAtUnixMS = event.CreatedAtUnixMS
	}
	if role == "user" {
		current.Content += event.Text
		if current.StableID == "" {
			current.StableID = event.EventID
		}
		return history
	}
	if event.PromptID != "" {
		current.StableID = event.PromptID
		current.RunID = event.PromptID
	} else if current.StableID == "" {
		current.StableID = event.EventID
		current.RunID = event.EventID
	}
	if event.Kind == "agent_message_chunk" {
		current.Content += event.Text
	} else if event.Kind == "agent_thought_chunk" {
		current.ReasoningContent += event.Text
	}
	return history
}

func (s *Service) syncGrokLeaderTrace(ctx context.Context, conversation *model.Conversation, assistant *model.Message, history llm.GrokLeaderHistoryMessage, eventFrom int) error {
	if conversation == nil || assistant == nil {
		return nil
	}
	runID := strings.TrimSpace(history.RunID)
	if runID == "" {
		runID = strings.TrimSpace(history.StableID)
	}
	if runID == "" {
		runID = assistant.PublicID
	}
	latestThoughtID := ""
	for seq, event := range history.Events {
		startedAt := assistant.CreatedAt
		if event.CreatedAtUnixMS > 0 {
			startedAt = time.UnixMilli(event.CreatedAtUnixMS).UTC()
		}
		switch event.Kind {
		case "agent_thought_chunk":
			content := normalizeGrokLeaderThought(event.Text)
			if content == "" {
				continue
			}
			eventID := strings.TrimSpace(event.EventID)
			if eventID == "" {
				eventID = grokLeaderTraceEventID(runID, event.Kind, seq)
			}
			latestThoughtID = eventID
			if seq < eventFrom {
				continue
			}
			payloadJSON := marshalGrokLeaderTracePayload(map[string]interface{}{
				"source": "grok_leader", "event_id": event.EventID, "prompt_id": event.PromptID, "chunk_id": event.ChunkID,
			})
			endedAt := startedAt
			if err := s.repo.UpsertConversationMessageTraceEvent(ctx, &model.MessageTraceEventRow{
				MessageID: assistant.ID, ConversationID: conversation.ID, UserID: conversation.UserID, RunID: runID,
				EventID: eventID, EventType: "think", Phase: messageTraceTypeUpstreamThink, Stage: messageTraceStageThink,
				RoundID: runID, Status: messageTraceStatusCompleted, Title: "模型思考", Summary: summarizeThinkText(content),
				ContentMarkdown: content, PayloadJSON: payloadJSON, Seq: seq + 1, StartedAt: startedAt, EndedAt: &endedAt,
			}); err != nil {
				return err
			}
		case "tool_call", "tool_call_update":
			if event.ToolCall == nil {
				continue
			}
			if seq < eventFrom {
				continue
			}
			call := event.ToolCall
			toolID := strings.TrimSpace(call.ToolCallID)
			if toolID == "" {
				toolID = strings.TrimSpace(event.EventID)
			}
			if toolID == "" {
				toolID = grokLeaderTraceEventID(runID, event.Kind, seq)
			}
			status := grokLeaderTraceStatus(call.Status)
			row := model.ToolCall{
				MessageID: assistant.ID, ConversationID: conversation.ID, UserID: conversation.UserID, RunID: runID,
				ToolCallID: toolID, ToolType: call.ToolType, ToolName: call.ToolName, Status: call.Status,
				InputJSON: call.ArgumentsJSON, OutputJSON: call.OutputJSON, ErrorJSON: call.ErrorJSON,
			}
			summary, markdown, payload := buildToolTrace([]model.ToolCall{row})
			payload["source"] = "grok_leader"
			payload["event_id"] = event.EventID
			var endedAt *time.Time
			if status != messageTraceStatusStreaming {
				value := startedAt
				endedAt = &value
			}
			if err := s.repo.UpsertConversationMessageTraceEvent(ctx, &model.MessageTraceEventRow{
				MessageID: assistant.ID, ConversationID: conversation.ID, UserID: conversation.UserID, RunID: runID,
				EventID: toolID, EventType: "tool", Phase: messageTraceTypeTools, Stage: messageTraceStageTool,
				RoundID: runID, ParentEventID: latestThoughtID, Status: status, Title: call.ToolName, Summary: summary,
				ContentMarkdown: markdown, PayloadJSON: marshalGrokLeaderTracePayload(payload), Seq: seq + 1, StartedAt: startedAt, EndedAt: endedAt,
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func grokLeaderStableHistoryPublicID(conversationPublicID string, sessionID string, stableID string, index int) string {
	stableID = strings.TrimSpace(stableID)
	if stableID == "" {
		return grokLeaderHistoryPublicID(conversationPublicID, sessionID, index)
	}
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s:%s:event:%s", strings.TrimSpace(conversationPublicID), strings.TrimSpace(sessionID), stableID)))
	return fmt.Sprintf("%x", digest[:16])
}

func grokLeaderHistoryPublicID(conversationPublicID string, sessionID string, index int) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s:%s:%d", strings.TrimSpace(conversationPublicID), strings.TrimSpace(sessionID), index)))
	return fmt.Sprintf("%x", digest[:16])
}

func grokLeaderTraceEventID(runID string, kind string, seq int) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s:%s:%d", strings.TrimSpace(runID), strings.TrimSpace(kind), seq)))
	return fmt.Sprintf("grok_%x", digest[:12])
}

func grokLeaderTraceStatus(status string) string {
	switch strings.TrimSpace(status) {
	case "completed", "success", "reused":
		return messageTraceStatusCompleted
	case "failed", "error":
		return messageTraceStatusError
	default:
		return messageTraceStatusStreaming
	}
}

func normalizeGrokLeaderThought(text string) string {
	return strings.TrimSpace(strings.ReplaceAll(text, "****", "\n\n"))
}

func marshalGrokLeaderTracePayload(payload map[string]interface{}) string {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "{}"
	}
	return string(encoded)
}

func (s *Service) resolveGrokLeaderRoute(ctx context.Context, userID uint, conversationID uint, platformModelName string, requestID string) (llm.RouteConfig, error) {
	if s.routeResolver == nil || s.llmClient == nil || strings.TrimSpace(platformModelName) == "" {
		return llm.RouteConfig{}, ErrModelRouteNotConfigured
	}
	route, err := s.routeResolver.ResolveRoute(ctx, channel.ResolveRouteInput{
		PlatformModelName: strings.TrimSpace(platformModelName),
		TaskType:          channel.TaskTypeChat,
		Scope:             channel.RouteScopeUser,
		UserID:            userID,
		ConversationID:    conversationID,
		RequestID:         strings.TrimSpace(requestID),
	})
	if err != nil {
		return llm.RouteConfig{}, mapRouteResolutionError(err)
	}
	if llm.NormalizeAdapter(route.Protocol) != llm.AdapterGrokLeader {
		return llm.RouteConfig{}, ErrGrokLeaderModelRequired
	}
	attributionReferer, attributionTitle := s.llmAttribution()
	return messageRouteConfig(route, attributionReferer, attributionTitle), nil
}
