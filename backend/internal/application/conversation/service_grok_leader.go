package conversation

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/DEEIX-AI/DEEIX-Chat/backend/internal/application/channel"
	model "github.com/DEEIX-AI/DEEIX-Chat/backend/internal/domain/conversation"
	"github.com/DEEIX-AI/DEEIX-Chat/backend/internal/ports/llm"
)

type grokLeaderGateway interface {
	ListGrokLeaderSessions(ctx context.Context, route llm.RouteConfig) ([]llm.GrokLeaderSession, error)
	BindGrokLeaderSession(ctx context.Context, route llm.RouteConfig, conversationSessionKey string, sessionID string) (*llm.GrokLeaderSession, error)
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
	if conversation.MessageCount == 0 && len(boundSession.History) > 0 {
		conversation.MessageCount, err = s.importGrokLeaderHistory(ctx, conversation, boundSession.SessionID, boundSession.History)
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

func (s *Service) importGrokLeaderHistory(ctx context.Context, conversation *model.Conversation, sessionID string, history []llm.GrokLeaderHistoryMessage) (int, error) {
	if conversation == nil || conversation.MessageCount != 0 || len(history) == 0 {
		return 0, nil
	}
	existing, err := s.repo.ListAllMessages(ctx, conversation.ID)
	if err != nil {
		return 0, err
	}
	messageIDs := make(map[string]uint, len(existing))
	for _, message := range existing {
		messageIDs[message.PublicID] = message.ID
	}

	parentID := uint(0)
	messageCount := 0
	for index, item := range history {
		role := strings.ToLower(strings.TrimSpace(item.Role))
		content := strings.TrimSpace(item.Content)
		reasoning := strings.TrimSpace(item.ReasoningContent)
		if (role != "user" && role != "assistant") || (content == "" && reasoning == "") {
			continue
		}
		publicID := grokLeaderHistoryPublicID(conversation.PublicID, sessionID, index)
		if existingID := messageIDs[publicID]; existingID > 0 {
			parentID = existingID
			messageCount++
			continue
		}
		message := &model.Message{
			ConversationID:   conversation.ID,
			UserID:           conversation.UserID,
			PublicID:         publicID,
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
		messageIDs[publicID] = message.ID
		parentID = message.ID
		messageCount++
	}
	if messageCount > 0 {
		if err = s.repo.IncrementMessageCount(ctx, conversation.ID, messageCount); err != nil {
			return 0, err
		}
	}
	return messageCount, nil
}

func grokLeaderHistoryPublicID(conversationPublicID string, sessionID string, index int) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s:%s:%d", strings.TrimSpace(conversationPublicID), strings.TrimSpace(sessionID), index)))
	return fmt.Sprintf("%x", digest[:16])
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
