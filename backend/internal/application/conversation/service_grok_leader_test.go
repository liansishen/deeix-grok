package conversation

import (
	"context"
	"testing"
	"time"

	model "github.com/DEEIX-AI/DEEIX-Chat/backend/internal/domain/conversation"
	"github.com/DEEIX-AI/DEEIX-Chat/backend/internal/ports/llm"
	"github.com/DEEIX-AI/DEEIX-Chat/backend/internal/repository"
)

type grokLeaderSyncRepository struct {
	repository.ConversationRepository
	messages   []model.Message
	created    []model.Message
	updates    []repository.AssistantMessageCompletionUpdate
	countDelta int
	traceRows  []model.MessageTraceEventRow
}

func (r *grokLeaderSyncRepository) ListAllMessages(context.Context, uint) ([]model.Message, error) {
	return append([]model.Message(nil), r.messages...), nil
}

func (r *grokLeaderSyncRepository) CreateMessage(_ context.Context, item *model.Message) error {
	item.ID = uint(100 + len(r.created))
	r.created = append(r.created, *item)
	r.messages = append(r.messages, *item)
	return nil
}

func (r *grokLeaderSyncRepository) UpdateAssistantMessageCompletion(_ context.Context, messageID uint, update repository.AssistantMessageCompletionUpdate) error {
	r.updates = append(r.updates, update)
	for index := range r.messages {
		if r.messages[index].ID == messageID {
			r.messages[index].ContentType = update.ContentType
			r.messages[index].Content = update.Content
			r.messages[index].ReasoningContent = update.ReasoningContent
			r.messages[index].Status = update.Status
		}
	}
	return nil
}

func (r *grokLeaderSyncRepository) IncrementMessageCount(_ context.Context, _ uint, delta int) error {
	r.countDelta += delta
	return nil
}

func (r *grokLeaderSyncRepository) UpsertConversationMessageTraceEvent(_ context.Context, item *model.MessageTraceEventRow) error {
	r.traceRows = append(r.traceRows, *item)
	return nil
}

func TestSyncGrokLeaderHistoryPreservesLegacyMessagesAndAddsMissingTail(t *testing.T) {
	conversation := &model.Conversation{ID: 1, UserID: 2, PublicID: "conversation-1"}
	sessionID := "session-1"
	repo := &grokLeaderSyncRepository{messages: []model.Message{
		{ID: 10, ConversationID: 1, UserID: 2, PublicID: grokLeaderHistoryPublicID(conversation.PublicID, sessionID, 0), Role: "user", ContentType: "text", Content: "Question", Status: "success"},
		{ID: 11, ConversationID: 1, UserID: 2, PublicID: grokLeaderHistoryPublicID(conversation.PublicID, sessionID, 1), Role: "assistant", ContentType: "markdown", Content: "Old answer", Status: "pending"},
	}}
	service := &Service{repo: repo}
	history := []llm.GrokLeaderHistoryMessage{
		{Role: "user", StableID: "user-1", Content: "Question"},
		{
			Role: "assistant", StableID: "prompt-1", RunID: "prompt-1", Content: "Answer", ReasoningContent: "Reason",
			Events: []llm.GrokLeaderEvent{
				{Kind: "agent_thought_chunk", EventID: "thought-1", PromptID: "prompt-1", Text: "Reason"},
				{Kind: "tool_call", EventID: "tool-event-1", PromptID: "prompt-1", ToolCall: &llm.ToolCall{ToolCallID: "tool-1", ToolType: "function", ToolName: "Read", Status: "completed"}},
			},
		},
		{Role: "user", StableID: "user-2", Content: "Follow-up"},
		{Role: "assistant", StableID: "prompt-2", RunID: "prompt-2", Content: "Second answer"},
	}

	count, err := service.syncGrokLeaderHistory(context.Background(), conversation, sessionID, history)
	if err != nil {
		t.Fatalf("sync history: %v", err)
	}
	if count != 4 || repo.countDelta != 2 || len(repo.created) != 2 {
		t.Fatalf("unexpected reconciliation counts: count=%d delta=%d created=%d", count, repo.countDelta, len(repo.created))
	}
	if len(repo.updates) != 1 || repo.updates[0].Content != "Answer" || repo.updates[0].ReasoningContent != "Reason" {
		t.Fatalf("unexpected legacy assistant update: %#v", repo.updates)
	}
	if repo.created[0].ParentMessageID == nil || *repo.created[0].ParentMessageID != 11 {
		t.Fatalf("new user message lost legacy parent: %#v", repo.created[0])
	}
	if repo.created[1].ParentMessageID == nil || *repo.created[1].ParentMessageID != repo.created[0].ID {
		t.Fatalf("new assistant message lost tail parent: %#v", repo.created[1])
	}
	if len(repo.traceRows) != 2 || repo.traceRows[0].EventID != "thought-1" || repo.traceRows[1].EventID != "tool-1" {
		t.Fatalf("unexpected imported trace rows: %#v", repo.traceRows)
	}

	if _, err = service.syncGrokLeaderHistory(context.Background(), conversation, sessionID, history); err != nil {
		t.Fatalf("repeat sync history: %v", err)
	}
	if len(repo.created) != 2 || repo.countDelta != 2 {
		t.Fatalf("repeat sync was not idempotent: created=%d delta=%d", len(repo.created), repo.countDelta)
	}
}

func TestSyncGrokLeaderTraceKeepsAbsoluteSequenceAcrossLiveBatches(t *testing.T) {
	repo := &grokLeaderSyncRepository{}
	service := &Service{repo: repo}
	conversation := &model.Conversation{ID: 1, UserID: 2}
	assistant := &model.Message{ID: 3, PublicID: "assistant-1", CreatedAt: time.Unix(1, 0).UTC()}
	history := llm.GrokLeaderHistoryMessage{
		StableID: "prompt-1",
		RunID:    "prompt-1",
		Events: []llm.GrokLeaderEvent{
			{Kind: "agent_thought_chunk", EventID: "thought-1", Text: "First thought"},
			{Kind: "tool_call", ToolCall: &llm.ToolCall{ToolCallID: "tool-1", ToolName: "Read", Status: "completed"}},
			{Kind: "agent_thought_chunk", EventID: "thought-2", Text: "Second thought"},
			{Kind: "tool_call_update", ToolCall: &llm.ToolCall{ToolCallID: "tool-2", ToolName: "Command", Status: "completed"}},
		},
	}

	if err := service.syncGrokLeaderTrace(context.Background(), conversation, assistant, history, 2); err != nil {
		t.Fatalf("sync live trace batch: %v", err)
	}
	if len(repo.traceRows) != 2 {
		t.Fatalf("expected two tail trace rows, got %#v", repo.traceRows)
	}
	if repo.traceRows[0].EventID != "thought-2" || repo.traceRows[0].Seq != 3 {
		t.Fatalf("unexpected thought sequence: %#v", repo.traceRows[0])
	}
	if repo.traceRows[1].EventID != "tool-2" || repo.traceRows[1].Seq != 4 || repo.traceRows[1].ParentEventID != "thought-2" {
		t.Fatalf("unexpected tool sequence or parent: %#v", repo.traceRows[1])
	}
}

func TestAppendGrokLeaderLiveEventPreservesDeltaBoundaries(t *testing.T) {
	history := appendGrokLeaderLiveEvent(nil, llm.GrokLeaderEvent{Kind: "user_message_chunk", EventID: "user-1", Text: "Question"})
	history = appendGrokLeaderLiveEvent(history, llm.GrokLeaderEvent{Kind: "agent_thought_chunk", EventID: "thought-1", PromptID: "prompt-1", Text: "First"})
	history = appendGrokLeaderLiveEvent(history, llm.GrokLeaderEvent{Kind: "agent_thought_chunk", EventID: "thought-2", PromptID: "prompt-1", Text: " line\n"})
	history = appendGrokLeaderLiveEvent(history, llm.GrokLeaderEvent{Kind: "agent_message_chunk", EventID: "answer-1", PromptID: "prompt-1", Text: "Answer"})
	history = appendGrokLeaderLiveEvent(history, llm.GrokLeaderEvent{Kind: "agent_message_chunk", EventID: "answer-2", PromptID: "prompt-1", Text: " text\n"})

	if len(history) != 2 || history[1].ReasoningContent != "First line\n" || history[1].Content != "Answer text\n" {
		t.Fatalf("live deltas were not preserved exactly: %#v", history)
	}
	if !grokLeaderLiveStartsMessage(history, llm.GrokLeaderEvent{Kind: "user_message_chunk"}) {
		t.Fatal("new user event should start a message after an assistant tail")
	}
}
