package conversation

import (
	"context"
	"errors"
	"net/http"
	"strings"

	appconversation "github.com/DEEIX-AI/DEEIX-Chat/backend/internal/application/conversation"
	"github.com/DEEIX-AI/DEEIX-Chat/backend/internal/shared/response"
	"github.com/DEEIX-AI/DEEIX-Chat/backend/internal/transport/http/middleware"
	"github.com/gin-gonic/gin"
)

// ListGrokLeaderSessions godoc
// @Summary 查询共享 Grok leader 会话
// @Description 返回与 Deeix 使用同一 Grok leader 的可发现会话
// @Tags chat
// @Produce json
// @Security BearerAuth
// @Param model query string true "使用 grok_leader 协议的平台模型名"
// @Success 200 {object} GrokLeaderSessionListResponseDoc
// @Failure 400 {object} ErrorDoc
// @Failure 409 {object} ErrorDoc
// @Failure 502 {object} ErrorDoc
// @Failure 503 {object} ErrorDoc
// @Router /grok/leader/sessions [get]
func (h *Handler) ListGrokLeaderSessions(c *gin.Context) {
	modelName := strings.TrimSpace(c.Query("model"))
	if modelName == "" || len(modelName) > 128 {
		response.Error(c, http.StatusBadRequest, "invalid Grok leader model")
		return
	}
	items, err := h.service.ListGrokLeaderSessions(
		c.Request.Context(),
		middleware.MustUserID(c),
		modelName,
		middleware.MustRequestID(c),
	)
	if err != nil {
		handleGrokLeaderSessionError(c, err)
		return
	}
	results := make([]GrokLeaderSessionResponse, 0, len(items))
	for _, item := range items {
		results = append(results, toGrokLeaderSessionResponse(item))
	}
	response.Success(c, results)
}

// BindGrokLeaderSession godoc
// @Summary 绑定已有 Grok leader 会话
// @Description 加载目标 Grok 会话并将其绑定到当前 Deeix 会话
// @Tags chat
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param id path string true "会话 public_id"
// @Param body body BindGrokLeaderSessionRequest true "绑定参数"
// @Success 200 {object} GrokLeaderSessionBindingResponseDoc
// @Failure 400 {object} ErrorDoc
// @Failure 404 {object} ErrorDoc
// @Failure 409 {object} ErrorDoc
// @Failure 502 {object} ErrorDoc
// @Failure 503 {object} ErrorDoc
// @Router /conversations/{id}/grok-session [post]
func (h *Handler) BindGrokLeaderSession(c *gin.Context) {
	publicID, err := stringParam(c, "id")
	if err != nil {
		response.Error(c, http.StatusBadRequest, "invalid conversation id")
		return
	}
	var req BindGrokLeaderSessionRequest
	if err = c.ShouldBindJSON(&req); err != nil {
		response.InvalidRequestBody(c, err)
		return
	}
	result, err := h.service.BindGrokLeaderSession(
		c.Request.Context(),
		middleware.MustUserID(c),
		publicID,
		req.SessionID,
		req.PlatformModelName,
		middleware.MustRequestID(c),
	)
	if err != nil {
		handleGrokLeaderSessionError(c, err)
		return
	}
	h.recordAudit(c, "bind_grok_leader_session", "conversation", publicID, map[string]string{
		"session_id": result.Session.SessionID,
		"model":      result.Conversation.Model,
	})
	response.Success(c, GrokLeaderSessionBindingResponse{
		Conversation: toConversationResponse(result.Conversation),
		Session:      toGrokLeaderSessionResponse(result.Session),
	})
}

// ObserveGrokLeaderConversation godoc
// @Summary 观察已绑定的 Grok leader 会话
// @Description 对账完整历史，并在会话发生更新时发送变更事件
// @Tags chat
// @Produce text/event-stream
// @Security BearerAuth
// @Param id path string true "会话 public_id"
// @Success 200 {string} string "SSE stream"
// @Failure 404 {object} ErrorDoc
// @Failure 409 {object} ErrorDoc
// @Failure 502 {object} ErrorDoc
// @Failure 503 {object} ErrorDoc
// @Router /conversations/{id}/grok-session/stream [get]
func (h *Handler) ObserveGrokLeaderConversation(c *gin.Context) {
	publicID, err := stringParam(c, "id")
	if err != nil {
		response.Error(c, http.StatusBadRequest, "invalid conversation id")
		return
	}

	observerCtx, cancel := context.WithCancel(c.Request.Context())
	defer cancel()
	go func() {
		select {
		case <-h.shutdown.Done():
			cancel()
		case <-observerCtx.Done():
		}
	}()

	started := false
	writeChanged := func() error {
		if !started {
			c.Header("Content-Type", "text/event-stream; charset=utf-8")
			c.Header("Cache-Control", "no-cache, no-transform")
			c.Header("Connection", "keep-alive")
			c.Header("X-Accel-Buffering", "no")
			c.Status(http.StatusOK)
			started = true
		}
		if _, writeErr := c.Writer.Write([]byte("data: {\"type\":\"changed\"}\n\n")); writeErr != nil {
			return writeErr
		}
		c.Writer.Flush()
		return nil
	}

	err = h.service.ObserveGrokLeaderConversation(
		observerCtx,
		middleware.MustUserID(c),
		publicID,
		middleware.MustRequestID(c),
		writeChanged,
	)
	if started || errors.Is(err, context.Canceled) {
		return
	}
	handleGrokLeaderSessionError(c, err)
}

func handleGrokLeaderSessionError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, appconversation.ErrConversationNotFound):
		response.Error(c, http.StatusNotFound, "conversation not found")
	case errors.Is(err, appconversation.ErrGrokLeaderSessionNotFound):
		response.Error(c, http.StatusNotFound, "Grok leader session not found")
	case errors.Is(err, appconversation.ErrGrokLeaderModelRequired):
		response.Error(c, http.StatusConflict, "selected model does not use Grok leader")
	case errors.Is(err, appconversation.ErrModelAccessDenied):
		response.Error(c, http.StatusForbidden, "model access denied")
	case errors.Is(err, appconversation.ErrModelRouteNotConfigured):
		response.Error(c, http.StatusServiceUnavailable, "Grok leader model route not configured")
	default:
		response.Error(c, http.StatusBadGateway, "Grok leader unavailable")
	}
}
