"use client";

import { FolderGit2, LoaderCircle, Radio, RefreshCw } from "lucide-react";
import { useTranslations } from "next-intl";
import * as React from "react";
import { toast } from "sonner";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { bindGrokLeaderSession, listGrokLeaderSessions } from "@/shared/api/conversation";
import type {
  ConversationDTO,
  GrokLeaderSessionBindingDTO,
  GrokLeaderSessionDTO,
} from "@/shared/api/conversation.types";
import { resolveAccessToken } from "@/shared/auth/resolve-access-token";
import { cn } from "@/lib/utils";

const ROSTER_POLL_INTERVAL_MS = 5_000;

type GrokSessionBrowserDialogProps = {
  open: boolean;
  conversation: ConversationDTO;
  platformModelName: string;
  busy: boolean;
  onOpenChange: (open: boolean) => void;
  onBound: (binding: GrokLeaderSessionBindingDTO) => void;
};

function formatLastChange(value: number): string {
  if (!Number.isFinite(value) || value <= 0) {
    return "";
  }
  return new Date(value).toLocaleString();
}

export function GrokSessionBrowserDialog({
  open,
  conversation,
  platformModelName,
  busy,
  onOpenChange,
  onBound,
}: GrokSessionBrowserDialogProps) {
  const t = useTranslations("chat.grokSession");
  const activityLabels = React.useMemo<Record<string, string>>(() => ({
    working: t("activity.working"),
    idle: t("activity.idle"),
    needs_input: t("activity.needsInput"),
    dormant: t("activity.dormant"),
    completed: t("activity.completed"),
    dead: t("activity.dead"),
  }), [t]);
  const [sessions, setSessions] = React.useState<GrokLeaderSessionDTO[]>([]);
  const [loading, setLoading] = React.useState(false);
  const [refreshing, setRefreshing] = React.useState(false);
  const [error, setError] = React.useState("");
  const [bindingSessionID, setBindingSessionID] = React.useState("");
  const [refreshRevision, setRefreshRevision] = React.useState(0);

  React.useEffect(() => {
    if (!open) {
      setSessions([]);
      setError("");
      return;
    }
    const controller = new AbortController();
    let active = true;
    let requestRunning = false;
    setLoading(true);
    setError("");

    const load = async (polling: boolean) => {
      if (requestRunning) {
        return;
      }
      requestRunning = true;
      if (polling) {
        setRefreshing(true);
      }
      try {
        const token = await resolveAccessToken();
        if (!token) {
          throw new Error("missing access token");
        }
        const nextSessions = await listGrokLeaderSessions(token, platformModelName, controller.signal);
        if (active) {
          setSessions(nextSessions);
          setError("");
        }
      } catch (loadError) {
        if (active && !controller.signal.aborted) {
          setError(loadError instanceof Error ? loadError.message : t("browser.loadFailed"));
        }
      } finally {
        requestRunning = false;
        if (active) {
          setLoading(false);
          setRefreshing(false);
        }
      }
    };

    void load(false);
    const intervalID = window.setInterval(() => void load(true), ROSTER_POLL_INTERVAL_MS);
    return () => {
      active = false;
      controller.abort();
      window.clearInterval(intervalID);
    };
  }, [open, platformModelName, refreshRevision, t]);

  const bindSession = React.useCallback(async (session: GrokLeaderSessionDTO) => {
    if (busy || bindingSessionID) {
      return;
    }
    setBindingSessionID(session.sessionID);
    try {
      const token = await resolveAccessToken();
      if (!token) {
        throw new Error("missing access token");
      }
      const binding = await bindGrokLeaderSession(token, conversation.publicID, {
        sessionID: session.sessionID,
        platformModelName,
      });
      onBound(binding);
      toast.success(t("browser.bindSuccess"));
      onOpenChange(false);
    } catch {
      toast.error(t("browser.bindFailed"));
    } finally {
      setBindingSessionID("");
    }
  }, [bindingSessionID, busy, conversation.publicID, onBound, onOpenChange, platformModelName, t]);

  const currentSessionID = conversation.lastResponseID?.trim() ?? "";
  return (
    <Dialog open={open} onOpenChange={(nextOpen) => !bindingSessionID && onOpenChange(nextOpen)}>
      <DialogContent className="sm:max-w-[720px]">
        <DialogHeader className="pr-10">
          <div className="flex items-start justify-between gap-3">
            <div className="min-w-0">
              <DialogTitle>{t("browser.title")}</DialogTitle>
              <DialogDescription>{t("browser.description")}</DialogDescription>
            </div>
            <Tooltip>
              <TooltipTrigger asChild>
                <Button
                  type="button"
                  size="icon-sm"
                  variant="ghost"
                  aria-label={t("browser.refresh")}
                  disabled={loading || refreshing}
                  onClick={() => setRefreshRevision((value) => value + 1)}
                >
                  <RefreshCw className={cn("size-3.5", refreshing && "animate-spin")} />
                </Button>
              </TooltipTrigger>
              <TooltipContent>{t("browser.refresh")}</TooltipContent>
            </Tooltip>
          </div>
        </DialogHeader>

        <div className="min-h-48 max-h-[min(62vh,560px)] overflow-y-auto pr-1">
          {loading && sessions.length === 0 ? (
            <div className="flex min-h-48 items-center justify-center text-sm text-muted-foreground">
              <LoaderCircle className="mr-2 size-4 animate-spin" />
              {t("browser.loading")}
            </div>
          ) : error && sessions.length === 0 ? (
            <div className="flex min-h-48 flex-col items-center justify-center gap-3 px-6 text-center">
              <p className="text-sm font-medium">{t("browser.loadFailed")}</p>
              <p className="max-w-md text-xs text-muted-foreground">{error}</p>
              <Button type="button" size="sm" variant="outline" onClick={() => setRefreshRevision((value) => value + 1)}>
                <RefreshCw />
                {t("browser.retry")}
              </Button>
            </div>
          ) : sessions.length === 0 ? (
            <div className="flex min-h-48 items-center justify-center px-6 text-center text-sm text-muted-foreground">
              {t("browser.empty")}
            </div>
          ) : (
            <div className="space-y-2">
              {sessions.map((session) => {
                const current = session.sessionID === currentSessionID;
                const binding = bindingSessionID === session.sessionID;
                const lastChange = formatLastChange(session.lastChangeUnixMS);
                return (
                  <div key={session.sessionID} className="rounded-md border border-border/60 px-3 py-3">
                    <div className="flex items-start gap-3">
                      <Radio
                        className={cn(
                          "mt-0.5 size-4 shrink-0 text-muted-foreground",
                          session.activity === "working" && "animate-pulse text-emerald-600 dark:text-emerald-400",
                          session.activity === "needs_input" && "text-amber-600 dark:text-amber-400",
                        )}
                        aria-hidden
                      />
                      <div className="min-w-0 flex-1">
                        <div className="flex flex-wrap items-center gap-1.5">
                          <p className="max-w-full truncate text-sm font-medium" title={session.title || session.sessionID}>
                            {session.title || session.sessionID}
                          </p>
                          <Badge variant="secondary">{activityLabels[session.activity] ?? session.activity ?? t("activity.unknown")}</Badge>
                          {session.resident ? <Badge variant="outline">{t("browser.resident")}</Badge> : null}
                          {session.isWorktree ? <Badge variant="outline">{t("browser.worktree")}</Badge> : null}
                        </div>
                        <div className="mt-1.5 flex min-w-0 items-center gap-1.5 text-xs text-muted-foreground">
                          <FolderGit2 className="size-3.5 shrink-0" aria-hidden />
                          <code className="truncate font-mono" title={session.cwd}>{session.cwd}</code>
                        </div>
                        {session.lastTurnSummary ? (
                          <p className="mt-1.5 line-clamp-2 text-xs leading-5 text-muted-foreground">{session.lastTurnSummary}</p>
                        ) : null}
                        <div className="mt-2 flex flex-wrap gap-x-3 gap-y-1 text-[11px] text-muted-foreground">
                          {session.modelID ? <span>{t("browser.model", { model: session.modelID })}</span> : null}
                          {lastChange ? <span>{t("browser.updated", { time: lastChange })}</span> : null}
                          <code className="max-w-full truncate font-mono" title={session.sessionID}>{session.sessionID}</code>
                        </div>
                      </div>
                      <Button
                        type="button"
                        size="sm"
                        variant={current ? "secondary" : "outline"}
                        disabled={current || busy || Boolean(bindingSessionID)}
                        onClick={() => void bindSession(session)}
                      >
                        {binding ? <LoaderCircle className="animate-spin" /> : null}
                        {current ? t("browser.current") : t("browser.bind")}
                      </Button>
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </DialogContent>
    </Dialog>
  );
}
