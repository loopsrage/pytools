import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { RefreshCw, Search, Eye, EyeOff, Code2, X, Send } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";

export interface BrainCardSummary {
  entry_id: string;
  conversation_id: string;
  entry_type?: string;
  message_role?: string;
  title?: string;
  snippet?: string;
  message_text?: string;
  thoughts_text?: string;
  workspace_project?: string;
  updated_at?: string;
  created_at?: string;
  sort_ts_ms?: number;
  mtime_ns?: number;
}

interface ConversationTab {
  conversation_id: string;
  label: string;
  friendly_name?: string;
  entry_count?: number;
}

interface BrainOverviewResponse {
  total_entries: number;
  primary_conversations_count: number;
  artifacts_count: number;
  conversation_tabs?: ConversationTab[];
  entries?: BrainCardSummary[];
}

interface BrainEntryCodeBlock {
  block_type: string;
  label?: string;
  language?: string;
  content: string;
}

interface BrainEntryDetail {
  entry_id: string;
  title?: string;
  conversation_id?: string;
  updated_at?: string;
  code_blocks?: BrainEntryCodeBlock[];
  rejoined_qdrant?: {
    integrity_verified?: boolean;
    chunk_count?: number;
    rejoined_text?: string;
  };
}

const TIME_FILTERS = [
  { value: "all", label: "All Time" },
  { value: "1h", label: "1h" },
  { value: "6h", label: "6h" },
  { value: "24h", label: "24h" },
  { value: "7d", label: "7d" },
  { value: "30d", label: "30d" },
];

const ROLE_FILTERS = [
  { value: "ALL", label: "All Roles" },
  { value: "USER", label: "User" },
  { value: "AGENT", label: "Agent" },
  { value: "ARTIFACT", label: "Artifacts" },
];

function formatCleanTimestamp(raw?: string, sortTsMs?: number): string {
  if (sortTsMs && sortTsMs > 0) {
    return new Date(sortTsMs).toISOString().replace("T", " ").slice(0, 19) + " UTC";
  }
  if (!raw) return "";
  const parsed = Date.parse(raw);
  if (Number.isFinite(parsed)) {
    return new Date(parsed).toISOString().replace("T", " ").slice(0, 19) + " UTC";
  }
  return raw.replace("T", " ").slice(0, 19);
}

export const BrainExplorerView: React.FC = () => {
  const [overview, setOverview] = useState<BrainOverviewResponse | null>(null);
  const [entries, setEntries] = useState<BrainCardSummary[]>([]);
  const [activeConv, setActiveConv] = useState<string>("All");
  const [query, setQuery] = useState<string>("");
  const [timeRange, setTimeRange] = useState<string>("all");
  const [roleFilter, setRoleFilter] = useState<string>("ALL");
  const [showThoughts, setShowThoughts] = useState<boolean>(false);
  const [loading, setLoading] = useState<boolean>(false);
  const [syncing, setSyncing] = useState<boolean>(false);
  const [chatDraft, setChatDraft] = useState<string>("");
  const [selectedDetail, setSelectedDetail] = useState<BrainEntryDetail | null>(null);
  const wsRef = useRef<WebSocket | null>(null);
  const scrollRef = useRef<HTMLDivElement | null>(null);

  const loadOverview = useCallback(async () => {
    try {
      const res = await fetch("/api/antigravity/overview");
      if (!res.ok) return;
      const data = (await res.json()) as BrainOverviewResponse;
      setOverview(data);
    } catch {
      // transient network error
    }
  }, []);

  const searchEntries = useCallback(async () => {
    setLoading(true);
    try {
      const params = new URLSearchParams({
        query: query.trim(),
        time_range: timeRange,
        sort_order: "asc",
        top_k: "120",
      });
      if (activeConv !== "All") params.set("conversation_id", activeConv);
      if (roleFilter !== "ALL") params.set("role_filter", roleFilter);
      const res = await fetch(`/api/antigravity/search?${params.toString()}`);
      if (res.ok) {
        const body = await res.json();
        if (Array.isArray(body.results)) {
          setEntries(body.results);
        }
      }
    } finally {
      setLoading(false);
    }
  }, [activeConv, query, roleFilter, timeRange]);

  useEffect(() => {
    void loadOverview();
  }, [loadOverview]);

  useEffect(() => {
    void searchEntries();
  }, [searchEntries]);

  useEffect(() => {
    const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
    const ws = new WebSocket(`${proto}//${window.location.host}/api/antigravity/ws/chat`);
    wsRef.current = ws;
    ws.onmessage = (evt) => {
      try {
        const frame = JSON.parse(evt.data);
        if (
          (frame.type === "snapshot" || frame.type === "stream_delta") &&
          Array.isArray(frame.messages)
        ) {
          setEntries(frame.messages);
        } else if (frame.type === "stream_complete") {
          void searchEntries();
        }
      } catch {
        // ignore malformed frame
      }
    };
    return () => {
      ws.close();
      wsRef.current = null;
    };
  }, [searchEntries]);

  useEffect(() => {
    const ws = wsRef.current;
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(
        JSON.stringify({
          type: "subscribe",
          conversation_id: activeConv === "All" ? null : activeConv,
          query: query.trim(),
          time_range: timeRange,
          role_filter: roleFilter,
          sort_order: "asc",
          top_k: 120,
        })
      );
    }
  }, [activeConv, query, roleFilter, timeRange]);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [entries.length, activeConv]);

  const handleSync = async () => {
    setSyncing(true);
    try {
      await fetch("/api/antigravity/sync", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ force: false }),
      });
      await loadOverview();
      await searchEntries();
    } finally {
      setSyncing(false);
    }
  };

  const handleInspectEntry = async (entryId: string) => {
    const res = await fetch(`/api/antigravity/entries/${encodeURIComponent(entryId)}`);
    if (res.ok) {
      setSelectedDetail((await res.json()) as BrainEntryDetail);
    }
  };

  const handleSendChat = async (e: React.FormEvent) => {
    e.preventDefault();
    const text = chatDraft.trim();
    if (!text) return;
    setChatDraft("");
    const convTarget =
      activeConv !== "All"
        ? activeConv
        : overview?.conversation_tabs?.[0]?.conversation_id || "default";
    await fetch("/api/antigravity/chat/send", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ conversation_id: convTarget, text }),
    });
    await searchEntries();
  };

  const tabs = useMemo(() => {
    const list = overview?.conversation_tabs || [];
    return [{ conversation_id: "All", label: "All Sessions" }, ...list];
  }, [overview]);

  return (
    <div
      data-testid="antigravity-brain-section"
      className="flex flex-col h-full gap-3 min-h-0"
    >
      <Card className="p-3 flex flex-wrap items-center justify-between gap-2">
        <div className="flex items-center gap-2 flex-1 min-w-[240px]">
          <Search className="h-4 w-4 text-muted-foreground shrink-0" />
          <input
            data-testid="antigravity-search-input"
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search Antigravity Brain conversations & Qdrant chunks..."
            className="w-full bg-transparent text-sm outline-none"
          />
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <select
            value={timeRange}
            onChange={(e) => setTimeRange(e.target.value)}
            aria-label="Time filter"
            className="h-8 rounded-md border border-border bg-background px-2 text-xs"
          >
            {TIME_FILTERS.map((opt) => (
              <option key={opt.value} value={opt.value}>
                {opt.label}
              </option>
            ))}
          </select>

          <select
            value={roleFilter}
            onChange={(e) => setRoleFilter(e.target.value)}
            aria-label="Role filter"
            className="h-8 rounded-md border border-border bg-background px-2 text-xs"
          >
            {ROLE_FILTERS.map((opt) => (
              <option key={opt.value} value={opt.value}>
                {opt.label}
              </option>
            ))}
          </select>

          <Button
            variant="outline"
            size="sm"
            data-testid="toggle-agent-thoughts-btn"
            onClick={() => setShowThoughts((v) => !v)}
          >
            {showThoughts ? (
              <EyeOff className="h-3.5 w-3.5 mr-1.5" />
            ) : (
              <Eye className="h-3.5 w-3.5 mr-1.5" />
            )}
            Thoughts
          </Button>

          <Button
            variant="outline"
            size="sm"
            onClick={handleSync}
            disabled={syncing}
          >
            <RefreshCw className={`h-3.5 w-3.5 mr-1.5 ${syncing ? "animate-spin" : ""}`} />
            Sync ({overview?.total_entries ?? entries.length})
          </Button>
        </div>
      </Card>

      <div className="flex items-center gap-1.5 overflow-x-auto pb-1 shrink-0">
        {tabs.map((tab, idx) => {
          const selected = activeConv === tab.conversation_id;
          return (
            <button
              key={tab.conversation_id}
              data-testid={`antigravity-conv-tab-${idx}`}
              onClick={() => setActiveConv(tab.conversation_id)}
              className={`px-3 py-1.5 rounded-md text-xs font-medium whitespace-nowrap border transition-colors ${
                selected
                  ? "bg-primary text-primary-foreground border-primary"
                  : "bg-card text-muted-foreground border-border hover:text-foreground"
              }`}
            >
              {tab.friendly_name || tab.label || tab.conversation_id}
            </button>
          );
        })}
      </div>

      <Card className="flex-1 flex flex-col min-h-0 overflow-hidden">
        <div
          ref={scrollRef}
          data-testid="antigravity-infinite-scroll-container"
          className="flex-1 overflow-y-auto p-4 space-y-3"
        >
          {loading && entries.length === 0 ? (
            <p className="text-sm text-muted-foreground">Loading brain entries...</p>
          ) : entries.length === 0 ? (
            <p className="text-sm text-muted-foreground">No matching brain entries found.</p>
          ) : (
            entries.map((entry) => {
              const role = (entry.message_role || "AGENT").toUpperCase();
              const isUser = role === "USER";
              const body = entry.message_text || entry.snippet || entry.title || "";
              return (
                <div
                  key={entry.entry_id}
                  data-testid={`antigravity-card-${entry.entry_id}`}
                  className={`rounded-lg border p-3 text-sm ${
                    isUser
                      ? "border-primary/40 bg-primary/5 ml-6"
                      : "border-border bg-secondary/20 mr-6"
                  }`}
                >
                  <div className="flex items-center justify-between gap-2 mb-1.5 text-xs text-muted-foreground">
                    <span
                      data-testid={`antigravity-card-timestamp-${entry.entry_id}`}
                      className="font-mono"
                    >
                      {formatCleanTimestamp(
                        entry.updated_at || entry.created_at,
                        entry.sort_ts_ms
                      )}
                    </span>
                    <button
                      onClick={() => handleInspectEntry(entry.entry_id)}
                      className="inline-flex items-center gap-1 hover:text-foreground"
                    >
                      <Code2 className="h-3.5 w-3.5" />
                      Inspect Blocks
                    </button>
                  </div>
                  {showThoughts && entry.thoughts_text && (
                    <pre
                      data-testid={`agent-thoughts-${entry.entry_id}`}
                      className="mb-2 rounded bg-muted/60 p-2 text-xs font-mono whitespace-pre-wrap text-muted-foreground"
                    >
                      {entry.thoughts_text}
                    </pre>
                  )}
                  <div
                    data-testid={`antigravity-card-snippet-${entry.entry_id}`}
                    className="whitespace-pre-wrap break-words leading-relaxed"
                  >
                    {body}
                  </div>
                </div>
              );
            })
          )}
        </div>

        <form
          onSubmit={handleSendChat}
          className="border-t border-border p-3 flex items-center gap-2"
        >
          <input
            type="text"
            value={chatDraft}
            onChange={(e) => setChatDraft(e.target.value)}
            placeholder="Append message to active brain session..."
            className="flex-1 h-9 rounded-md border border-border bg-background px-3 text-sm outline-none"
          />
          <Button type="submit" size="sm">
            <Send className="h-3.5 w-3.5 mr-1.5" />
            Send
          </Button>
        </form>
      </Card>

      {selectedDetail && (
        <div
          role="dialog"
          aria-modal="true"
          className="fixed inset-0 z-50 bg-black/60 flex items-center justify-center p-4"
        >
          <Card className="w-full max-w-4xl max-h-[85vh] flex flex-col overflow-hidden bg-background">
            <div className="flex items-center justify-between px-4 py-3 border-b border-border">
              <div>
                <h3 className="font-semibold text-sm">
                  {selectedDetail.title || selectedDetail.entry_id}
                </h3>
                <p className="text-xs text-muted-foreground font-mono">
                  {selectedDetail.conversation_id} • Qdrant SHA-256 verified:{" "}
                  {String(Boolean(selectedDetail.rejoined_qdrant?.integrity_verified))}
                </p>
              </div>
              <Button
                variant="outline"
                size="sm"
                onClick={() => setSelectedDetail(null)}
              >
                <X className="h-4 w-4 mr-1" />
                Close
              </Button>
            </div>
            <div className="flex-1 overflow-y-auto p-4 space-y-3">
              {(selectedDetail.code_blocks || []).map((blk, i) => (
                <div key={i} className="rounded-lg border border-border overflow-hidden">
                  <div className="bg-muted px-3 py-1.5 text-xs font-mono font-semibold">
                    {blk.block_type} {blk.label ? `— ${blk.label}` : ""}
                  </div>
                  <pre className="p-3 text-xs font-mono whitespace-pre-wrap overflow-x-auto">
                    {blk.content}
                  </pre>
                </div>
              ))}
            </div>
          </Card>
        </div>
      )}
    </div>
  );
};
