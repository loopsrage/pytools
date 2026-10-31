import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  Cpu,
  Download,
  FilePlus,
  FileText,
  FolderOpen,
  Plus,
  RefreshCw,
  RotateCcw,
  Save,
  Send,
  Trash2,
  Upload,
  X,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";

interface PromotedModelOption {
  model_id: string;
  model_name: string;
  friendly_title?: string;
  class_name?: string;
  overall_rank?: number;
  composite_score?: number;
  is_promoted?: boolean;
}

interface PromotedConversationItem {
  conversation_id: string;
  title?: string;
  updated_at?: string;
  entry_count?: number;
}

interface PromotedChatEntry {
  entry_id: string;
  conversation_id?: string;
  message_role?: string;
  title?: string;
  snippet?: string;
  message_text?: string;
  updated_at?: string;
  created_at?: string;
  sort_ts_ms?: number;
  mtime_ns?: number;
}

interface FsTreeFile {
  name: string;
  relative_path: string;
  scope?: string;
  size_bytes?: number;
  mime_type?: string;
  is_multimedia?: boolean;
}

interface FsTreeRoot {
  short_id?: string;
  label?: string;
  title?: string;
  scope?: string;
  files?: FsTreeFile[];
  artifact_files?: FsTreeFile[];
}

interface PromotedStatusPayload {
  active_conversation_id?: string;
  active_chat_model?: PromotedModelOption | null;
  promoted_model?: PromotedModelOption | null;
  available_models?: PromotedModelOption[];
  conversations?: PromotedConversationItem[];
  chat_messages?: PromotedChatEntry[];
  fs_tree?: FsTreeRoot[];
  harness_status?: {
    collection?: string;
    qdrant_connected?: boolean;
    any_gram_entries?: number;
    active_model_id?: string;
  };
}

interface SelectedFsFileState {
  path: string;
  name: string;
  scope: string;
  mimeType: string;
  isMultimedia: boolean;
  isBinary: boolean;
  content: string;
  dataUri?: string | null;
  sizeBytes: number;
}

const TELEMETRY_LINE_PREFIXES = [
  "- **Action**:",
  "- **Callan-Symanzik RG-Flow Learning**:",
  "- **Top-5 Next Tokens**:",
  "- **6-Scale Any-Gram Store Entries**:",
  "- **Poincaré Ball Curvature",
  "- **O(1) Any-Gram Memorization**:",
  "- **Knowledge Reset**:",
];

function friendlyModelTitle(rawName?: string, className?: string): string {
  const base = (rawName || "").split(" (")[0].trim() || className || "Promoted Model";
  return base
    .replace(/Model$/, "")
    .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
    .replace(/Cohesive Manifold/g, "Cohesive-Manifold")
    .replace(/VCPU/g, "vCPU")
    .replace(/2 GRU/g, "2-GRU")
    .trim();
}

function isSyntheticSeedEntry(entryId: string, rawText: string): boolean {
  return (
    entryId.startsWith("promoted_chat_init_") ||
    rawText.trimStart().startsWith("### Promoted Model Harness Online")
  );
}

function stripPredictedInfoFromAgentText(rawText: string, prevUserText = ""): string {
  let text = String(rawText || "");
  if (text.includes("Autoregressive Output") && text.includes("```")) {
    const match = text.match(/```(?:text|python)?\n([\s\S]*?)\n```/);
    if (match && typeof match[1] === "string") {
      text = match[1];
    }
  } else {
    text = text
      .split("\n")
      .filter((ln) => {
        const trimmed = ln.trim();
        if (trimmed.includes("Autoregressive Output**:")) return false;
        return !TELEMETRY_LINE_PREFIXES.some((prefix) => trimmed.startsWith(prefix));
      })
      .join("\n");
  }
  const cleanPrev = prevUserText.trim();
  if (cleanPrev && text.startsWith(cleanPrev)) {
    const continuation = text.slice(cleanPrev.length).trim();
    if (continuation) return continuation;
  }
  return text.trim();
}

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

function fileToBase64(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => {
      const result = String(reader.result || "");
      const idx = result.indexOf(",");
      resolve(idx >= 0 ? result.slice(idx + 1) : result);
    };
    reader.onerror = () => reject(reader.error);
    reader.readAsDataURL(file);
  });
}

export const PromotedModelView: React.FC = () => {
  const [statusData, setStatusData] = useState<PromotedStatusPayload | null>(null);
  const [chatEntries, setChatEntries] = useState<PromotedChatEntry[]>([]);
  const [activeAction, setActiveAction] = useState<"learn" | "generate" | "memorize">("learn");
  const [chatDraft, setChatDraft] = useState<string>("");
  const [sending, setSending] = useState<boolean>(false);

  const [selectedFile, setSelectedFile] = useState<SelectedFsFileState | null>(null);
  const [editedContent, setEditedContent] = useState<string>("");
  const [newFileOpen, setNewFileOpen] = useState<boolean>(false);
  const [newFilePath, setNewFilePath] = useState<string>("artifacts/note.md");
  const [newFileContent, setNewFileContent] = useState<string>("");

  const wsRef = useRef<WebSocket | null>(null);
  const uploadInputRef = useRef<HTMLInputElement | null>(null);

  const fetchStatus = useCallback(async (convId?: string) => {
    try {
      const res = await fetch("/api/benchmarks/promoted/status");
      if (!res.ok) return;
      const data = (await res.json()) as PromotedStatusPayload;
      setStatusData(data);

      const targetConv = convId || data.active_conversation_id || "";
      if ((!convId || convId === data.active_conversation_id) && Array.isArray(data.chat_messages)) {
        setChatEntries(data.chat_messages);
        return;
      }

      const chatUrl = targetConv
        ? `/api/benchmarks/promoted/chat?conversation_id=${encodeURIComponent(targetConv)}`
        : "/api/benchmarks/promoted/chat";
      const chatRes = await fetch(chatUrl);
      if (chatRes.ok) {
        const chatJson = await chatRes.json();
        if (Array.isArray(chatJson.messages)) {
          setChatEntries(chatJson.messages);
        }
        setStatusData((prev) =>
          prev
            ? {
                ...prev,
                active_conversation_id: chatJson.active_conversation_id || prev.active_conversation_id,
                conversations: chatJson.conversations || prev.conversations,
                available_models: chatJson.available_models || prev.available_models,
                active_chat_model: chatJson.active_chat_model || prev.active_chat_model,
                fs_tree: chatJson.fs_tree || prev.fs_tree,
                harness_status: chatJson.harness_status || prev.harness_status,
              }
            : prev
        );
      }
    } catch {
      // Retain existing UI state on transient disconnect
    }
  }, []);

  useEffect(() => {
    void fetchStatus();
  }, [fetchStatus]);

  useEffect(() => {
    const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
    const ws = new WebSocket(`${proto}//${window.location.host}/api/benchmarks/promoted/ws/chat`);
    wsRef.current = ws;

    ws.onmessage = (evt) => {
      try {
        const payload = JSON.parse(evt.data);
        if (Array.isArray(payload?.messages)) {
          setChatEntries(payload.messages);
        } else if (payload?.type === "chat_ack" || payload?.type === "promoted_update") {
          void fetchStatus(payload?.conversation_id);
        }
      } catch {
        // Ignore malformed frame
      }
    };

    return () => {
      ws.close();
      wsRef.current = null;
    };
  }, [fetchStatus]);

  const activeConversationId = statusData?.active_conversation_id || "promoted_model_harness";
  const activeModel = statusData?.active_chat_model || statusData?.promoted_model || null;
  const availableModels = statusData?.available_models || [];
  const conversations = statusData?.conversations || [];
  const fsRoots = statusData?.fs_tree || [];

  const cleanMessages = useMemo(() => {
    const out: Array<{ id: string; role: "USER" | "AGENT"; text: string; timestamp: string }> = [];
    let lastUser = "";
    for (const e of chatEntries) {
      const id = String(e.entry_id || "");
      const raw = String(e.message_text || e.snippet || e.title || "");
      if (isSyntheticSeedEntry(id, raw)) continue;
      const role = e.message_role === "USER" ? "USER" : "AGENT";
      let clean = raw.trim();
      if (role === "USER") {
        lastUser = clean;
      } else {
        clean = stripPredictedInfoFromAgentText(raw, lastUser) || clean;
      }
      out.push({
        id,
        role,
        text: clean,
        timestamp: formatCleanTimestamp(e.updated_at || e.created_at, e.sort_ts_ms),
      });
    }
    return out;
  }, [chatEntries]);

  const handleSelectModel = async (modelId: string) => {
    if (!modelId) return;
    const res = await fetch("/api/benchmarks/promoted/select-model", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ model_id: modelId }),
    });
    if (res.ok) {
      await fetchStatus(activeConversationId);
    }
  };

  const handleResetKnowledge = async () => {
    const res = await fetch("/api/benchmarks/promoted/reset-knowledge", { method: "POST" });
    if (res.ok) {
      await fetchStatus(activeConversationId);
    }
  };

  const handleCreateConversation = async () => {
    const res = await fetch("/api/benchmarks/promoted/conversations", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        action: "create",
        title: `Session ${(conversations.length || 0) + 1}`,
        model_id: activeModel?.model_id || "",
      }),
    });
    if (res.ok) {
      const data = await res.json();
      if (Array.isArray(data.messages)) setChatEntries(data.messages);
      await fetchStatus(data.active_conversation_id);
    }
  };

  const handleOpenConversation = async (convId: string) => {
    const res = await fetch("/api/benchmarks/promoted/conversations", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action: "open", conversation_id: convId }),
    });
    if (res.ok) {
      const data = await res.json();
      if (Array.isArray(data.messages)) setChatEntries(data.messages);
      await fetchStatus(convId);
    }
  };

  const handleSendChat = async () => {
    const text = chatDraft.trim();
    if (!text || sending) return;
    setSending(true);
    setChatDraft("");
    try {
      const res = await fetch("/api/benchmarks/promoted/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          text,
          action: activeAction,
          steps: 3,
          max_new_tokens: 36,
          temperature: 0.7,
          conversation_id: activeConversationId,
          model_id: activeModel?.model_id || "",
        }),
      });
      if (res.ok) {
        const data = await res.json();
        await fetchStatus(data.conversation_id || activeConversationId);
      }
    } finally {
      setSending(false);
    }
  };

  const handleOpenFsFile = async (file: FsTreeFile, root: FsTreeRoot) => {
    const scope = file.scope || root.scope || (root.short_id === "promoted" ? "promoted_repo" : "workspace");
    const relPath = file.relative_path || file.name;
    const res = await fetch(
      `/api/benchmarks/promoted/fs?action=read&path=${encodeURIComponent(relPath)}&scope=${encodeURIComponent(scope)}`
    );
    if (!res.ok) return;
    const data = await res.json();
    if (data.status === "ok") {
      setSelectedFile({
        path: data.relative_path || relPath,
        name: data.name || file.name,
        scope,
        mimeType: data.mime_type || "text/plain",
        isMultimedia: Boolean(data.is_multimedia),
        isBinary: Boolean(data.is_binary),
        content: data.content || "",
        dataUri: data.data_uri || null,
        sizeBytes: Number(data.size_bytes || 0),
      });
      setEditedContent(data.content || "");
    }
  };

  const handleCreateFsFile = async () => {
    const trimmed = newFilePath.trim();
    if (!trimmed) return;
    const normalizedPath = trimmed.includes("/") ? trimmed : `artifacts/${trimmed}`;
    const res = await fetch("/api/benchmarks/promoted/fs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        action: "create",
        path: normalizedPath,
        content: newFileContent,
        scope: "workspace",
      }),
    });
    if (res.ok) {
      setNewFileOpen(false);
      setNewFileContent("");
      await fetchStatus(activeConversationId);
    }
  };

  const handleUploadFsFiles = async (files: FileList | null) => {
    if (!files || files.length === 0) return;
    for (const file of Array.from(files)) {
      const b64 = await fileToBase64(file);
      const isMedia =
        file.type.startsWith("image/") ||
        file.type.startsWith("audio/") ||
        file.type.startsWith("video/") ||
        file.type === "application/pdf";
      const folder = isMedia ? "multimedia" : "artifacts";
      await fetch("/api/benchmarks/promoted/fs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          action: "upload",
          path: `${folder}/${file.name}`,
          base64_content: b64,
          mime_type: file.type || "application/octet-stream",
          scope: "workspace",
        }),
      });
    }
    await fetchStatus(activeConversationId);
  };

  const handleSaveFsFile = async () => {
    if (!selectedFile) return;
    const res = await fetch("/api/benchmarks/promoted/fs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        action: "update",
        path: selectedFile.path,
        content: editedContent,
        scope: selectedFile.scope,
      }),
    });
    if (res.ok) {
      setSelectedFile((prev) => (prev ? { ...prev, content: editedContent } : prev));
      await fetchStatus(activeConversationId);
    }
  };

  const handleDeleteFsFile = async () => {
    if (!selectedFile) return;
    const res = await fetch("/api/benchmarks/promoted/fs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        action: "delete",
        path: selectedFile.path,
        scope: selectedFile.scope,
      }),
    });
    if (res.ok) {
      setSelectedFile(null);
      await fetchStatus(activeConversationId);
    }
  };

  return (
    <div className="space-y-4" data-testid="promoted-model-section">
      {/* Top Bar: Model Slot Selector + Qdrant Provenance + Actions */}
      <Card className="p-4 space-y-3">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="flex items-center gap-2.5">
            <Cpu className="h-5 w-5 text-primary" />
            <div>
              <h1 className="text-lg font-bold leading-tight">
                {friendlyModelTitle(activeModel?.friendly_title || activeModel?.model_name, activeModel?.class_name)}
              </h1>
              <p className="text-xs text-muted-foreground" data-testid="promoted-qdrant-provenance">
                Collection: {statusData?.harness_status?.collection || "portfolio_neural_benchmarks"} · Rank #
                {activeModel?.overall_rank ?? 1} · Composite{" "}
                {typeof activeModel?.composite_score === "number"
                  ? activeModel.composite_score.toFixed(4)
                  : "—"}
              </p>
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <select
              data-testid="promoted-model-select"
              value={activeModel?.model_id || ""}
              onChange={(e) => void handleSelectModel(e.target.value)}
              className="h-9 rounded-lg border border-border bg-background px-3 text-xs font-medium"
            >
              {availableModels.map((m) => (
                <option key={m.model_id} value={m.model_id}>
                  #{m.overall_rank ?? "—"} {friendlyModelTitle(m.friendly_title || m.model_name, m.class_name)}
                </option>
              ))}
            </select>

            <Button variant="outline" size="sm" onClick={() => void handleCreateConversation()}>
              <Plus className="h-3.5 w-3.5 mr-1" />
              New Session
            </Button>
            <Button variant="outline" size="sm" onClick={() => void handleResetKnowledge()}>
              <RotateCcw className="h-3.5 w-3.5 mr-1" />
              Reset Knowledge
            </Button>
            <Button variant="ghost" size="sm" onClick={() => void fetchStatus(activeConversationId)}>
              <RefreshCw className="h-3.5 w-3.5" />
            </Button>
          </div>
        </div>

        {/* Conversation Tabs & Action Mode */}
        <div className="flex flex-wrap items-center justify-between gap-2 pt-2 border-t border-border">
          <div className="flex flex-wrap items-center gap-1.5">
            {conversations.map((c) => {
              const active = c.conversation_id === activeConversationId;
              return (
                <button
                  key={c.conversation_id}
                  onClick={() => void handleOpenConversation(c.conversation_id)}
                  className={`px-2.5 py-1 rounded-md text-xs font-medium transition-colors ${
                    active
                      ? "bg-primary text-primary-foreground"
                      : "bg-secondary text-secondary-foreground hover:opacity-90"
                  }`}
                >
                  {c.title || c.conversation_id}
                </button>
              );
            })}
          </div>

          <div className="flex items-center gap-1">
            {(["learn", "generate", "memorize"] as const).map((mode) => (
              <button
                key={mode}
                onClick={() => setActiveAction(mode)}
                className={`px-2.5 py-1 rounded-md text-xs font-semibold uppercase tracking-wide ${
                  activeAction === mode
                    ? "bg-primary text-primary-foreground"
                    : "border border-border text-muted-foreground hover:text-foreground"
                }`}
              >
                {mode}
              </button>
            ))}
          </div>
        </div>
      </Card>

      {/* Main Split Grid: Left FS Explorer, Right Chat Window */}
      <div className="grid grid-cols-1 lg:grid-cols-12 gap-4">
        {/* Left: Workspace & Staging FS CRUD */}
        <Card className="lg:col-span-4 p-3 flex flex-col h-[620px]">
          <div className="flex items-center justify-between pb-2 mb-2 border-b border-border">
            <div className="flex items-center gap-1.5 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
              <FolderOpen className="h-4 w-4" />
              <span>Workspace FS</span>
            </div>
            <div className="flex items-center gap-1">
              <Button variant="outline" size="sm" onClick={() => setNewFileOpen((v) => !v)}>
                <FilePlus className="h-3.5 w-3.5 mr-1" />
                New
              </Button>
              <Button variant="outline" size="sm" onClick={() => uploadInputRef.current?.click()}>
                <Upload className="h-3.5 w-3.5 mr-1" />
                Upload
              </Button>
              <input
                ref={uploadInputRef}
                type="file"
                multiple
                className="hidden"
                onChange={(e) => void handleUploadFsFiles(e.target.files)}
              />
            </div>
          </div>

          {newFileOpen && (
            <div className="mb-3 p-2.5 rounded-lg border border-border bg-secondary/40 space-y-2">
              <input
                type="text"
                value={newFilePath}
                onChange={(e) => setNewFilePath(e.target.value)}
                placeholder="artifacts/note.md"
                className="w-full rounded border border-border bg-background px-2 py-1 text-xs"
              />
              <textarea
                rows={3}
                value={newFileContent}
                onChange={(e) => setNewFileContent(e.target.value)}
                placeholder="File contents..."
                className="w-full rounded border border-border bg-background px-2 py-1 text-xs font-mono"
              />
              <div className="flex justify-end gap-1.5">
                <Button variant="ghost" size="sm" onClick={() => setNewFileOpen(false)}>
                  Cancel
                </Button>
                <Button size="sm" onClick={() => void handleCreateFsFile()}>
                  Create
                </Button>
              </div>
            </div>
          )}

          <div className="flex-1 overflow-y-auto space-y-3 pr-1">
            {fsRoots.length === 0 ? (
              <p className="text-xs text-muted-foreground p-2">No workspace roots indexed yet.</p>
            ) : (
              fsRoots.map((root, idx) => {
                const items = root.files || root.artifact_files || [];
                return (
                  <div key={root.short_id || root.label || idx} className="space-y-1">
                    <div className="text-xs font-semibold text-foreground px-1">
                      {root.label || root.title || root.short_id || "Workspace"} ({items.length})
                    </div>
                    <div className="space-y-0.5">
                      {items.map((f) => (
                        <button
                          key={`${f.scope || ""}:${f.relative_path}`}
                          onClick={() => void handleOpenFsFile(f, root)}
                          className="w-full flex items-center justify-between gap-2 px-2 py-1.5 rounded-md text-left text-xs hover:bg-secondary/60 transition-colors"
                        >
                          <span className="flex items-center gap-1.5 truncate">
                            <FileText className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
                            <span className="truncate">{f.relative_path || f.name}</span>
                          </span>
                          {typeof f.size_bytes === "number" && (
                            <span className="text-[10px] text-muted-foreground shrink-0">
                              {Math.max(1, Math.round(f.size_bytes / 1024))} KB
                            </span>
                          )}
                        </button>
                      ))}
                    </div>
                  </div>
                );
              })
            )}
          </div>
        </Card>

        {/* Right: Clean Real-Time Promoted Model Chat */}
        <Card className="lg:col-span-8 p-3 flex flex-col h-[620px]">
          <div className="flex-1 overflow-y-auto space-y-2.5 p-2">
            {cleanMessages.length === 0 ? (
              <div className="h-full flex items-center justify-center text-xs text-muted-foreground">
                Send a prompt to interact with the promoted model slot.
              </div>
            ) : (
              cleanMessages.map((msg) => (
                <div
                  key={msg.id}
                  className={`flex flex-col rounded-xl px-3.5 py-2.5 text-sm ${
                    msg.role === "USER"
                      ? "ml-auto max-w-[82%] bg-primary text-primary-foreground"
                      : "mr-auto max-w-[88%] border border-border bg-secondary/40 text-foreground"
                  }`}
                >
                  <div className="flex items-center justify-between gap-4 mb-1 text-[10px] opacity-75">
                    <span className="font-semibold">{msg.role}</span>
                    <span>{msg.timestamp}</span>
                  </div>
                  <div className="whitespace-pre-wrap break-words leading-relaxed">{msg.text}</div>
                </div>
              ))
            )}
          </div>

          <div className="pt-2 border-t border-border flex items-center gap-2">
            <input
              type="text"
              data-testid="promoted-chat-input"
              value={chatDraft}
              onChange={(e) => setChatDraft(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter" && !e.shiftKey) {
                  e.preventDefault();
                  void handleSendChat();
                }
              }}
              placeholder={`Message promoted model (${activeAction})...`}
              className="flex-1 h-10 rounded-lg border border-border bg-background px-3 text-sm"
            />
            <Button
              data-testid="promoted-chat-send"
              disabled={sending || !chatDraft.trim()}
              onClick={() => void handleSendChat()}
            >
              <Send className="h-4 w-4 mr-1" />
              Send
            </Button>
          </div>
        </Card>
      </div>

      {/* File Inspector / Editor Modal */}
      {selectedFile && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4">
          <Card className="w-full max-w-3xl max-h-[85vh] flex flex-col p-4 space-y-3 bg-background">
            <div className="flex items-center justify-between border-b border-border pb-2">
              <div>
                <h2 className="text-sm font-bold">{selectedFile.name}</h2>
                <p className="text-xs text-muted-foreground">
                  {selectedFile.scope} · {selectedFile.path} · {selectedFile.mimeType}
                </p>
              </div>
              <div className="flex items-center gap-1.5">
                <a
                  href={`/api/benchmarks/promoted/fs/download?path=${encodeURIComponent(selectedFile.path)}&scope=${encodeURIComponent(selectedFile.scope)}`}
                  className="inline-flex items-center px-2.5 py-1 rounded-md border border-border text-xs hover:bg-secondary"
                >
                  <Download className="h-3.5 w-3.5 mr-1" />
                  Download
                </a>
                {selectedFile.scope === "workspace" && !selectedFile.isBinary && (
                  <Button size="sm" onClick={() => void handleSaveFsFile()}>
                    <Save className="h-3.5 w-3.5 mr-1" />
                    Save
                  </Button>
                )}
                {selectedFile.scope === "workspace" && (
                  <Button variant="outline" size="sm" onClick={() => void handleDeleteFsFile()}>
                    <Trash2 className="h-3.5 w-3.5 mr-1" />
                    Delete
                  </Button>
                )}
                <button
                  onClick={() => setSelectedFile(null)}
                  className="p-1 text-muted-foreground hover:text-foreground"
                >
                  <X className="h-4 w-4" />
                </button>
              </div>
            </div>

            <div className="flex-1 overflow-y-auto">
              {selectedFile.isMultimedia && selectedFile.dataUri ? (
                selectedFile.mimeType.startsWith("image/") ? (
                  <img src={selectedFile.dataUri} alt={selectedFile.name} className="max-h-[60vh] mx-auto rounded" />
                ) : (
                  <iframe src={selectedFile.dataUri} title={selectedFile.name} className="w-full h-[60vh] rounded border border-border" />
                )
              ) : (
                <textarea
                  value={editedContent}
                  onChange={(e) => setEditedContent(e.target.value)}
                  readOnly={selectedFile.scope !== "workspace" || selectedFile.isBinary}
                  rows={18}
                  className="w-full rounded-lg border border-border bg-secondary/30 p-3 font-mono text-xs"
                />
              )}
            </div>
          </Card>
        </div>
      )}
    </div>
  );
};
