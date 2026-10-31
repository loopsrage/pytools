import React, { useCallback, useEffect, useState } from "react";
import { Download, FileText, Search } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";

interface BrainArtifactItem {
  artifact_id?: string;
  name: string;
  title?: string;
  path: string;
  relative_path?: string;
  conversation_id?: string;
  friendly_session_name?: string;
  size_bytes?: number;
  updated_at?: string;
  snippet?: string;
  content?: string;
}

interface BrainArtifactsResponse {
  total_artifacts: number;
  artifacts: BrainArtifactItem[];
  conversations?: Array<{ conversation_id: string; friendly_name?: string; count?: number }>;
}

const TIME_RANGES = [
  { value: "all", label: "All Time" },
  { value: "1h", label: "Last 1h" },
  { value: "6h", label: "Last 6h" },
  { value: "24h", label: "Last 24h" },
  { value: "7d", label: "Last 7d" },
  { value: "30d", label: "Last 30d" },
];

export const BrainArtifactsView: React.FC = () => {
  const [query, setQuery] = useState<string>("");
  const [timeRange, setTimeRange] = useState<string>("all");
  const [conversationId, setConversationId] = useState<string>("All");
  const [catalog, setCatalog] = useState<BrainArtifactsResponse | null>(null);
  const [selected, setSelected] = useState<BrainArtifactItem | null>(null);
  const [fileContent, setFileContent] = useState<string>("");
  const [loading, setLoading] = useState<boolean>(false);

  const loadCatalog = useCallback(async () => {
    setLoading(true);
    try {
      const params = new URLSearchParams({
        query: query.trim(),
        time_range: timeRange,
      });
      if (conversationId !== "All") {
        params.set("conversation_id", conversationId);
      }
      const res = await fetch(`/api/antigravity/artifacts?${params.toString()}`);
      if (!res.ok) return;
      const data = (await res.json()) as BrainArtifactsResponse;
      setCatalog(data);
      if (Array.isArray(data.artifacts) && data.artifacts.length > 0) {
        setSelected((prev) => {
          if (prev) {
            const match = data.artifacts.find((a) => a.path === prev.path);
            if (match) return match;
          }
          return data.artifacts[0];
        });
      } else {
        setSelected(null);
        setFileContent("");
      }
    } finally {
      setLoading(false);
    }
  }, [conversationId, query, timeRange]);

  useEffect(() => {
    void loadCatalog();
  }, [loadCatalog]);

  useEffect(() => {
    if (!selected?.path) {
      setFileContent("");
      return;
    }
    if (selected.content) {
      setFileContent(selected.content);
      return;
    }
    let cancelled = false;
    fetch(`/api/antigravity/files/content?path=${encodeURIComponent(selected.path)}`)
      .then(async (res) => {
        if (!res.ok) return;
        const body = await res.json();
        if (!cancelled) {
          setFileContent(String(body.content || ""));
        }
      })
      .catch(() => {});
    return () => {
      cancelled = true;
    };
  }, [selected]);

  const artifacts = catalog?.artifacts || [];

  return (
    <div
      data-testid="brain-artifacts-section"
      className="flex flex-col h-full gap-3 min-h-0"
    >
      <Card className="p-3 flex flex-wrap items-center justify-between gap-2">
        <div className="flex items-center gap-2 flex-1 min-w-[220px]">
          <Search className="h-4 w-4 text-muted-foreground shrink-0" />
          <input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Filter brain artifacts by title, filename, or session..."
            className="w-full bg-transparent text-sm outline-none"
          />
        </div>

        <div className="flex items-center gap-2">
          <select
            value={conversationId}
            onChange={(e) => setConversationId(e.target.value)}
            aria-label="Conversation filter"
            className="h-8 rounded-md border border-border bg-background px-2 text-xs"
          >
            <option value="All">All Sessions</option>
            {(catalog?.conversations || []).map((c) => (
              <option key={c.conversation_id} value={c.conversation_id}>
                {c.friendly_name || c.conversation_id}
              </option>
            ))}
          </select>

          <select
            value={timeRange}
            onChange={(e) => setTimeRange(e.target.value)}
            aria-label="Artifact time range"
            className="h-8 rounded-md border border-border bg-background px-2 text-xs"
          >
            {TIME_RANGES.map((opt) => (
              <option key={opt.value} value={opt.value}>
                {opt.label}
              </option>
            ))}
          </select>
        </div>
      </Card>

      <div className="flex-1 grid grid-cols-1 lg:grid-cols-12 gap-3 min-h-0">
        <Card className="lg:col-span-4 flex flex-col min-h-0 overflow-hidden">
          <div className="px-3 py-2 border-b border-border text-xs font-semibold text-muted-foreground">
            Artifacts ({artifacts.length})
          </div>
          <div className="flex-1 overflow-y-auto divide-y divide-border">
            {loading && artifacts.length === 0 ? (
              <p className="p-3 text-xs text-muted-foreground">Loading artifacts...</p>
            ) : artifacts.length === 0 ? (
              <p className="p-3 text-xs text-muted-foreground">No brain artifacts found.</p>
            ) : (
              artifacts.map((art) => {
                const isSelected = selected?.path === art.path;
                return (
                  <button
                    key={art.path}
                    onClick={() => setSelected(art)}
                    className={`w-full text-left p-3 transition-colors ${
                      isSelected ? "bg-primary/10" : "hover:bg-accent/50"
                    }`}
                  >
                    <div className="flex items-center gap-2 font-medium text-sm truncate">
                      <FileText className="h-4 w-4 shrink-0 text-primary" />
                      <span className="truncate">{art.title || art.name}</span>
                    </div>
                    <div className="mt-1 text-xs text-muted-foreground truncate font-mono">
                      {art.friendly_session_name || art.conversation_id || art.relative_path}
                    </div>
                  </button>
                );
              })
            )}
          </div>
        </Card>

        <Card className="lg:col-span-8 flex flex-col min-h-0 overflow-hidden">
          {selected ? (
            <>
              <div className="px-4 py-2.5 border-b border-border flex items-center justify-between gap-2">
                <div className="min-w-0">
                  <h3 className="font-semibold text-sm truncate">
                    {selected.title || selected.name}
                  </h3>
                  <p className="text-xs text-muted-foreground font-mono truncate">
                    {selected.relative_path || selected.path}
                  </p>
                </div>
                <Button asChild variant="outline" size="sm">
                  <a
                    href={`/api/antigravity/files/download?path=${encodeURIComponent(
                      selected.path
                    )}`}
                    download={selected.name}
                  >
                    <Download className="h-3.5 w-3.5 mr-1.5" />
                    Download
                  </a>
                </Button>
              </div>
              <pre className="flex-1 overflow-auto p-4 text-xs font-mono whitespace-pre-wrap leading-relaxed">
                {fileContent || "(Empty artifact)"}
              </pre>
            </>
          ) : (
            <div className="flex-1 flex items-center justify-center text-sm text-muted-foreground">
              Select an artifact to inspect its contents.
            </div>
          )}
        </Card>
      </div>
    </div>
  );
};
