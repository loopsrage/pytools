import React, { useCallback, useEffect, useState } from "react";
import { RefreshCw, GitBranch, Layers, Terminal } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";

export const StagingQdrantView: React.FC = () => {
  const [systemsData, setSystemsData] = useState<any>(null);
  const [juxQuery, setJuxQuery] = useState<string>("Spinor Strut Lattice");
  const [juxResult, setJuxResult] = useState<any>(null);
  const [mcpTools, setMcpTools] = useState<any[]>([]);
  const [busy, setBusy] = useState<boolean>(false);

  const loadSystems = useCallback(async () => {
    setBusy(true);
    try {
      const [sysRes, mcpRes] = await Promise.all([
        fetch("/api/qdrant/staging/systems"),
        fetch("/api/mcp/geodesic/tools"),
      ]);
      if (sysRes.ok) setSystemsData(await sysRes.json());
      if (mcpRes.ok) {
        const body = await mcpRes.json();
        setMcpTools(Array.isArray(body.tools) ? body.tools : []);
      }
    } finally {
      setBusy(false);
    }
  }, []);

  const runJuxtaposition = useCallback(async () => {
    if (!juxQuery.trim()) return;
    const res = await fetch(
      `/api/qdrant/staging/juxtaposition?query=${encodeURIComponent(juxQuery.trim())}`
    );
    if (res.ok) {
      setJuxResult(await res.json());
    }
  }, [juxQuery]);

  useEffect(() => {
    void loadSystems();
    void runJuxtaposition();
  }, [loadSystems, runJuxtaposition]);

  const handleLifecycle = async (action: string) => {
    setBusy(true);
    try {
      await fetch("/api/qdrant/staging/lifecycle", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action, system_id: "all" }),
      });
      await loadSystems();
    } finally {
      setBusy(false);
    }
  };

  const systems = Array.isArray(systemsData?.systems) ? systemsData.systems : [];

  return (
    <div
      data-testid="staging-qdrant-section"
      className="flex flex-col h-full gap-4 overflow-y-auto"
    >
      <Card className="p-4 flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="text-base font-semibold flex items-center gap-2">
            <Layers className="h-4 w-4 text-primary" />
            Unified Staging Qdrant &amp; Geodesic Lifecycle MCP
          </h2>
          <p className="text-xs text-muted-foreground font-mono mt-0.5">
            Endpoint: POST /api/mcp/geodesic • Registered Systems: {systems.length} • MCP Tools:{" "}
            {mcpTools.length}
          </p>
        </div>

        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            onClick={() => handleLifecycle("sync")}
            disabled={busy}
          >
            <RefreshCw className={`h-3.5 w-3.5 mr-1.5 ${busy ? "animate-spin" : ""}`} />
            Sync Staging
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={() => handleLifecycle("rebuild")}
            disabled={busy}
          >
            Rebuild Graph
          </Button>
        </div>
      </Card>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <Card className="p-4 space-y-3">
          <h3 className="text-sm font-semibold">Registered Qdrant Systems</h3>
          <div className="divide-y divide-border text-xs">
            {systems.map((sys: any) => (
              <div key={sys.system_id} className="py-2 flex items-center justify-between gap-2">
                <div>
                  <div className="font-mono font-semibold">{sys.system_id}</div>
                  <div className="text-muted-foreground">{sys.repo_path || sys.storage_path}</div>
                </div>
                <span className="font-mono px-2 py-0.5 rounded bg-secondary">
                  {sys.node_count ?? sys.collection_count ?? "ready"}
                </span>
              </div>
            ))}
          </div>
        </Card>

        <Card className="p-4 space-y-3">
          <h3 className="text-sm font-semibold flex items-center gap-1.5">
            <Terminal className="h-4 w-4" />
            Geodesic MCP Tools ({mcpTools.length})
          </h3>
          <div className="divide-y divide-border text-xs">
            {mcpTools.map((tool: any) => (
              <div key={tool.name} className="py-2">
                <div className="font-mono font-semibold text-primary">{tool.name}</div>
                <div className="text-muted-foreground line-clamp-2">{tool.description}</div>
              </div>
            ))}
          </div>
        </Card>
      </div>

      <Card className="p-4 space-y-3">
        <div className="flex items-center justify-between gap-2 flex-wrap">
          <h3 className="text-sm font-semibold flex items-center gap-1.5">
            <GitBranch className="h-4 w-4" />
            Design / Compiler / Code Geodesic Juxtaposition
          </h3>
          <div className="flex items-center gap-2">
            <input
              type="text"
              value={juxQuery}
              onChange={(e) => setJuxQuery(e.target.value)}
              className="h-8 w-64 rounded-md border border-border bg-background px-2.5 text-xs font-mono"
            />
            <Button size="sm" onClick={runJuxtaposition}>
              Query
            </Button>
          </div>
        </div>
        {juxResult && (
          <pre className="max-h-72 overflow-auto rounded-lg bg-muted/50 p-3 text-xs font-mono whitespace-pre-wrap">
            {JSON.stringify(juxResult, null, 2)}
          </pre>
        )}
      </Card>
    </div>
  );
};
