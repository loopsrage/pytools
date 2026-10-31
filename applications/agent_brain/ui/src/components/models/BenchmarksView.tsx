import React, { useCallback, useEffect, useMemo, useState } from "react";
import {
  Award,
  BookOpen,
  Check,
  Code2,
  Copy,
  FileSpreadsheet,
  Hammer,
  History,
  Layers,
  RefreshCw,
  Save,
  Search,
  Trophy,
  X,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";

interface BenchmarkEntryRow {
  benchmark_id: string;
  benchmark_name: string;
  benchmark_title?: string;
  category?: string;
  rank?: number;
  total_models?: number;
  primary_metric?: number;
  primary_metric_name?: string;
  secondary_metric?: number;
  secondary_metric_name?: string;
  tertiary_metric?: number;
  tertiary_metric_name?: string;
  higher_is_better?: boolean;
  elapsed_sec?: number;
  num_params?: number;
  delta_vs_best_pct?: number;
}

interface HistoricalRankPoint {
  run_id?: string;
  run_label?: string;
  commit_hash?: string;
  recorded_at?: string;
  wins_count?: number;
  mean_rank?: number;
  benchmarks_evaluated?: number;
  overall_rank?: number;
  composite_score?: number;
}

interface QdrantRejoinTelemetry {
  chunk_count?: number;
  chain_continuous?: boolean;
  integrity_verified?: boolean;
  rejoined_sha256?: string;
  expected_sha256?: string;
}

interface ModelProfile {
  model_id: string;
  model_name: string;
  friendly_title?: string;
  class_name?: string;
  family?: string;
  is_polyphonic?: boolean;
  is_overall_best?: boolean;
  is_promoted?: boolean;
  overall_rank?: number;
  composite_score?: number;
  mean_rank?: number;
  median_rank?: number;
  wins_count?: number;
  top3_count?: number;
  benchmarks_evaluated?: number;
  avg_num_params?: number;
  avg_elapsed_sec?: number;
  summary?: string;
  architectural_highlights?: string[];
  markdown_overview?: string;
  source_code?: string;
  source_file?: string;
  source_line_start?: number;
  source_line_end?: number;
  last_tested_at?: string;
  historical_ranks?: HistoricalRankPoint[];
  benchmark_entries?: BenchmarkEntryRow[];
  related_modules?: string[];
  related_kis?: string[];
  qdrant_doc_id?: string;
  qdrant_chunks_count?: number;
  qdrant_rejoin?: QdrantRejoinTelemetry;
}

interface BenchmarkDocument {
  doc_id: string;
  doc_type: string;
  title: string;
  friendly_title?: string;
  subtitle?: string;
  category?: string;
  summary?: string;
  overall_rank?: number;
  composite_score?: number;
  mean_rank?: number;
  wins_count?: number;
  top3_count?: number;
  best_model_name?: string;
  is_overall_best?: boolean;
  is_promoted?: boolean;
  last_tested_at?: string;
  payload?: Record<string, unknown>;
}

interface BenchmarksOverviewResponse {
  collection?: string;
  qdrant_status?: {
    collection_name?: string;
    total_chunks?: number;
    total_documents?: number;
  };
  total_models?: number;
  total_benchmarks?: number;
  total_runs?: number;
  language_benchmarks_count?: number;
  continuous_benchmarks_count?: number;
  promoted_model?: ModelProfile | null;
  best_model?: ModelProfile | null;
  models?: ModelProfile[];
  documents?: BenchmarkDocument[];
}

interface SlotMatrixRow {
  model_slot_id: string;
  model_class: string;
  harness: string;
  param_count: number;
  tests_evaluated: number;
  tests_passed: number;
  tests_learned: number;
  pass_rate: number;
  mean_accuracy: number;
  mean_test_loss: number;
  mean_coherent_io_ratio: number | null;
  total_train_seconds: number;
  latest_timestamp: string;
}

interface SlotTestRecord {
  point_id?: string;
  run_id?: string;
  run_timestamp?: string;
  model_slot_id: string;
  model_class: string;
  harness: string;
  test_id: string;
  benchmark?: string;
  dataset?: string;
  input_kind?: string;
  loss_type?: string;
  param_count?: number;
  epochs?: number;
  test_passed?: boolean;
  learned?: boolean;
  loss_before?: number;
  loss_after?: number;
  test_loss?: number;
  accuracy?: number;
  coherent_io_ratio?: number | null;
  train_seconds?: number;
  split_digest?: string;
}

interface SlotHarnessesResponse {
  collection: string;
  storage_path: string;
  available: boolean;
  total_points: number;
  unique_cells: number;
  slots: string[];
  harnesses: string[];
  datasets: string[];
  identical_splits_verified: boolean;
  summary_matrix: SlotMatrixRow[];
  records: SlotTestRecord[];
}

interface RecallSpecResponse {
  path: string;
  exists: boolean;
  size_bytes: number;
  line_count: number;
  preview_lines: string[];
}

type FilterTab =
  | "Models (Ranked)"
  | "All"
  | "Language Benchmarks"
  | "Continuous Dynamical Systems"
  | "Run History"
  | "4-Harness Slot Suite";

type ModalTab = "architecture" | "source" | "history" | "kis";

const FILTER_TABS: FilterTab[] = [
  "Models (Ranked)",
  "All",
  "Language Benchmarks",
  "Continuous Dynamical Systems",
  "Run History",
  "4-Harness Slot Suite",
];

function humanizeModelTitle(rawName?: string, className?: string, friendly?: string): string {
  if (friendly && friendly.trim()) return friendly.trim();
  const base = (rawName || "").split(" (")[0].trim() || className || "Architecture";
  return base
    .replace(/Model$/, "")
    .replace(/([a-z])([A-Z0-9])/g, "$1 $2")
    .replace(/([0-9])([A-Za-z])/g, "$1 $2")
    .trim();
}

function formatIsoShort(raw?: string): string {
  if (!raw) return "—";
  const parsed = Date.parse(raw);
  if (!Number.isFinite(parsed)) return raw.slice(0, 19).replace("T", " ");
  return new Date(parsed).toISOString().slice(0, 19).replace("T", " ") + " UTC";
}

function formatParams(numParams?: number): string {
  if (typeof numParams !== "number" || !Number.isFinite(numParams) || numParams <= 0) {
    return "—";
  }
  if (numParams >= 1_000_000) {
    return `${(numParams / 1_000_000).toFixed(2)}M`;
  }
  if (numParams >= 1_000) {
    return `${(numParams / 1_000).toFixed(1)}K`;
  }
  return String(Math.round(numParams));
}

function formatMetricNumber(val: unknown, digits = 4): string {
  if (typeof val !== "number" || !Number.isFinite(val)) return "—";
  if (Math.abs(val) > 0 && Math.abs(val) < 0.0001) return val.toExponential(2);
  return val.toFixed(digits);
}

export const BenchmarksView: React.FC = () => {
  const [overview, setOverview] = useState<BenchmarksOverviewResponse | null>(null);
  const [slotSuite, setSlotSuite] = useState<SlotHarnessesResponse | null>(null);
  const [recallSpec, setRecallSpec] = useState<RecallSpecResponse | null>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const [searchQuery, setSearchQuery] = useState<string>("");
  const [selectedFilter, setSelectedFilter] = useState<FilterTab>("Models (Ranked)");
  const [searchResults, setSearchResults] = useState<BenchmarkDocument[] | null>(null);
  const [building, setBuilding] = useState<boolean>(false);
  const [saving, setSaving] = useState<boolean>(false);
  const [statusBanner, setStatusBanner] = useState<string>("");

  const [slotFilter, setSlotFilter] = useState<string>("");
  const [harnessFilter, setHarnessFilter] = useState<string>("");
  const [datasetFilter, setDatasetFilter] = useState<string>("");

  const [selectedModel, setSelectedModel] = useState<ModelProfile | null>(null);
  const [modalTab, setModalTab] = useState<ModalTab>("architecture");
  const [copiedSource, setCopiedSource] = useState<boolean>(false);

  const loadSlotHarnesses = useCallback(
    async (slot = slotFilter, harness = harnessFilter, dataset = datasetFilter, query = searchQuery) => {
      const params = new URLSearchParams();
      if (slot) params.set("slot", slot);
      if (harness) params.set("harness", harness);
      if (dataset) params.set("dataset", dataset);
      if (query.trim()) params.set("query", query.trim());
      params.set("limit", "250");
      try {
        const res = await fetch(`/api/benchmarks/slot-harnesses?${params.toString()}`);
        if (res.ok) {
          setSlotSuite((await res.json()) as SlotHarnessesResponse);
        }
      } catch {
        // Non-fatal if staging collection is unavailable
      }
    },
    [datasetFilter, harnessFilter, searchQuery, slotFilter]
  );

  const loadOverview = useCallback(async () => {
    setLoading(true);
    try {
      const [ovRes, specRes] = await Promise.all([
        fetch("/api/benchmarks/overview"),
        fetch("/api/benchmarks/recall-spec?max_lines=40"),
      ]);
      if (ovRes.ok) {
        const data = (await ovRes.json()) as BenchmarksOverviewResponse;
        setOverview(data);
      }
      if (specRes.ok) {
        setRecallSpec((await specRes.json()) as RecallSpecResponse);
      }
      await loadSlotHarnesses();
    } finally {
      setLoading(false);
    }
  }, [loadSlotHarnesses]);

  useEffect(() => {
    void loadOverview();
  }, [loadOverview]);

  useEffect(() => {
    if (selectedFilter === "4-Harness Slot Suite") {
      void loadSlotHarnesses(slotFilter, harnessFilter, datasetFilter, searchQuery);
    }
  }, [datasetFilter, harnessFilter, loadSlotHarnesses, searchQuery, selectedFilter, slotFilter]);

  useEffect(() => {
    if (selectedFilter === "4-Harness Slot Suite") {
      return;
    }
    const trimmed = searchQuery.trim();
    if (!trimmed) {
      setSearchResults(null);
      return;
    }
    let cancelled = false;
    const timer = window.setTimeout(async () => {
      try {
        const res = await fetch(
          `/api/benchmarks/search?query=${encodeURIComponent(trimmed)}&top_k=100`
        );
        if (!res.ok) return;
        const data = await res.json();
        if (!cancelled) {
          setSearchResults(Array.isArray(data.results) ? data.results : []);
        }
      } catch {
        if (!cancelled) setSearchResults(null);
      }
    }, 180);
    return () => {
      cancelled = true;
      window.clearTimeout(timer);
    };
  }, [searchQuery, selectedFilter]);

  const bestModel = overview?.promoted_model || overview?.best_model || null;

  const allDocs: BenchmarkDocument[] = useMemo(() => {
    if (Array.isArray(overview?.documents) && overview!.documents.length > 0) {
      return overview!.documents;
    }
    return (overview?.models || []).map((m) => ({
      doc_id: m.model_id,
      doc_type: "benchmark_model",
      title: m.model_name,
      friendly_title: m.friendly_title,
      subtitle: m.family,
      summary: m.summary,
      overall_rank: m.overall_rank,
      composite_score: m.composite_score,
      mean_rank: m.mean_rank,
      wins_count: m.wins_count,
      top3_count: m.top3_count,
      is_overall_best: m.is_overall_best,
      is_promoted: m.is_promoted,
      last_tested_at: m.last_tested_at,
      payload: m as unknown as Record<string, unknown>,
    }));
  }, [overview]);

  const tabCounts = useMemo(() => {
    let models = 0;
    let lang = 0;
    let cont = 0;
    let runs = 0;
    for (const d of allDocs) {
      const plCat = String((d.payload || {}).category || "").toLowerCase();
      if (d.doc_type === "benchmark_model") models += 1;
      else if (
        d.doc_type === "benchmark_task" &&
        (d.category === "Language Benchmarks" || plCat === "language")
      ) {
        lang += 1;
      } else if (
        d.doc_type === "benchmark_task" &&
        (d.category === "Continuous Dynamical Systems" || plCat === "continuous")
      ) {
        cont += 1;
      } else if (d.doc_type === "benchmark_run") {
        runs += 1;
      }
    }
    return {
      "Models (Ranked)": models,
      All: allDocs.length,
      "Language Benchmarks": lang,
      "Continuous Dynamical Systems": cont,
      "Run History": runs,
      "4-Harness Slot Suite": slotSuite?.summary_matrix?.length ?? 0,
    } as Record<FilterTab, number>;
  }, [allDocs, slotSuite]);

  const filteredDocs = useMemo(() => {
    const base = searchResults !== null ? searchResults : allDocs;
    const seen = new Set<string>();
    return base
      .filter((doc) => {
        if (!doc?.doc_id || seen.has(doc.doc_id)) return false;
        const plCat = String((doc.payload || {}).category || "").toLowerCase();
        if (selectedFilter === "Models (Ranked)" && doc.doc_type !== "benchmark_model") {
          return false;
        }
        if (
          selectedFilter === "Language Benchmarks" &&
          !(
            doc.doc_type === "benchmark_task" &&
            (doc.category === "Language Benchmarks" || plCat === "language")
          )
        ) {
          return false;
        }
        if (
          selectedFilter === "Continuous Dynamical Systems" &&
          !(
            doc.doc_type === "benchmark_task" &&
            (doc.category === "Continuous Dynamical Systems" || plCat === "continuous")
          )
        ) {
          return false;
        }
        if (selectedFilter === "Run History" && doc.doc_type !== "benchmark_run") {
          return false;
        }
        seen.add(doc.doc_id);
        return true;
      })
      .sort((a, b) => {
        if (selectedFilter === "Run History") {
          const tsA = String((a.payload || {}).recorded_at || a.last_tested_at || "");
          const tsB = String((b.payload || {}).recorded_at || b.last_tested_at || "");
          return tsB.localeCompare(tsA);
        }
        const typeOrder: Record<string, number> = {
          benchmark_model: 0,
          benchmark_task: 1,
          benchmark_run: 2,
        };
        const tDiff = (typeOrder[a.doc_type] ?? 9) - (typeOrder[b.doc_type] ?? 9);
        if (tDiff !== 0) return tDiff;
        return (a.overall_rank ?? 999) - (b.overall_rank ?? 999);
      });
  }, [allDocs, searchResults, selectedFilter]);

  const handleInspectModel = async (modelIdOrName: string) => {
    const target = (modelIdOrName || "").trim();
    if (!target) return;
    const res = await fetch(`/api/benchmarks/models/${encodeURIComponent(target)}`);
    if (res.ok) {
      const detail = (await res.json()) as ModelProfile;
      setSelectedModel(detail);
      setModalTab("architecture");
      setCopiedSource(false);
    }
  };

  const handleCopySource = async () => {
    const code = selectedModel?.source_code || "";
    if (!code) return;
    try {
      await navigator.clipboard.writeText(code);
      setCopiedSource(true);
      window.setTimeout(() => setCopiedSource(false), 1800);
    } catch {
      setCopiedSource(false);
    }
  };

  const handleSaveSnapshot = async () => {
    setSaving(true);
    setStatusBanner("");
    try {
      const res = await fetch("/api/benchmarks/save", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ run_label: "Agent Brain UI Checkpoint" }),
      });
      if (res.ok) {
        setStatusBanner("Saved benchmark ledger snapshot to Qdrant.");
        await loadOverview();
      }
    } finally {
      setSaving(false);
    }
  };

  const handleBuildBenchmarks = async () => {
    setBuilding(true);
    setStatusBanner("");
    try {
      const res = await fetch("/api/benchmarks/build", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ include_git_history: true }),
      });
      if (res.ok) {
        setStatusBanner("Rebuilt neural benchmark index and synchronized Qdrant.");
        await loadOverview();
      }
    } finally {
      setBuilding(false);
    }
  };

  const collectionLabel =
    overview?.collection ||
    overview?.qdrant_status?.collection_name ||
    "portfolio_neural_benchmarks";

  const isTaskTab =
    selectedFilter === "Language Benchmarks" ||
    selectedFilter === "Continuous Dynamical Systems";
  const isRunTab = selectedFilter === "Run History";

  return (
    <div className="space-y-4" data-testid="neural-benchmarks-section">
      {/* Header & Actions */}
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h1 className="text-xl font-bold">Model Benchmarks</h1>
          <p className="text-xs text-muted-foreground">
            Qdrant: <span className="font-mono">{collectionLabel}</span> ·{" "}
            {overview?.total_models ?? tabCounts["Models (Ranked)"]} Architectures ·{" "}
            {overview?.total_benchmarks ?? 12} Canonical Tasks ·{" "}
            {overview?.total_runs ?? tabCounts["Run History"]} Recorded Runs
            {slotSuite?.available
              ? ` · ${slotSuite.total_points} Staged 4-Harness Slot Points`
              : ""}
          </p>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            disabled={saving}
            onClick={() => void handleSaveSnapshot()}
          >
            <Save className="h-3.5 w-3.5 mr-1" />
            {saving ? "Saving..." : "Save Run Snapshot"}
          </Button>
          <Button
            variant="outline"
            size="sm"
            disabled={building}
            onClick={() => void handleBuildBenchmarks()}
          >
            <Hammer className="h-3.5 w-3.5 mr-1" />
            {building ? "Building..." : "Scrape & Build"}
          </Button>
          <Button
            variant="ghost"
            size="sm"
            disabled={loading}
            onClick={() => void loadOverview()}
            aria-label="Refresh benchmarks"
          >
            <RefreshCw className={`h-3.5 w-3.5 ${loading ? "animate-spin" : ""}`} />
          </Button>
        </div>
      </div>

      {statusBanner && (
        <div className="rounded-lg border border-border bg-secondary/50 px-3 py-2 text-xs">
          {statusBanner}
        </div>
      )}

      {/* Pinned #1 ★ Promoted Model Summary Card */}
      {bestModel && (
        <Card
          className="p-4 border-primary/40 cursor-pointer hover:border-primary transition-colors"
          data-testid="best-model-hero-card"
          onClick={() => void handleInspectModel(bestModel.model_id)}
        >
          <div className="flex flex-wrap items-center justify-between gap-4">
            <div className="space-y-1 max-w-3xl">
              <div className="flex flex-wrap items-center gap-2">
                <span className="inline-flex items-center gap-1 rounded-full bg-primary px-2.5 py-0.5 text-[11px] font-semibold text-primary-foreground">
                  <Trophy className="h-3 w-3" />
                  #1 ★ Promoted Architecture
                </span>
                <span className="text-xs font-medium text-muted-foreground">
                  {bestModel.family}
                </span>
                <span className="text-[11px] font-mono text-muted-foreground">
                  Last Tested: {formatIsoShort(bestModel.last_tested_at)}
                </span>
              </div>
              <div className="flex flex-wrap items-baseline gap-2">
                <h2 className="text-lg font-bold">
                  {humanizeModelTitle(
                    bestModel.model_name,
                    bestModel.class_name,
                    bestModel.friendly_title
                  )}
                </h2>
                <span className="text-xs font-mono text-muted-foreground">
                  ({bestModel.class_name || bestModel.model_name})
                </span>
              </div>
              <p className="text-xs text-muted-foreground line-clamp-2">
                {bestModel.summary}
              </p>
            </div>

            <div className="flex flex-wrap items-center gap-4 text-right">
              <div>
                <div className="text-[11px] uppercase text-muted-foreground">Composite</div>
                <div className="text-base font-bold font-mono">
                  {typeof bestModel.composite_score === "number"
                    ? `${bestModel.composite_score.toFixed(2)} pts`
                    : "—"}
                </div>
              </div>
              <div>
                <div className="text-[11px] uppercase text-muted-foreground">Mean Rank</div>
                <div className="text-base font-bold font-mono">
                  {typeof bestModel.mean_rank === "number"
                    ? `#${bestModel.mean_rank.toFixed(2)}`
                    : "—"}
                </div>
              </div>
              <div>
                <div className="text-[11px] uppercase text-muted-foreground">Rank #1 Wins</div>
                <div className="text-base font-bold font-mono">
                  {bestModel.wins_count ?? 0} / {bestModel.benchmarks_evaluated ?? 12}
                </div>
              </div>
              <div>
                <div className="text-[11px] uppercase text-muted-foreground">Top-3 / Params</div>
                <div className="text-base font-bold font-mono">
                  {bestModel.top3_count ?? 0} · {formatParams(bestModel.avg_num_params)}
                </div>
              </div>
              <Button
                variant="outline"
                size="sm"
                onClick={(e) => {
                  e.stopPropagation();
                  void handleInspectModel(bestModel.model_id);
                }}
              >
                Inspect
              </Button>
            </div>
          </div>
        </Card>
      )}

      {/* Search & Category Filter Tabs */}
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex flex-wrap items-center gap-1.5">
          {FILTER_TABS.map((tab) => (
            <button
              key={tab}
              data-testid={`benchmark-tab-${tab}`}
              onClick={() => setSelectedFilter(tab)}
              className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-colors ${
                selectedFilter === tab
                  ? "bg-primary text-primary-foreground"
                  : "bg-secondary text-secondary-foreground hover:opacity-90"
              }`}
            >
              {tab}{" "}
              <span className="opacity-75 font-mono text-[11px]">({tabCounts[tab] ?? 0})</span>
            </button>
          ))}
        </div>

        <div className="relative w-full sm:w-72">
          <Search className="h-3.5 w-3.5 absolute left-3 top-1/2 -translate-y-1/2 text-muted-foreground" />
          <input
            type="text"
            data-testid="benchmarks-search-input"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            placeholder={
              selectedFilter === "4-Harness Slot Suite"
                ? "Filter slot, harness, dataset, or test..."
                : "Search Qdrant model benchmarks..."
            }
            className="w-full h-9 rounded-lg border border-border bg-background pl-8 pr-3 text-xs"
          />
        </div>
      </div>

      {/* View Mode A: 4-Harness Universal Model-Slot Benchmark Suite (Staging Qdrant `unified_harness_benchmarks`) */}
      {selectedFilter === "4-Harness Slot Suite" ? (
        <div className="space-y-4" data-testid="slot-harness-suite-panel">
          <Card className="p-3.5 space-y-3">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div className="space-y-0.5">
                <div className="flex items-center gap-2">
                  <Layers className="h-4 w-4 text-primary" />
                  <span className="text-sm font-bold">
                    Unified 4-Harness 32-Test Model-Slot Matrix
                  </span>
                  <span className="rounded bg-secondary px-2 py-0.5 text-[11px] font-mono">
                    {slotSuite?.collection || "unified_harness_benchmarks"}
                  </span>
                  {slotSuite?.identical_splits_verified && (
                    <span className="rounded bg-emerald-500/15 text-emerald-500 px-2 py-0.5 text-[11px] font-medium">
                      SHA-256 split_digest verified
                    </span>
                  )}
                </div>
                <p className="text-xs text-muted-foreground">
                  Evaluates slotted PyTorch architectures across{" "}
                  <span className="font-mono">linear_harness</span>,{" "}
                  <span className="font-mono">hyperbolic_harness</span>,{" "}
                  <span className="font-mono">corpus_harness</span>, and{" "}
                  <span className="font-mono">model_harness</span> on identical 80/20 held-out
                  splits ({slotSuite?.unique_cells ?? 0} unique cells ·{" "}
                  {slotSuite?.total_points ?? 0} persisted points).
                  {recallSpec?.exists
                    ? ` Recall Spec: benchmark_recall.yaml (${recallSpec.line_count.toLocaleString()} lines).`
                    : ""}
                </p>
              </div>

              <div className="flex flex-wrap items-center gap-2 text-xs">
                <select
                  aria-label="Filter model slot"
                  value={slotFilter}
                  onChange={(e) => setSlotFilter(e.target.value)}
                  className="h-8 rounded-md border border-border bg-background px-2 text-xs"
                >
                  <option value="">All Slots ({slotSuite?.slots?.length ?? 0})</option>
                  {(slotSuite?.slots || []).map((s) => (
                    <option key={s} value={s}>
                      {s}
                    </option>
                  ))}
                </select>
                <select
                  aria-label="Filter harness"
                  value={harnessFilter}
                  onChange={(e) => setHarnessFilter(e.target.value)}
                  className="h-8 rounded-md border border-border bg-background px-2 text-xs"
                >
                  <option value="">All Harnesses ({slotSuite?.harnesses?.length ?? 0})</option>
                  {(slotSuite?.harnesses || []).map((h) => (
                    <option key={h} value={h}>
                      {h}
                    </option>
                  ))}
                </select>
                <select
                  aria-label="Filter dataset"
                  value={datasetFilter}
                  onChange={(e) => setDatasetFilter(e.target.value)}
                  className="h-8 rounded-md border border-border bg-background px-2 text-xs"
                >
                  <option value="">All Datasets ({slotSuite?.datasets?.length ?? 0})</option>
                  {(slotSuite?.datasets || []).map((d) => (
                    <option key={d} value={d}>
                      {d}
                    </option>
                  ))}
                </select>
              </div>
            </div>

            <div className="overflow-x-auto border border-border rounded-lg">
              <table className="w-full text-left text-xs">
                <thead className="border-b border-border bg-secondary/40 text-muted-foreground uppercase">
                  <tr>
                    <th className="px-3 py-2">Slot ID</th>
                    <th className="px-3 py-2">Model Class</th>
                    <th className="px-3 py-2">Harness</th>
                    <th className="px-3 py-2">Passed / Tests</th>
                    <th className="px-3 py-2">Pass Rate</th>
                    <th className="px-3 py-2">Mean Acc</th>
                    <th className="px-3 py-2">Mean Test Loss</th>
                    <th className="px-3 py-2">Params</th>
                    <th className="px-3 py-2">Latest Run</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border">
                  {(slotSuite?.summary_matrix || []).map((row) => (
                    <tr
                      key={`${row.model_slot_id}::${row.harness}`}
                      onClick={() => {
                        setSlotFilter(row.model_slot_id);
                        setHarnessFilter(row.harness);
                      }}
                      className="cursor-pointer hover:bg-secondary/40 transition-colors"
                    >
                      <td className="px-3 py-2 font-mono font-semibold">{row.model_slot_id}</td>
                      <td className="px-3 py-2 font-mono text-muted-foreground">
                        {row.model_class}
                      </td>
                      <td className="px-3 py-2 font-mono">{row.harness}</td>
                      <td className="px-3 py-2 font-mono">
                        {row.tests_passed} / {row.tests_evaluated}
                      </td>
                      <td className="px-3 py-2 font-mono font-semibold">
                        {row.pass_rate.toFixed(1)}%
                      </td>
                      <td className="px-3 py-2 font-mono">
                        {(row.mean_accuracy * 100).toFixed(2)}%
                      </td>
                      <td className="px-3 py-2 font-mono">{row.mean_test_loss.toFixed(4)}</td>
                      <td className="px-3 py-2 font-mono">{formatParams(row.param_count)}</td>
                      <td className="px-3 py-2 text-muted-foreground">
                        {formatIsoShort(row.latest_timestamp)}
                      </td>
                    </tr>
                  ))}
                  {(slotSuite?.summary_matrix || []).length === 0 && (
                    <tr>
                      <td colSpan={9} className="px-4 py-6 text-center text-muted-foreground">
                        No staged 4-harness slot benchmark cells matched the current filter.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </Card>

          <Card className="overflow-x-auto">
            <div className="px-3.5 py-2.5 border-b border-border flex items-center justify-between text-xs">
              <span className="font-semibold flex items-center gap-1.5">
                <FileSpreadsheet className="h-3.5 w-3.5" />
                Held-Out Cell Executions ({slotSuite?.records?.length ?? 0} shown)
              </span>
              {(slotFilter || harnessFilter || datasetFilter) && (
                <button
                  onClick={() => {
                    setSlotFilter("");
                    setHarnessFilter("");
                    setDatasetFilter("");
                  }}
                  className="text-primary hover:underline"
                >
                  Clear cell filters
                </button>
              )}
            </div>
            <table className="w-full text-left text-xs">
              <thead className="border-b border-border bg-secondary/40 text-muted-foreground uppercase">
                <tr>
                  <th className="px-3 py-2">Slot</th>
                  <th className="px-3 py-2">Harness</th>
                  <th className="px-3 py-2">Dataset / Test</th>
                  <th className="px-3 py-2">Status</th>
                  <th className="px-3 py-2">Loss (Before → After)</th>
                  <th className="px-3 py-2">Test Loss</th>
                  <th className="px-3 py-2">Accuracy</th>
                  <th className="px-3 py-2">Split Digest</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border">
                {(slotSuite?.records || []).slice(0, 120).map((rec, idx) => (
                  <tr key={rec.point_id || `${rec.model_slot_id}-${rec.harness}-${rec.test_id}-${idx}`}>
                    <td className="px-3 py-2 font-mono font-semibold">{rec.model_slot_id}</td>
                    <td className="px-3 py-2 font-mono">{rec.harness}</td>
                    <td className="px-3 py-2">
                      <span className="font-mono">{rec.test_id}</span>
                      <span className="ml-1.5 text-[11px] text-muted-foreground">
                        ({rec.loss_type})
                      </span>
                    </td>
                    <td className="px-3 py-2 font-mono">
                      <span
                        className={`rounded px-1.5 py-0.5 text-[11px] ${
                          rec.test_passed
                            ? "bg-emerald-500/15 text-emerald-500"
                            : "bg-amber-500/15 text-amber-500"
                        }`}
                      >
                        {rec.test_passed ? "PASS" : "HOLD"}
                      </span>
                    </td>
                    <td className="px-3 py-2 font-mono text-muted-foreground">
                      {formatMetricNumber(rec.loss_before, 4)} →{" "}
                      {formatMetricNumber(rec.loss_after, 4)}
                    </td>
                    <td className="px-3 py-2 font-mono">{formatMetricNumber(rec.test_loss, 4)}</td>
                    <td className="px-3 py-2 font-mono">
                      {typeof rec.accuracy === "number"
                        ? `${(rec.accuracy * 100).toFixed(2)}%`
                        : "—"}
                    </td>
                    <td className="px-3 py-2 font-mono text-[11px] text-muted-foreground">
                      {(rec.split_digest || "").slice(0, 12) || "—"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </Card>
        </div>
      ) : (
        /* View Mode B: Canonical 25-Model 12-Benchmark Leaderboard, Tasks & Run History */
        <Card className="overflow-x-auto">
          <table className="w-full text-left text-xs">
            <thead className="border-b border-border bg-secondary/40 text-muted-foreground uppercase">
              {isTaskTab ? (
                <tr>
                  <th className="px-3 py-2.5">Benchmark Task</th>
                  <th className="px-3 py-2.5">Domain</th>
                  <th className="px-3 py-2.5">#1 Winner Architecture</th>
                  <th className="px-3 py-2.5">Primary Metric</th>
                  <th className="px-3 py-2.5">Secondary Metric</th>
                  <th className="px-3 py-2.5">Win Margin</th>
                  <th className="px-3 py-2.5">Models</th>
                </tr>
              ) : isRunTab ? (
                <tr>
                  <th className="px-3 py-2.5">Run Label & Commit</th>
                  <th className="px-3 py-2.5">Overall Best Model</th>
                  <th className="px-3 py-2.5">Best Mean Rank</th>
                  <th className="px-3 py-2.5">Best Wins</th>
                  <th className="px-3 py-2.5">Suite Scope</th>
                  <th className="px-3 py-2.5">Recorded (UTC)</th>
                </tr>
              ) : (
                <tr>
                  <th className="px-3 py-2.5">Rank</th>
                  <th className="px-3 py-2.5">Architecture / Entry</th>
                  <th className="px-3 py-2.5">Family / Category</th>
                  <th className="px-3 py-2.5">Composite</th>
                  <th className="px-3 py-2.5">Mean Rank</th>
                  <th className="px-3 py-2.5">Wins</th>
                  <th className="px-3 py-2.5">Top-3</th>
                  <th className="px-3 py-2.5">Params</th>
                  <th className="px-3 py-2.5">Last Tested</th>
                </tr>
              )}
            </thead>
            <tbody className="divide-y divide-border">
              {filteredDocs.map((doc) => {
                const pl = (doc.payload || {}) as Record<string, unknown>;
                if (isTaskTab) {
                  const winnerId = String(pl.best_model_id || pl.best_model_name || doc.best_model_name || "");
                  const winnerName = String(pl.best_model_name || doc.best_model_name || "—");
                  const lb = Array.isArray(pl.leaderboard) ? pl.leaderboard : [];
                  const margin =
                    typeof pl.relative_margin_pct === "number" ? pl.relative_margin_pct : null;
                  return (
                    <tr
                      key={doc.doc_id}
                      data-testid={`benchmark-card-${doc.doc_id}`}
                      onClick={() => {
                        if (winnerId) void handleInspectModel(winnerId);
                      }}
                      className="cursor-pointer hover:bg-secondary/40 transition-colors"
                    >
                      <td className="px-3 py-2.5">
                        <div className="font-semibold text-foreground">
                          {String(pl.title || doc.title)}
                        </div>
                        <div className="text-[11px] font-mono text-muted-foreground">
                          {String(pl.name || doc.doc_id)}
                        </div>
                      </td>
                      <td className="px-3 py-2.5 text-muted-foreground">
                        {String(pl.domain_label || doc.category || "—")}
                      </td>
                      <td className="px-3 py-2.5 font-semibold">
                        {humanizeModelTitle(winnerName)}
                        <div className="text-[11px] font-mono text-muted-foreground">
                          {winnerName}
                        </div>
                      </td>
                      <td className="px-3 py-2.5 font-mono">
                        <span className="text-muted-foreground">
                          {String(pl.primary_metric_name || "Metric")}:{" "}
                        </span>
                        <span className="font-semibold">
                          {formatMetricNumber(pl.best_primary_metric)}
                        </span>
                      </td>
                      <td className="px-3 py-2.5 font-mono">
                        {pl.secondary_metric_name ? (
                          <>
                            <span className="text-muted-foreground">
                              {String(pl.secondary_metric_name)}:{" "}
                            </span>
                            <span>{formatMetricNumber(pl.best_secondary_metric)}</span>
                          </>
                        ) : (
                          "—"
                        )}
                      </td>
                      <td className="px-3 py-2.5 font-mono">
                        {margin !== null ? `+${margin.toFixed(2)}%` : "—"}
                      </td>
                      <td className="px-3 py-2.5 font-mono">{lb.length || "—"}</td>
                    </tr>
                  );
                }

                if (isRunTab) {
                  const bestRunModel = String(pl.overall_best_model || doc.best_model_name || "");
                  return (
                    <tr
                      key={doc.doc_id}
                      data-testid={`benchmark-card-${doc.doc_id}`}
                      onClick={() => {
                        if (bestRunModel) void handleInspectModel(bestRunModel);
                      }}
                      className="cursor-pointer hover:bg-secondary/40 transition-colors"
                    >
                      <td className="px-3 py-2.5">
                        <div className="font-semibold text-foreground">
                          {String(pl.run_label || doc.title)}
                        </div>
                        <div className="text-[11px] font-mono text-muted-foreground">
                          {String(pl.commit_hash || "head")} · {String(pl.source_type || "ledger")}
                        </div>
                      </td>
                      <td className="px-3 py-2.5 font-semibold">
                        {humanizeModelTitle(bestRunModel)}
                        <div className="text-[11px] font-mono text-muted-foreground">
                          {bestRunModel || "—"}
                        </div>
                      </td>
                      <td className="px-3 py-2.5 font-mono">
                        {typeof pl.overall_best_model_mean_rank === "number"
                          ? `#${pl.overall_best_model_mean_rank.toFixed(2)}`
                          : "—"}
                      </td>
                      <td className="px-3 py-2.5 font-mono">
                        {String(pl.overall_best_model_wins ?? "—")} /{" "}
                        {String(pl.total_benchmarks ?? 12)}
                      </td>
                      <td className="px-3 py-2.5 font-mono text-muted-foreground">
                        {String(pl.total_models ?? "—")} models ·{" "}
                        {String(pl.epochs ?? "—")} ep · {String(pl.device || "cpu")}
                      </td>
                      <td className="px-3 py-2.5 text-muted-foreground">
                        {formatIsoShort(String(pl.recorded_at || doc.last_tested_at || ""))}
                      </td>
                    </tr>
                  );
                }

                const isModel = doc.doc_type === "benchmark_model";
                const avgParams =
                  typeof pl.avg_num_params === "number" ? pl.avg_num_params : undefined;
                const evalCount =
                  typeof pl.benchmarks_evaluated === "number"
                    ? pl.benchmarks_evaluated
                    : undefined;
                const inspectTarget = isModel
                  ? doc.doc_id
                  : String(pl.best_model_id || pl.best_model_name || pl.overall_best_model || "");

                return (
                  <tr
                    key={doc.doc_id}
                    data-testid={`benchmark-card-${doc.doc_id}`}
                    onClick={() => {
                      if (inspectTarget) void handleInspectModel(inspectTarget);
                    }}
                    className={`${
                      inspectTarget ? "cursor-pointer hover:bg-secondary/40" : ""
                    } transition-colors`}
                  >
                    <td className="px-3 py-2.5 font-bold font-mono">
                      {doc.overall_rank && doc.overall_rank < 900
                        ? `#${doc.overall_rank}${
                            doc.is_promoted || doc.is_overall_best ? " ★" : ""
                          }`
                        : doc.doc_type === "benchmark_task"
                        ? "TASK"
                        : "RUN"}
                    </td>
                    <td className="px-3 py-2.5">
                      <div className="font-semibold text-foreground">
                        {isModel
                          ? humanizeModelTitle(
                              doc.title,
                              String(pl.class_name || ""),
                              doc.friendly_title
                            )
                          : doc.title}
                      </div>
                      <div className="text-[11px] text-muted-foreground font-mono">
                        {String(pl.class_name || doc.title)}
                      </div>
                    </td>
                    <td className="px-3 py-2.5 text-muted-foreground">
                      {String(pl.family || doc.category || doc.subtitle || doc.doc_type)}
                    </td>
                    <td className="px-3 py-2.5 font-mono">
                      {isModel && typeof doc.composite_score === "number"
                        ? doc.composite_score.toFixed(2)
                        : "—"}
                    </td>
                    <td className="px-3 py-2.5 font-mono">
                      {isModel && typeof doc.mean_rank === "number"
                        ? `#${doc.mean_rank.toFixed(2)}`
                        : "—"}
                    </td>
                    <td className="px-3 py-2.5 font-mono">
                      {isModel
                        ? `${doc.wins_count ?? 0}${evalCount ? ` / ${evalCount}` : ""}`
                        : "—"}
                    </td>
                    <td className="px-3 py-2.5 font-mono">
                      {isModel ? doc.top3_count ?? "—" : "—"}
                    </td>
                    <td className="px-3 py-2.5 font-mono">{formatParams(avgParams)}</td>
                    <td className="px-3 py-2.5 text-muted-foreground">
                      {formatIsoShort(doc.last_tested_at)}
                    </td>
                  </tr>
                );
              })}
              {filteredDocs.length === 0 && (
                <tr>
                  <td colSpan={9} className="px-4 py-8 text-center text-muted-foreground">
                    No benchmark documents matched the current filter.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </Card>
      )}

      {/* 4-Tab Model Architecture Inspector Modal */}
      {selectedModel && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4"
          data-testid="benchmark-model-modal"
          onClick={() => setSelectedModel(null)}
        >
          <Card
            className="w-full max-w-5xl max-h-[88vh] flex flex-col p-4 space-y-3 bg-background"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-start justify-between gap-4 border-b border-border pb-2.5">
              <div>
                <div className="flex flex-wrap items-center gap-2">
                  <h2 className="text-base font-bold">
                    {humanizeModelTitle(
                      selectedModel.model_name,
                      selectedModel.class_name,
                      selectedModel.friendly_title
                    )}
                  </h2>
                  <span className="rounded bg-secondary px-2 py-0.5 text-[11px] font-mono">
                    Rank #{selectedModel.overall_rank ?? "—"}
                  </span>
                  {selectedModel.family && (
                    <span className="text-xs text-muted-foreground">{selectedModel.family}</span>
                  )}
                </div>
                <p className="text-xs text-muted-foreground font-mono mt-0.5">
                  {selectedModel.class_name || selectedModel.model_id} ·{" "}
                  {typeof selectedModel.composite_score === "number"
                    ? `${selectedModel.composite_score.toFixed(2)} pts`
                    : "—"}{" "}
                  · Mean #{selectedModel.mean_rank?.toFixed(2) ?? "—"} · Wins{" "}
                  {selectedModel.wins_count ?? 0}/{selectedModel.benchmarks_evaluated ?? 12} ·{" "}
                  {formatParams(selectedModel.avg_num_params)} params
                </p>
              </div>
              <button
                onClick={() => setSelectedModel(null)}
                aria-label="Close modal"
                className="p-1 text-muted-foreground hover:text-foreground"
              >
                <X className="h-4 w-4" />
              </button>
            </div>

            <div className="flex flex-wrap items-center gap-1.5 border-b border-border pb-2">
              <button
                onClick={() => setModalTab("architecture")}
                className={`inline-flex items-center gap-1.5 px-3 py-1 rounded-md text-xs font-medium ${
                  modalTab === "architecture"
                    ? "bg-primary text-primary-foreground"
                    : "bg-secondary text-secondary-foreground"
                }`}
              >
                <Award className="h-3.5 w-3.5" />
                Architecture & Benchmarks ({selectedModel.benchmark_entries?.length ?? 0})
              </button>
              <button
                onClick={() => setModalTab("source")}
                className={`inline-flex items-center gap-1.5 px-3 py-1 rounded-md text-xs font-medium ${
                  modalTab === "source"
                    ? "bg-primary text-primary-foreground"
                    : "bg-secondary text-secondary-foreground"
                }`}
              >
                <Code2 className="h-3.5 w-3.5" />
                PyTorch Source
              </button>
              <button
                onClick={() => setModalTab("history")}
                className={`inline-flex items-center gap-1.5 px-3 py-1 rounded-md text-xs font-medium ${
                  modalTab === "history"
                    ? "bg-primary text-primary-foreground"
                    : "bg-secondary text-secondary-foreground"
                }`}
              >
                <History className="h-3.5 w-3.5" />
                Historical Progression ({selectedModel.historical_ranks?.length ?? 0})
              </button>
              <button
                onClick={() => setModalTab("kis")}
                className={`inline-flex items-center gap-1.5 px-3 py-1 rounded-md text-xs font-medium ${
                  modalTab === "kis"
                    ? "bg-primary text-primary-foreground"
                    : "bg-secondary text-secondary-foreground"
                }`}
              >
                <BookOpen className="h-3.5 w-3.5" />
                Toolset & Skill KIs
              </button>
            </div>

            <div className="flex-1 overflow-y-auto space-y-3">
              {modalTab === "architecture" && (
                <div className="space-y-3 text-xs">
                  {selectedModel.summary && (
                    <p className="text-muted-foreground leading-relaxed">{selectedModel.summary}</p>
                  )}

                  {Array.isArray(selectedModel.architectural_highlights) &&
                    selectedModel.architectural_highlights.length > 0 && (
                      <div className="rounded-lg border border-border bg-secondary/20 p-3 space-y-1.5">
                        <div className="font-semibold text-foreground">
                          Architectural Invariants & Highlights
                        </div>
                        <ul className="list-disc pl-4 space-y-1 text-muted-foreground">
                          {selectedModel.architectural_highlights.map((hl, i) => (
                            <li key={i}>{hl}</li>
                          ))}
                        </ul>
                      </div>
                    )}

                  {Array.isArray(selectedModel.benchmark_entries) &&
                  selectedModel.benchmark_entries.length > 0 ? (
                    <div className="border border-border rounded-lg overflow-x-auto">
                      <table className="w-full text-left text-xs">
                        <thead className="bg-secondary/40 border-b border-border uppercase text-muted-foreground">
                          <tr>
                            <th className="px-3 py-2">Rank</th>
                            <th className="px-3 py-2">Benchmark Task</th>
                            <th className="px-3 py-2">Domain</th>
                            <th className="px-3 py-2">Primary Metric</th>
                            <th className="px-3 py-2">Secondary / Tertiary</th>
                            <th className="px-3 py-2">Params</th>
                            <th className="px-3 py-2">Elapsed</th>
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-border">
                          {selectedModel.benchmark_entries.map((be, i) => (
                            <tr key={be.benchmark_id || be.benchmark_name || i}>
                              <td className="px-3 py-1.5 font-mono font-bold">
                                #{be.rank ?? "—"}
                                {be.total_models ? (
                                  <span className="font-normal text-muted-foreground">
                                    /{be.total_models}
                                  </span>
                                ) : null}
                              </td>
                              <td className="px-3 py-1.5">
                                <div className="font-medium">
                                  {be.benchmark_title || be.benchmark_name}
                                </div>
                                <div className="font-mono text-[11px] text-muted-foreground">
                                  {be.benchmark_name}
                                </div>
                              </td>
                              <td className="px-3 py-1.5 capitalize text-muted-foreground">
                                {be.category || "—"}
                              </td>
                              <td className="px-3 py-1.5 font-mono">
                                <span className="text-muted-foreground">
                                  {be.primary_metric_name || "Primary"}:{" "}
                                </span>
                                <span className="font-semibold">
                                  {formatMetricNumber(be.primary_metric)}
                                </span>
                              </td>
                              <td className="px-3 py-1.5 font-mono text-muted-foreground">
                                {be.secondary_metric_name
                                  ? `${be.secondary_metric_name}: ${formatMetricNumber(
                                      be.secondary_metric
                                    )}`
                                  : "—"}
                                {be.tertiary_metric_name
                                  ? ` · ${be.tertiary_metric_name}: ${formatMetricNumber(
                                      be.tertiary_metric,
                                      2
                                    )}`
                                  : ""}
                              </td>
                              <td className="px-3 py-1.5 font-mono">
                                {formatParams(be.num_params)}
                              </td>
                              <td className="px-3 py-1.5 font-mono text-muted-foreground">
                                {typeof be.elapsed_sec === "number"
                                  ? `${be.elapsed_sec.toFixed(1)}s`
                                  : "—"}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <p className="text-muted-foreground">
                      No per-benchmark task entries recorded for this architecture.
                    </p>
                  )}
                </div>
              )}

              {modalTab === "source" && (
                <div className="space-y-2">
                  <div className="flex flex-wrap items-center justify-between gap-2 text-xs">
                    <span className="font-mono text-muted-foreground">
                      {selectedModel.source_file
                        ? `${selectedModel.source_file}${
                            selectedModel.source_line_start
                              ? ` (L${selectedModel.source_line_start}–L${
                                  selectedModel.source_line_end ?? selectedModel.source_line_start
                                })`
                              : ""
                          }`
                        : "AST-extracted PyTorch source"}
                    </span>
                    <Button
                      variant="outline"
                      size="sm"
                      disabled={!selectedModel.source_code}
                      onClick={() => void handleCopySource()}
                    >
                      {copiedSource ? (
                        <>
                          <Check className="h-3.5 w-3.5 mr-1" />
                          Copied
                        </>
                      ) : (
                        <>
                          <Copy className="h-3.5 w-3.5 mr-1" />
                          Copy Code
                        </>
                      )}
                    </Button>
                  </div>
                  <pre className="rounded-lg border border-border bg-secondary/30 p-3 font-mono text-xs overflow-x-auto whitespace-pre">
                    {selectedModel.source_code ||
                      "# No PyTorch class source recorded for this entry."}
                  </pre>
                </div>
              )}

              {modalTab === "history" && (
                <div className="space-y-1.5 text-xs">
                  {(selectedModel.historical_ranks || []).length === 0 ? (
                    <p className="text-muted-foreground">
                      No historical ledger checkpoints recorded.
                    </p>
                  ) : (
                    (selectedModel.historical_ranks || []).map((pt, idx) => (
                      <div
                        key={`${pt.run_id || "run"}-${idx}`}
                        className="flex flex-wrap items-center justify-between gap-2 rounded border border-border px-3 py-2"
                      >
                        <div>
                          <div className="font-semibold">
                            {pt.run_label || pt.run_id || "Benchmark Run"}
                          </div>
                          <div className="text-muted-foreground font-mono text-[11px]">
                            {pt.commit_hash || "head"} · {formatIsoShort(pt.recorded_at)}
                          </div>
                        </div>
                        <div className="text-right font-mono">
                          <div>
                            Rank #1 Wins: {pt.wins_count ?? 0} / {pt.benchmarks_evaluated ?? 12}
                          </div>
                          <div className="text-muted-foreground">
                            Mean Rank:{" "}
                            {typeof pt.mean_rank === "number"
                              ? `#${pt.mean_rank.toFixed(2)}`
                              : pt.overall_rank
                              ? `#${pt.overall_rank}`
                              : "—"}
                          </div>
                        </div>
                      </div>
                    ))
                  )}
                </div>
              )}

              {modalTab === "kis" && (
                <div className="space-y-3 text-xs">
                  <div className="rounded-lg border border-border p-3 space-y-2">
                    <div className="font-semibold">Related Toolset Modules</div>
                    <div className="flex flex-wrap gap-1.5">
                      {(selectedModel.related_modules || []).length > 0 ? (
                        (selectedModel.related_modules || []).map((mod) => (
                          <span
                            key={mod}
                            className="rounded bg-secondary px-2 py-1 font-mono text-[11px]"
                          >
                            {mod}
                          </span>
                        ))
                      ) : (
                        <span className="text-muted-foreground">None linked</span>
                      )}
                    </div>
                  </div>

                  <div className="rounded-lg border border-border p-3 space-y-2">
                    <div className="font-semibold">Related Knowledge Items (KIs)</div>
                    <div className="flex flex-wrap gap-1.5">
                      {(selectedModel.related_kis || []).length > 0 ? (
                        (selectedModel.related_kis || []).map((ki) => (
                          <span
                            key={ki}
                            className="rounded bg-secondary px-2 py-1 font-mono text-[11px]"
                          >
                            {ki}
                          </span>
                        ))
                      ) : (
                        <span className="text-muted-foreground">None linked</span>
                      )}
                    </div>
                  </div>

                  <div className="rounded-lg border border-border p-3 space-y-1 font-mono text-[11px]">
                    <div className="font-sans font-semibold text-xs">
                      Qdrant Chunk Chain & SHA-256 Rejoin Integrity
                    </div>
                    <div>Document ID: {selectedModel.qdrant_doc_id || selectedModel.model_id}</div>
                    <div>Chunks Rejoined: {selectedModel.qdrant_chunks_count ?? 1}</div>
                    <div>
                      Chain Continuous:{" "}
                      {String(selectedModel.qdrant_rejoin?.chain_continuous ?? true)}
                    </div>
                    <div>
                      SHA-256 Verified:{" "}
                      {String(selectedModel.qdrant_rejoin?.integrity_verified ?? true)}
                    </div>
                    {selectedModel.qdrant_rejoin?.rejoined_sha256 && (
                      <div className="text-muted-foreground truncate">
                        SHA-256: {selectedModel.qdrant_rejoin.rejoined_sha256}
                      </div>
                    )}
                  </div>
                </div>
              )}
            </div>
          </Card>
        </div>
      )}
    </div>
  );
};
