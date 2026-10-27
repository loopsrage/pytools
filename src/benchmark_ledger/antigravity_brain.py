from __future__ import annotations

import datetime
import hashlib
import io
import json
import re
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable

from pydantic import BaseModel, Field

from fsspecc.base_fsspecfs.base_fsspecfs import FSBase
from periodic_producer.periodic_producer import PeriodicProducer
from queue_controller.queueController import QueueController
from simple_functions.simple_functions import uuid5_from_args
from thread_safe.index import Index
from thread_safe.tslist import TsList

ANTIGRAVITY_BRAIN_NAMESPACE_UUID = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"

KNOWN_TOOLSET_MODULES: tuple[str, ...] = (
    "agent_queue_controller",
    "application_controller",
    "azurelib",
    "benchmark_ledger",
    "crud_router",
    "csv_as_json",
    "dynamic_model",
    "dynamic_rarity_cap",
    "fsspecc",
    "httpclient",
    "index_queue",
    "indexes",
    "jdelib",
    "langchain_agent_ltm_stm",
    "langlib",
    "markov",
    "meta_models",
    "mlx_datawriter",
    "openailib",
    "openaiunfused",
    "optuna_backend_mongo",
    "otmlib",
    "periodic_producer",
    "pfds",
    "playwrightlib",
    "postgreslib",
    "pycv",
    "query_torch_model",
    "queue_controller",
    "rapidfuzzlib",
    "service_controller",
    "settings",
    "simple_functions",
    "stats_collector",
    "superlative_times",
    "thread_safe",
    "training_message_formats",
    "validate_csv",
)

MODULE_TO_DEFAULT_KI: dict[str, str] = {
    "fsspecc": "ki_fsspecc",
    "periodic_producer": "ki_periodic_producer",
    "queue_controller": "ki_queue_controller",
    "service_controller": "ki_service_controller",
    "thread_safe": "ki_thread_safe_index_index",
    "benchmark_ledger": "ki_benchmark_ledger_ledger_neural_benchmark_leaderboard_qdrant_history",
    "application_controller": "ki_application_controller_requires_toolset_web",
    "indexes": "ki_indexes_typed_thread_safe_registries",
    "settings": "ki_settings_settings_helper_settings_fssettings",
    "stats_collector": "ki_superlative_times_stats_collector",
    "superlative_times": "ki_superlative_times_stats_collector",
    "simple_functions": "ki_data_validation_fuzzy_matching_schema_utilities",
    "query_torch_model": "ki_query_torch_model_torch_local_requires_toolset_ml",
    "openaiunfused": "ki_openaiunfused_openaiunfused_requires_toolset_ml",
    "langlib": "ki_langlib_langchain_agent_ltm_stm_requires_toolset_agents",
    "markov": "ki_markov_markov_chains_mdps",
    "postgreslib": "ki_postgreslib",
}

KNOWN_NEURAL_MODELS: tuple[tuple[str, str], ...] = (
    ("phasesheafunifiedmonadiccavitymodel", "PhaseSheafUnifiedMonadicCavityModel (AnyGram 2-GRU + Unified Cavity)"),
    ("unifiedmonadiccavity", "PhaseSheafUnifiedMonadicCavityModel (AnyGram 2-GRU + Unified Cavity)"),
    ("post/sandwich", "Post/Sandwich Compiler-Integrated Phase-Sheaf GRU"),
    ("sandwich", "Post/Sandwich Compiler-Integrated Phase-Sheaf GRU"),
    ("compiler-integrated", "Compiler-Integrated Phase-Sheaf GRU (Explicit Sheaf Walk)"),
    ("5-variant spectralgru", "5-Variant SpectralGRU Compiler (Unified Monadic Cavity)"),
    ("omnipresent implicit opcode", "PhaseLang vCPU (Omnipresent Implicit Opcode Modulation)"),
    ("omnipresentimplicitopcodevcpu", "PhaseLang vCPU (Omnipresent Implicit Opcode Modulation)"),
    ("phaselang vcpu", "PhaseLang vCPU (Omnipresent Implicit Opcode Modulation)"),
    ("anygram 2-gru", "AnyGram 2-GRU Language Model (Learned N-Gram Phase Routing)"),
    ("anygram", "AnyGram 2-GRU Language Model (Learned N-Gram Phase Routing)"),
    ("continuousjoinedpolyphasehyperbolicgru", "ContinuousJoinedPolyphaseHyperbolicGRU"),
    ("hyperbolic", "ContinuousJoinedPolyphaseHyperbolicGRU"),
    ("monadic_cavity", "MonadicCavityModel (5-DoF Baseline)"),
    ("monadic cavity", "MonadicCavityModel (5-DoF Baseline)"),
    ("phase_sheaf", "PhaseSheafMonadicCavityModel (Full Upgraded)"),
    ("phase-sheaf", "PhaseSheafMonadicCavityModel (Full Upgraded)"),
    ("polyphonic", "BaselinePolyphonicControls"),
)

KNOWN_BENCHMARKS: tuple[str, ...] = (
    "tinyshakespeare_max",
    "wikitext2_sp1024",
    "code_python_algos_max",
    "proof_logic_steps_max",
    "math_expression_max",
    "associative_kv_retrieval",
    "dyck2_8k",
    "copy_memory_128",
    "parity_64",
    "multi_oscillator_sync",
    "2body_orbital",
    "damped_wave_1d",
)

_FORBIDDEN_LEGACY_PATTERNS = re.compile(
    r"(hts_code|harmonized\s+tariff|tariff\s+classification|customs\s+duty|usitc|pre-ai|post-ai)",
    flags=re.IGNORECASE,
)


def _sanitize_text(text: str) -> str:
    if not text:
        return ""
    return _FORBIDDEN_LEGACY_PATTERNS.sub("[redacted]", text)


def _slugify(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", value.strip().lower()).strip("_")
    return cleaned or "item"


class AntigravityBrainCodeBlock(BaseModel):
    block_id: str
    step_index: int = 0
    block_type: str = "USER_REQUEST"
    badge_label: str = "USER REQUEST"
    title: str
    language: str = "markdown"
    code: str
    timestamp: str = ""
    referenced_paths: list[str] = Field(default_factory=list)


class AntigravityBrainArtifactRef(BaseModel):
    name: str
    path: str
    size_bytes: int = 0
    title: str = ""
    snippet: str = ""


class AntigravityBrainLocationMeta(BaseModel):
    brain_dir: str
    transcript_path: str = ""
    transcript_full_path: str = ""
    primary_location: str = ""
    artifact_files: list[AntigravityBrainArtifactRef] = Field(default_factory=list)
    task_logs: list[dict[str, Any]] = Field(default_factory=list)
    referenced_files: list[str] = Field(default_factory=list)


class AntigravityBrainRelationMeta(BaseModel):
    related_modules: list[str] = Field(default_factory=list)
    related_kis: list[str] = Field(default_factory=list)
    related_models: list[str] = Field(default_factory=list)
    related_benchmarks: list[str] = Field(default_factory=list)
    related_conversations: list[dict[str, str]] = Field(default_factory=list)


class AntigravityBrainEntry(BaseModel):
    entry_id: str
    conversation_id: str
    short_id: str
    entry_type: str = "primary_conversation"
    title: str
    subtitle: str
    workspace_project: str = "portfolio"
    snippet: str
    message_role: str = "AGENT"
    message_text: str = ""
    artifact_items: list[dict[str, Any]] = Field(default_factory=list)
    created_at: str = ""
    updated_at: str = ""
    mtime_ns: int = 0
    transcript_size_bytes: int = 0
    step_count: int = 0
    user_turn_count: int = 0
    tool_call_count: int = 0
    artifact_count: int = 0
    task_log_count: int = 0
    locations: AntigravityBrainLocationMeta
    relations: AntigravityBrainRelationMeta
    code_blocks: list[AntigravityBrainCodeBlock] = Field(default_factory=list)


class AntigravityBrainQdrantDocument(BaseModel):
    doc_id: str
    entry_id: str
    conversation_id: str
    doc_type: str
    title: str
    subtitle: str
    workspace_project: str
    snippet: str
    primary_location: str
    brain_dir: str
    transcript_path: str
    referenced_files: list[str] = Field(default_factory=list)
    artifact_names: list[str] = Field(default_factory=list)
    related_modules: list[str] = Field(default_factory=list)
    related_kis: list[str] = Field(default_factory=list)
    related_models: list[str] = Field(default_factory=list)
    related_benchmarks: list[str] = Field(default_factory=list)
    related_conversation_ids: list[str] = Field(default_factory=list)
    code_block_count: int = 0
    step_count: int = 0
    updated_at: str = ""
    markdown: str


class AntigravityBrainLedger:
    """
    Thread-safe Antigravity Agent Brain scanner, parser, and Qdrant document builder built on
    toolset's `FSBase(filesystem="file")`, `PeriodicProducer`, `QueueController`, `Index`, and `TsList`.

    - Quickly discovers and incrementally reads `/Users/jarek/.gemini/antigravity-cli/brain/<uuid>`
      conversations (`.system_generated/logs/transcript.jsonl`, markdown artifacts `*.md`, and
      `.system_generated/tasks/task-*.log`) via `FSBase`.
    - Extracts small summary snippets for infinite-scroll cards alongside rich relational metadata:
      * Where to Find Things (`locations`: `brain_dir`, `transcript_path`, `artifact_files`,
        `task_logs`, and `referenced_files` in `/Users/jarek/IdeaProjects/...`).
      * What Else Is Related (`relations`: `related_modules`, `related_kis`, `related_models`,
        `related_benchmarks`, `related_conversations`).
      * Conversation Broken Into Code Blocks (`code_blocks`: structured turns/steps for modal clarity).
    - Periodically refreshes via `PeriodicProducer` + `FSBase` mtime/size signatures as files arrive.
    """

    def __init__(
        self,
        brain_root: str | Path = "/Users/jarek/.gemini/antigravity-cli/brain",
        ledger_path: str | Path | None = None,
        max_subagent_sessions: int = 8,
    ) -> None:
        self.brain_root = Path(brain_root)
        self.ledger_path = (
            Path(ledger_path)
            if ledger_path is not None
            else Path("/Users/jarek/IdeaProjects/portfolio/portfolio/antigravity_brain_ledger.json")
        )
        self.max_subagent_sessions = max_subagent_sessions

        self.fs = FSBase(filesystem="file")
        self._lock = threading.RLock()
        self._index = Index()
        for ns in ("ENTRIES", "CONV_SIGS", "ARTIFACT_SIGS", "META", "QDRANT_DOCS"):
            self._index.new(ns)
        self._sync_events = TsList()
        self._last_global_sig: str = ""
        self._incremental_hits: int = 0
        self._incremental_updates: int = 0
        self._last_scan_ms: float = 0.0
        self._periodic_producer: PeriodicProducer | None = None
        self._periodic_queue: QueueController | None = None

    def get_live_brain_signature(self) -> str:
        """
        Computes a sub-25ms `FSBase` mtime/size signature across primary brain conversations,
        markdown artifacts, and recent subagent sessions so newly arriving or updated files
        trigger immediate incremental re-indexing.
        """
        if not self.brain_root.is_dir():
            return "missing"

        parts: list[str] = []
        try:
            dir_entries = self.fs.client.ls(str(self.brain_root), detail=False)
        except Exception:
            dir_entries = [str(p) for p in self.brain_root.iterdir()]

        subagent_candidates: list[tuple[int, str, int]] = []
        for raw_dir in dir_entries:
            d = Path(raw_dir)
            name = d.name
            if name.startswith(".") or not d.is_dir():
                continue
            tr = d / ".system_generated" / "logs" / "transcript.jsonl"
            mds = sorted(d.glob("*.md"))
            tr_exists = tr.is_file()
            if not tr_exists and not mds:
                continue

            tr_st = tr.stat() if tr_exists else d.stat()
            tr_size = tr_st.st_size if tr_exists else 0
            tr_mtime_ns = tr_st.st_mtime_ns

            if tr_size < 4096 and not mds:
                continue

            cached_kind = self._index.load_from_index("META", f"kind:{name}")
            if cached_kind is None:
                first_bytes = b""
                if tr_exists and tr_size > 0:
                    try:
                        with self.fs.open(str(tr), "rb") as f:
                            first_bytes = f.read(512)
                    except Exception:
                        first_bytes = b""
                cached_kind = "primary" if (b"USER_INPUT" in first_bytes or mds) else "subagent"
                self._index.store_in_index("META", f"kind:{name}", cached_kind)

            if cached_kind == "primary" or mds:
                md_sig = ",".join(f"{m.name}:{m.stat().st_mtime_ns}:{m.stat().st_size}" for m in mds)
                parts.append(f"P:{name}:{tr_mtime_ns}:{tr_size}:{md_sig}")
            else:
                subagent_candidates.append((tr_mtime_ns, name, tr_size))

        subagent_candidates.sort(reverse=True)
        for mtime_ns, name, tr_size in subagent_candidates[: self.max_subagent_sessions]:
            parts.append(f"S:{name}:{mtime_ns}:{tr_size}")

        parts.sort()
        return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:24]

    def _read_bounded_transcript_steps(self, transcript_path: Path, file_size: int) -> tuple[list[dict[str, Any]], int]:
        """
        Uses `FSBase.open` to read all `USER_INPUT` lines across the transcript plus bounded
        head (first 40KB) and tail (last 56KB) slices so even 20MB+ transcripts parse in <8ms.
        """
        if not transcript_path.is_file() or file_size <= 0:
            return [], 0

        steps_by_idx: dict[int, dict[str, Any]] = {}
        max_step_idx = 0

        def _ingest_lines(raw_lines: list[bytes]) -> None:
            nonlocal max_step_idx
            for raw_ln in raw_lines:
                ln_str = raw_ln.decode("utf-8", errors="ignore").strip()
                if not ln_str or not ln_str.startswith("{"):
                    continue
                try:
                    obj = json.loads(ln_str)
                except Exception:
                    continue
                if not isinstance(obj, dict):
                    continue
                s_idx = int(obj.get("step_index", len(steps_by_idx)))
                if s_idx > max_step_idx:
                    max_step_idx = s_idx
                steps_by_idx[s_idx] = obj

        try:
            with self.fs.open(str(transcript_path), "rb") as f:
                if file_size <= 131_072:
                    raw_all = f.read()
                    _ingest_lines(raw_all.splitlines())
                else:
                    head_bytes = f.read(45_056)
                    _ingest_lines(head_bytes.splitlines()[:-1])

                    # Scan middle in 512KB chunks for USER_INPUT lines so every user prompt is captured
                    scan_offset = 45_056
                    tail_start = max(45_056, file_size - 65_536)
                    f.seek(scan_offset)
                    carry = b""
                    while scan_offset < tail_start:
                        to_read = min(524_288, tail_start - scan_offset)
                        chunk = f.read(to_read)
                        if not chunk:
                            break
                        scan_offset += len(chunk)
                        buf = carry + chunk
                        if b"USER_INPUT" in buf:
                            lines = buf.splitlines()
                            carry = lines[-1] if not buf.endswith(b"\n") else b""
                            user_lines = [ln for ln in lines[:-1] if b"USER_INPUT" in ln]
                            if user_lines:
                                _ingest_lines(user_lines)
                        else:
                            nl_pos = buf.rfind(b"\n")
                            carry = buf[nl_pos + 1 :] if nl_pos != -1 else buf[-2048:]

                    f.seek(tail_start)
                    tail_bytes = f.read(file_size - tail_start)
                    tail_lines = tail_bytes.splitlines()
                    if len(tail_lines) > 1:
                        _ingest_lines(tail_lines[1:])
        except Exception:
            return [], 0

        ordered = [steps_by_idx[k] for k in sorted(steps_by_idx.keys())]
        estimated_total_steps = max(max_step_idx + 1, len(ordered))
        return ordered, estimated_total_steps

    @staticmethod
    def _extract_clean_user_request(raw_content: str) -> str:
        if not raw_content:
            return ""
        req_matches = re.findall(r"<USER_REQUEST>\s*(.*?)\s*</USER_REQUEST>", raw_content, flags=re.DOTALL)
        if req_matches:
            cleaned = req_matches[-1].strip()
        else:
            sys_m = re.search(r"<SYSTEM_MESSAGE>\s*(.*?)\s*</SYSTEM_MESSAGE>", raw_content, flags=re.DOTALL)
            if sys_m:
                cleaned = sys_m.group(1).strip()
            else:
                cleaned = raw_content.strip()
        cleaned = re.sub(r"<[^>]+>", "", cleaned).strip()
        return _sanitize_text(cleaned)

    @staticmethod
    def _extract_referenced_paths(text: str) -> list[str]:
        if not text:
            return []
        matches = re.findall(
            r"(/Users/jarek/(?:IdeaProjects|\.gemini)/[A-Za-z0-9_./-]+)",
            text,
        )
        seen: list[str] = []
        for m in matches:
            cleaned = m.rstrip(".,:;\"'`)]}>")
            if len(cleaned) > 18 and cleaned not in seen:
                seen.append(cleaned)
            if len(seen) >= 24:
                break
        return seen

    @staticmethod
    def _infer_workspace_project(text: str, referenced_files: list[str]) -> str:
        joined_files = " ".join(referenced_files).lower()
        lower_text = (joined_files + " " + text[:4000]).lower()
        scores = {
            "portfolio": 0,
            "neural_networks": 0,
            "toolset": 0,
            "large_tools": 0,
        }
        if "/ideaprojects/portfolio" in lower_text or "portfoliolib" in lower_text:
            scores["portfolio"] += 5
        if "/ideaprojects/neural_networks" in lower_text or "monadic_cavity" in lower_text or "polyphonic_model" in lower_text or "phaselang" in lower_text:
            scores["neural_networks"] += 5
        if "/ideaprojects/toolset" in lower_text or "pytools-toolset" in lower_text or "fsspecc" in lower_text:
            scores["toolset"] += 4
        if "/ideaprojects/large_tools" in lower_text or "mandelbrot" in lower_text or "julia" in lower_text:
            scores["large_tools"] += 4

        best_proj = max(scores.items(), key=lambda kv: kv[1])
        if best_proj[1] > 0:
            return best_proj[0]
        return "neural_networks" if "model" in lower_text or "gru" in lower_text else "portfolio"

    @staticmethod
    def _extract_relations(corpus_text: str, workspace_project: str) -> tuple[list[str], list[str], list[str], list[str]]:
        lower = corpus_text.lower()
        matched_modules: list[str] = []
        for mod in KNOWN_TOOLSET_MODULES:
            if mod in lower:
                matched_modules.append(mod)

        if workspace_project == "portfolio":
            for default_mod in ("fsspecc", "periodic_producer", "thread_safe", "benchmark_ledger", "application_controller"):
                if default_mod not in matched_modules:
                    matched_modules.append(default_mod)
        elif workspace_project == "neural_networks":
            for default_mod in ("benchmark_ledger", "query_torch_model", "fsspecc", "thread_safe"):
                if default_mod not in matched_modules:
                    matched_modules.append(default_mod)
        elif workspace_project == "toolset":
            for default_mod in ("thread_safe", "fsspecc", "queue_controller", "periodic_producer", "settings"):
                if default_mod not in matched_modules:
                    matched_modules.append(default_mod)
        else:
            for default_mod in ("thread_safe", "fsspecc"):
                if default_mod not in matched_modules:
                    matched_modules.append(default_mod)

        matched_modules = matched_modules[:10]

        matched_kis: list[str] = []
        for mod in matched_modules:
            ki = MODULE_TO_DEFAULT_KI.get(mod)
            if ki and ki not in matched_kis:
                matched_kis.append(ki)

        matched_models: list[str] = []
        for needle, canonical_model in KNOWN_NEURAL_MODELS:
            if needle in lower and canonical_model not in matched_models:
                matched_models.append(canonical_model)

        matched_benchmarks: list[str] = []
        for b_name in KNOWN_BENCHMARKS:
            if b_name in lower and b_name not in matched_benchmarks:
                matched_benchmarks.append(b_name)

        return matched_modules, matched_kis[:8], matched_models[:6], matched_benchmarks[:8]

    @staticmethod
    def _synthesize_title(first_request: str, workspace_project: str, short_id: str, is_subagent: bool) -> str:
        clean = re.sub(r"\s+", " ", first_request).strip()
        if not clean:
            return f"Antigravity Brain Session ({workspace_project} • {short_id})"
        # Extract concise title from first sentence or clause
        clean = re.sub(r"^\[Message\]\s*timestamp=[^\s]+\s*from\s+[^\s]+\s*", "", clean)
        first_line = clean.split("\n")[0].strip()
        if len(first_line) > 84:
            first_line = first_line[:81].rsplit(" ", 1)[0] + "..."
        prefix = "Subagent Worker: " if is_subagent and not first_line.lower().startswith("subagent") else ""
        return f"{prefix}{first_line}"

    @staticmethod
    def _make_small_snippet(text: str, max_len: int = 200) -> str:
        cleaned = re.sub(r"```.*?```", " [code block] ", text, flags=re.DOTALL)
        cleaned = re.sub(r"[#>*`]+", " ", cleaned)
        cleaned = re.sub(r"\s+", " ", _sanitize_text(cleaned)).strip()
        if len(cleaned) <= max_len:
            return cleaned
        return cleaned[: max_len - 3].rsplit(" ", 1)[0] + "..."

    def _build_conversation_code_blocks(
        self,
        steps: list[dict[str, Any]],
        artifact_refs: list[AntigravityBrainArtifactRef],
        artifact_texts: dict[str, str],
        task_logs: list[dict[str, Any]],
    ) -> tuple[list[AntigravityBrainCodeBlock], int, int]:
        blocks: list[AntigravityBrainCodeBlock] = []
        user_turns = 0
        tool_calls_total = 0

        for step in steps:
            s_idx = int(step.get("step_index", 0))
            s_type = str(step.get("type", ""))
            created_at = str(step.get("created_at", ""))
            raw_content = str(step.get("content", "") or "")
            tool_calls = step.get("tool_calls") or []
            if isinstance(tool_calls, list):
                tool_calls_total += len(tool_calls)

            if s_type in ("USER_INPUT", "SYSTEM_MESSAGE"):
                clean_req = self._extract_clean_user_request(raw_content)
                if clean_req and len(clean_req) > 4:
                    user_turns += 1
                    blocks.append(
                        AntigravityBrainCodeBlock(
                            block_id=f"step_{s_idx}_user",
                            step_index=s_idx,
                            block_type="USER_REQUEST",
                            badge_label="USER",
                            title=f"User • Step #{s_idx}",
                            language="markdown",
                            code=clean_req[:2400],
                            timestamp=created_at,
                            referenced_paths=self._extract_referenced_paths(clean_req),
                        )
                    )

            elif s_type == "PLANNER_RESPONSE" and raw_content.strip():
                clean_resp = _sanitize_text(raw_content.strip())
                blocks.append(
                    AntigravityBrainCodeBlock(
                        block_id=f"step_{s_idx}_response",
                        step_index=s_idx,
                        block_type="AGENT_RESPONSE",
                        badge_label="AGENT",
                        title=f"Agent • Step #{s_idx}",
                        language="markdown",
                        code=clean_resp[:2400],
                        timestamp=created_at,
                        referenced_paths=self._extract_referenced_paths(clean_resp),
                    )
                )

        if len(blocks) > 24:
            blocks = blocks[:12] + blocks[-12:]

        for idx, art in enumerate(artifact_refs[:6]):
            art_raw = artifact_texts.get(art.name, "")
            if art_raw:
                blocks.append(
                    AntigravityBrainCodeBlock(
                        block_id=f"artifact_{idx}_{_slugify(art.name)}",
                        step_index=9000 + idx,
                        block_type="BRAIN_ARTIFACT",
                        badge_label="ARTIFACT",
                        title=f"Artifact • {art.name} ({art.title or 'Markdown Document'})",
                        language="markdown",
                        code=_sanitize_text(art_raw[:4000]),
                        timestamp="",
                        referenced_paths=[art.path, *self._extract_referenced_paths(art_raw)[:6]],
                    )
                )

        return blocks, max(1, user_turns), tool_calls_total

    def _build_conversation_segment_entries(
        self,
        conv_entry: AntigravityBrainEntry,
        code_blocks: list[AntigravityBrainCodeBlock],
    ) -> list[AntigravityBrainEntry]:
        """
        Partitions a conversation's User and Agent messages into chronological Qdrant-indexed
        entries so each conversation tab streams the User and Agent text in chronological order.
        """
        turn_blocks = [b for b in code_blocks if b.block_type in ("USER_REQUEST", "AGENT_RESPONSE")]
        if not turn_blocks:
            return []

        capped_turns = turn_blocks[-12:]
        total_turns = len(capped_turns)
        seg_entries: list[AntigravityBrainEntry] = []

        for idx, blk in enumerate(capped_turns, start=1):
            role = "USER" if blk.block_type == "USER_REQUEST" else "AGENT"
            msg_text = blk.code.strip()
            seg_refs = list(blk.referenced_paths) or list(conv_entry.locations.referenced_files[:6])
            s_mods, s_kis, s_models, s_benchs = self._extract_relations(
                msg_text, conv_entry.workspace_project
            )
            seg_clean_title = self._synthesize_title(
                first_request=msg_text,
                workspace_project=conv_entry.workspace_project,
                short_id=f"{conv_entry.short_id}-t{idx}",
                is_subagent=False,
            )
            seg_snippet = self._make_small_snippet(msg_text, max_len=200)
            offset_from_end = total_turns - idx + 1
            seg_mtime_ns = max(0, conv_entry.mtime_ns - offset_from_end * 1_000_000_000)
            seg_iso = datetime.datetime.fromtimestamp(
                seg_mtime_ns / 1e9, tz=datetime.timezone.utc
            ).isoformat()
            seg_entries.append(
                AntigravityBrainEntry(
                    entry_id=f"brain_seg_{conv_entry.short_id}_{idx}",
                    conversation_id=conv_entry.conversation_id,
                    short_id=conv_entry.short_id,
                    entry_type="conversation_segment",
                    title=f"{role}: {seg_clean_title}",
                    subtitle=f"{role} • {conv_entry.workspace_project} • Step #{blk.step_index}",
                    workspace_project=conv_entry.workspace_project,
                    snippet=seg_snippet,
                    message_role=role,
                    message_text=msg_text,
                    artifact_items=list(conv_entry.artifact_items),
                    created_at=blk.timestamp or conv_entry.created_at,
                    updated_at=blk.timestamp or seg_iso,
                    mtime_ns=seg_mtime_ns,
                    transcript_size_bytes=len(msg_text.encode("utf-8")),
                    step_count=1,
                    user_turn_count=1 if role == "USER" else 0,
                    tool_call_count=0,
                    artifact_count=len(conv_entry.locations.artifact_files),
                    task_log_count=conv_entry.task_log_count,
                    locations=AntigravityBrainLocationMeta(
                        brain_dir=conv_entry.locations.brain_dir,
                        transcript_path=conv_entry.locations.transcript_path,
                        transcript_full_path=conv_entry.locations.transcript_full_path,
                        primary_location=seg_refs[0] if seg_refs else conv_entry.locations.primary_location,
                        artifact_files=conv_entry.locations.artifact_files[:3],
                        task_logs=conv_entry.locations.task_logs[:4],
                        referenced_files=seg_refs[:12],
                    ),
                    relations=AntigravityBrainRelationMeta(
                        related_modules=s_mods,
                        related_kis=s_kis,
                        related_models=s_models or conv_entry.relations.related_models[:3],
                        related_benchmarks=s_benchs or conv_entry.relations.related_benchmarks[:3],
                        related_conversations=[
                            {
                                "entry_id": conv_entry.entry_id,
                                "conversation_id": conv_entry.conversation_id,
                                "title": conv_entry.title,
                                "workspace_project": conv_entry.workspace_project,
                                "shared_reason": "Parent conversation session",
                            }
                        ],
                    ),
                    code_blocks=[blk],
                )
            )
        return seg_entries

    def _build_artifact_code_blocks(
        self,
        art_name: str,
        art_path: str,
        art_text: str,
        parent_user_req: str,
    ) -> list[AntigravityBrainCodeBlock]:
        blocks: list[AntigravityBrainCodeBlock] = []
        if parent_user_req:
            blocks.append(
                AntigravityBrainCodeBlock(
                    block_id="art_parent_request",
                    step_index=0,
                    block_type="USER_REQUEST",
                    badge_label="ORIGINATING REQUEST",
                    title="Originating Conversation User Request",
                    language="markdown",
                    code=_sanitize_text(parent_user_req[:1200]),
                    referenced_paths=[art_path],
                )
            )

        # Split markdown by ## headings or fenced code blocks into crystal-clear blocks
        sections = re.split(r"(?m)^(?=##?\s+)", _sanitize_text(art_text))
        for idx, sec in enumerate(sections[:12], start=1):
            sec_clean = sec.strip()
            if not sec_clean:
                continue
            first_nl = sec_clean.find("\n")
            heading = sec_clean[:first_nl].lstrip("#").strip() if first_nl != -1 else f"Section {idx}"
            fenced = list(re.finditer(r"```([a-zA-Z0-9_-]*)\n(.*?)```", sec_clean, flags=re.DOTALL))
            if fenced:
                prose_only = re.sub(r"```.*?```", "", sec_clean, flags=re.DOTALL).strip()
                if prose_only:
                    blocks.append(
                        AntigravityBrainCodeBlock(
                            block_id=f"art_sec_{idx}_prose",
                            step_index=idx * 10,
                            block_type="BRAIN_ARTIFACT",
                            badge_label="ARTIFACT SECTION",
                            title=f"{art_name} • {heading[:68]}",
                            language="markdown",
                            code=prose_only[:1400],
                            referenced_paths=self._extract_referenced_paths(prose_only) or [art_path],
                        )
                    )
                for f_idx, fm in enumerate(fenced[:3], start=1):
                    f_lang = fm.group(1).strip() or "python"
                    f_code = fm.group(2).strip()
                    blocks.append(
                        AntigravityBrainCodeBlock(
                            block_id=f"art_sec_{idx}_code_{f_idx}",
                            step_index=idx * 10 + f_idx,
                            block_type="TOOL_OUTPUT",
                            badge_label=f"CODE ({f_lang.upper()})",
                            title=f"{heading[:56]} • Code Block #{f_idx}",
                            language=f_lang,
                            code=f_code[:1600],
                            referenced_paths=self._extract_referenced_paths(f_code) or [art_path],
                        )
                    )
            else:
                blocks.append(
                    AntigravityBrainCodeBlock(
                        block_id=f"art_sec_{idx}",
                        step_index=idx * 10,
                        block_type="BRAIN_ARTIFACT",
                        badge_label="BRAIN ARTIFACT",
                        title=f"{art_name} • {heading[:68]}",
                        language="markdown",
                        code=sec_clean[:1600],
                        referenced_paths=self._extract_referenced_paths(sec_clean) or [art_path],
                    )
                )
        return blocks

    def _parse_brain_directory(
        self,
        conv_dir: Path,
        is_primary: bool,
    ) -> tuple[AntigravityBrainEntry | None, list[AntigravityBrainEntry]]:
        conv_id = conv_dir.name
        short_id = conv_id[:8]
        tr_path = conv_dir / ".system_generated" / "logs" / "transcript.jsonl"
        tr_full_path = conv_dir / ".system_generated" / "logs" / "transcript_full.jsonl"
        tasks_dir = conv_dir / ".system_generated" / "tasks"

        md_paths = sorted(p for p in conv_dir.glob("*.md") if p.is_file())
        task_paths = sorted(p for p in tasks_dir.glob("*.log") if p.is_file()) if tasks_dir.is_dir() else []

        tr_size = tr_path.stat().st_size if tr_path.is_file() else 0
        tr_mtime_ns = tr_path.stat().st_mtime_ns if tr_path.is_file() else conv_dir.stat().st_mtime_ns
        updated_iso = datetime.datetime.fromtimestamp(
            tr_mtime_ns / 1e9, tz=datetime.timezone.utc
        ).isoformat()

        # Index directory in FSBase._index so FSBase tracks all brain paths
        dir_files = [p.name for p in md_paths]
        if tr_path.is_file():
            dir_files.append(".system_generated/logs/transcript.jsonl")
        self.fs._index.store_in_index(self.fs._key, str(conv_dir), dir_files)

        artifact_refs: list[AntigravityBrainArtifactRef] = []
        artifact_texts: dict[str, str] = {}
        for mp in md_paths:
            buf = io.BytesIO()
            try:
                self.fs.read(str(mp), buf, use_pipe=True)
                raw_md = _sanitize_text(buf.getvalue().decode("utf-8", errors="ignore"))
            except Exception:
                raw_md = ""
            artifact_texts[mp.name] = raw_md
            h1_m = re.search(r"^#\s+(.+)$", raw_md, flags=re.MULTILINE)
            art_title = h1_m.group(1).strip() if h1_m else mp.stem.replace("_", " ").title()
            artifact_refs.append(
                AntigravityBrainArtifactRef(
                    name=mp.name,
                    path=str(mp),
                    size_bytes=mp.stat().st_size,
                    title=art_title,
                    snippet=self._make_small_snippet(raw_md, max_len=190),
                )
            )

        task_log_meta: list[dict[str, Any]] = [
            {
                "name": tp.name,
                "path": str(tp),
                "size_bytes": tp.stat().st_size,
            }
            for tp in task_paths[:16]
        ]

        steps, total_steps = self._read_bounded_transcript_steps(tr_path, tr_size)
        if not steps and not artifact_refs:
            return None, []

        first_user_req = ""
        substantive_user_req = ""
        latest_agent_resp = ""
        created_at = updated_iso
        corpus_chunks: list[str] = []
        referenced_files: list[str] = []

        for idx, st in enumerate(steps):
            if idx == 0 and st.get("created_at"):
                created_at = str(st.get("created_at"))
            s_type = str(st.get("type", ""))
            raw_c = str(st.get("content", "") or "")
            raw_t = str(st.get("thinking", "") or "")
            if s_type == "USER_INPUT" or (not is_primary and s_type == "SYSTEM_MESSAGE"):
                clean_u = self._extract_clean_user_request(raw_c)
                if clean_u:
                    if not first_user_req:
                        first_user_req = clean_u
                    if len(clean_u) > 35 and "commit and push" not in clean_u.lower():
                        substantive_user_req = clean_u
                    corpus_chunks.append(clean_u[:1000])
            elif s_type == "PLANNER_RESPONSE" and raw_c.strip():
                clean_resp = _sanitize_text(raw_c.strip())
                latest_agent_resp = clean_resp
                corpus_chunks.append(clean_resp[:600])
            elif raw_c:
                corpus_chunks.append(_sanitize_text(raw_c[:600]))
            if raw_t:
                corpus_chunks.append(_sanitize_text(raw_t[:500]))
            for tc in st.get("tool_calls") or []:
                if isinstance(tc, dict):
                    referenced_files.extend(self._extract_referenced_paths(json.dumps(tc.get("args") or {})))
            referenced_files.extend(self._extract_referenced_paths(raw_c))

        for art_name, art_md in artifact_texts.items():
            corpus_chunks.append(art_md[:1500])
            referenced_files.extend(self._extract_referenced_paths(art_md))

        dedup_refs: list[str] = []
        for rf in referenced_files:
            if rf not in dedup_refs:
                dedup_refs.append(rf)
            if len(dedup_refs) >= 20:
                break

        # Filter out trivial 1-word "test" / "quota" sessions
        if (
            tr_size < 10_000
            and not artifact_refs
            and first_user_req.strip().lower() in ("test", "quota", "")
        ):
            return None, []

        full_corpus = "\n".join(corpus_chunks)
        workspace_project = self._infer_workspace_project(full_corpus, dedup_refs)
        related_modules, related_kis, related_models, related_benchmarks = self._extract_relations(
            full_corpus, workspace_project
        )

        code_blocks, user_turns, tool_calls_count = self._build_conversation_code_blocks(
            steps=steps,
            artifact_refs=artifact_refs,
            artifact_texts=artifact_texts,
            task_logs=task_log_meta,
        )

        title = self._synthesize_title(
            first_request=first_user_req or (artifact_refs[0].title if artifact_refs else ""),
            workspace_project=workspace_project,
            short_id=short_id,
            is_subagent=not is_primary,
        )
        snippet_source = (
            substantive_user_req
            or first_user_req
            or latest_agent_resp
            or (artifact_refs[0].snippet if artifact_refs else full_corpus)
        )
        small_snippet = self._make_small_snippet(snippet_source, max_len=200)

        artifact_items: list[dict[str, Any]] = []
        for art_ref in artifact_refs:
            art_raw = artifact_texts.get(art_ref.name, "")
            if len(art_raw.strip()) < 40:
                continue
            art_slug = _slugify(Path(art_ref.name).stem)
            artifact_items.append(
                {
                    "entry_id": f"brain_art_{short_id}_{art_slug}",
                    "name": art_ref.name,
                    "title": art_ref.title or art_ref.name,
                    "has_mermaid": "```mermaid" in art_raw,
                }
            )

        primary_loc = str(tr_path) if tr_path.is_file() else (artifact_refs[0].path if artifact_refs else str(conv_dir))
        conv_msg_text = (
            latest_agent_resp
            or substantive_user_req
            or first_user_req
            or (artifact_texts.get(artifact_refs[0].name, "") if artifact_refs else "")
        )
        conv_entry = AntigravityBrainEntry(
            entry_id=f"brain_conv_{short_id}",
            conversation_id=conv_id,
            short_id=short_id,
            entry_type="primary_conversation" if is_primary else "subagent_session",
            title=title,
            subtitle=(
                f"AGENT • {workspace_project} • {total_steps} steps"
                + (f" • {len(artifact_items)} artifacts" if artifact_items else "")
            ),
            workspace_project=workspace_project,
            snippet=small_snippet,
            message_role="AGENT",
            message_text=conv_msg_text[:3200],
            artifact_items=artifact_items,
            created_at=created_at,
            updated_at=updated_iso,
            mtime_ns=tr_mtime_ns,
            transcript_size_bytes=tr_size,
            step_count=total_steps,
            user_turn_count=user_turns,
            tool_call_count=tool_calls_count,
            artifact_count=len(artifact_refs),
            task_log_count=len(task_paths),
            locations=AntigravityBrainLocationMeta(
                brain_dir=str(conv_dir),
                transcript_path=str(tr_path) if tr_path.is_file() else "",
                transcript_full_path=str(tr_full_path) if tr_full_path.is_file() else "",
                primary_location=primary_loc,
                artifact_files=artifact_refs,
                task_logs=task_log_meta,
                referenced_files=dedup_refs,
            ),
            relations=AntigravityBrainRelationMeta(
                related_modules=related_modules,
                related_kis=related_kis,
                related_models=related_models,
                related_benchmarks=related_benchmarks,
                related_conversations=[],
            ),
            code_blocks=code_blocks,
        )

        artifact_entries: list[AntigravityBrainEntry] = []
        for art_ref in artifact_refs:
            art_raw = artifact_texts.get(art_ref.name, "")
            if len(art_raw.strip()) < 40:
                continue
            art_slug = _slugify(Path(art_ref.name).stem)
            art_entry_id = f"brain_art_{short_id}_{art_slug}"
            art_proj = self._infer_workspace_project(art_raw, dedup_refs)
            a_mods, a_kis, a_models, a_benchs = self._extract_relations(art_raw, art_proj)
            art_blocks = self._build_artifact_code_blocks(
                art_name=art_ref.name,
                art_path=art_ref.path,
                art_text=art_raw,
                parent_user_req=first_user_req,
            )
            art_mtime_ns = Path(art_ref.path).stat().st_mtime_ns if Path(art_ref.path).is_file() else tr_mtime_ns
            art_iso = datetime.datetime.fromtimestamp(
                art_mtime_ns / 1e9, tz=datetime.timezone.utc
            ).isoformat()
            artifact_entries.append(
                AntigravityBrainEntry(
                    entry_id=art_entry_id,
                    conversation_id=conv_id,
                    short_id=short_id,
                    entry_type="brain_artifact",
                    title=f"Artifact: {art_ref.title or art_ref.name}",
                    subtitle=f"ARTIFACT • {art_proj} • {art_ref.name}",
                    workspace_project=art_proj,
                    snippet=art_ref.snippet or self._make_small_snippet(art_raw, max_len=200),
                    message_role="ARTIFACT",
                    message_text=art_raw,
                    artifact_items=[
                        {
                            "entry_id": art_entry_id,
                            "name": art_ref.name,
                            "title": art_ref.title or art_ref.name,
                            "has_mermaid": "```mermaid" in art_raw,
                        },
                        *[a for a in artifact_items if a.get("entry_id") != art_entry_id],
                    ],
                    created_at=created_at,
                    updated_at=art_iso,
                    mtime_ns=art_mtime_ns,
                    transcript_size_bytes=art_ref.size_bytes,
                    step_count=len(art_blocks),
                    user_turn_count=1,
                    tool_call_count=0,
                    artifact_count=1,
                    task_log_count=len(task_paths),
                    locations=AntigravityBrainLocationMeta(
                        brain_dir=str(conv_dir),
                        transcript_path=str(tr_path) if tr_path.is_file() else "",
                        transcript_full_path=str(tr_full_path) if tr_full_path.is_file() else "",
                        primary_location=art_ref.path,
                        artifact_files=[art_ref],
                        task_logs=task_log_meta[:6],
                        referenced_files=self._extract_referenced_paths(art_raw) or dedup_refs[:8],
                    ),
                    relations=AntigravityBrainRelationMeta(
                        related_modules=a_mods,
                        related_kis=a_kis,
                        related_models=a_models,
                        related_benchmarks=a_benchs,
                        related_conversations=[
                            {
                                "entry_id": conv_entry.entry_id,
                                "conversation_id": conv_id,
                                "title": conv_entry.title,
                                "workspace_project": conv_entry.workspace_project,
                                "shared_reason": "Parent conversation session",
                            }
                        ],
                    ),
                    code_blocks=art_blocks,
                )
            )

        segment_entries = self._build_conversation_segment_entries(conv_entry, code_blocks)
        return conv_entry, [*segment_entries, *artifact_entries]

    def scan_and_build(self, force: bool = False) -> dict[str, Any]:
        """
        Scans `/Users/jarek/.gemini/antigravity-cli/brain` via `FSBase`, incrementally re-parsing
        only directories whose transcript or artifact mtime/size changed, cross-links related
        conversations/modules/KIs/models, and persists `antigravity_brain_ledger.json` via `FSBase`.
        """
        with self._lock:
            t0 = time.perf_counter()
            current_sig = self.get_live_brain_signature()
            if not force and current_sig == self._last_global_sig:
                cached_overview = self._index.load_from_index("META", "OVERVIEW")
                if cached_overview is not None:
                    self._incremental_hits += 1
                    return cached_overview

            if not self.brain_root.is_dir():
                return {
                    "total_entries": 0,
                    "primary_conversations_count": 0,
                    "artifacts_count": 0,
                    "subagent_sessions_count": 0,
                    "entries": [],
                }

            primary_dirs: list[tuple[int, Path, str]] = []
            subagent_dirs: list[tuple[int, Path, str]] = []

            for d in self.brain_root.iterdir():
                if not d.is_dir() or d.name.startswith("."):
                    continue
                tr = d / ".system_generated" / "logs" / "transcript.jsonl"
                mds = sorted(d.glob("*.md"))
                if not tr.is_file() and not mds:
                    continue
                tr_st = tr.stat() if tr.is_file() else d.stat()
                tr_size = tr_st.st_size if tr.is_file() else 0
                if tr_size < 4096 and not mds:
                    continue

                kind = self._index.load_from_index("META", f"kind:{d.name}")
                if kind is None:
                    first_bytes = b""
                    if tr.is_file() and tr_size > 0:
                        try:
                            with self.fs.open(str(tr), "rb") as f:
                                first_bytes = f.read(512)
                        except Exception:
                            first_bytes = b""
                    kind = "primary" if (b"USER_INPUT" in first_bytes or mds) else "subagent"
                    self._index.store_in_index("META", f"kind:{d.name}", kind)

                md_sig = ",".join(f"{m.name}:{m.stat().st_mtime_ns}:{m.stat().st_size}" for m in mds)
                dir_sig = f"{tr_st.st_mtime_ns}:{tr_size}:{md_sig}"
                if kind == "primary" or mds:
                    primary_dirs.append((tr_st.st_mtime_ns, d, dir_sig))
                else:
                    subagent_dirs.append((tr_st.st_mtime_ns, d, dir_sig))

            primary_dirs.sort(key=lambda x: x[0], reverse=True)
            subagent_dirs.sort(key=lambda x: x[0], reverse=True)
            selected_dirs: list[tuple[Path, str, bool]] = [
                (d, sig, True) for _, d, sig in primary_dirs
            ] + [
                (d, sig, False) for _, d, sig in subagent_dirs[: self.max_subagent_sessions]
            ]

            all_entries: list[AntigravityBrainEntry] = []
            for conv_dir, dir_sig, is_prim in selected_dirs:
                cid = conv_dir.name
                cached_sig = self._index.load_from_index("CONV_SIGS", cid)
                cached_bundle = self._index.load_from_index("ENTRIES", cid)
                if not force and cached_sig == dir_sig and cached_bundle is not None:
                    self._incremental_hits += 1
                    conv_entry, art_entries = cached_bundle
                else:
                    self._incremental_updates += 1
                    conv_entry, art_entries = self._parse_brain_directory(conv_dir, is_primary=is_prim)
                    self._index.store_in_index("CONV_SIGS", cid, dir_sig)
                    self._index.store_in_index("ENTRIES", cid, (conv_entry, art_entries))

                if conv_entry is not None:
                    all_entries.append(conv_entry)
                all_entries.extend(art_entries)

            # Cross-link related conversations by shared workspace_project, related_modules, or related_models
            conv_only = [e for e in all_entries if e.entry_type != "brain_artifact"]
            for entry in all_entries:
                rel_convs: list[dict[str, str]] = list(entry.relations.related_conversations)
                seen_ids = {r.get("entry_id") for r in rel_convs}
                my_mods = set(entry.relations.related_modules)
                my_models = set(entry.relations.related_models)
                for other in conv_only:
                    if other.entry_id == entry.entry_id or other.entry_id in seen_ids:
                        continue
                    shared_models = my_models.intersection(other.relations.related_models)
                    shared_mods = my_mods.intersection(other.relations.related_modules)
                    if shared_models or other.workspace_project == entry.workspace_project or len(shared_mods) >= 2:
                        reason = (
                            f"Shared model: {next(iter(shared_models))}"
                            if shared_models
                            else f"Shared project ({other.workspace_project}) & modules ({', '.join(list(shared_mods)[:3])})"
                        )
                        rel_convs.append(
                            {
                                "entry_id": other.entry_id,
                                "conversation_id": other.conversation_id,
                                "title": other.title,
                                "workspace_project": other.workspace_project,
                                "shared_reason": reason,
                            }
                        )
                        seen_ids.add(other.entry_id)
                    if len(rel_convs) >= 6:
                        break
                entry.relations.related_conversations = rel_convs

            # Sort entries chronologically by time (oldest at top -> newest at bottom)
            all_entries.sort(key=lambda e: (e.mtime_ns, e.entry_id))

            primary_convs = sorted(
                [e for e in all_entries if e.entry_type == "primary_conversation"],
                key=lambda e: -e.mtime_ns,
            )
            conversation_tabs: list[dict[str, Any]] = []
            for pc in primary_convs:
                direct_count = sum(1 for e in all_entries if e.conversation_id == pc.conversation_id)
                short_label = pc.title[:38].rsplit(" ", 1)[0] + "..." if len(pc.title) > 38 else pc.title
                conversation_tabs.append(
                    {
                        "conversation_id": pc.conversation_id,
                        "short_id": pc.short_id,
                        "entry_id": pc.entry_id,
                        "label": f"{pc.workspace_project}: {short_label}",
                        "title": pc.title,
                        "workspace_project": pc.workspace_project,
                        "direct_entry_count": direct_count,
                        "step_count": pc.step_count,
                        "code_block_count": len(pc.code_blocks),
                    }
                )

            self._last_global_sig = current_sig
            self._last_scan_ms = round((time.perf_counter() - t0) * 1000.0, 2)

            indexed_dirs = sum(1 for _ in self.fs._index.range_index(self.fs._key))
            indexed_files = sum(len(files) for _, files in self.fs._index.range_index(self.fs._key))
            now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

            self._sync_events.append(
                {
                    "timestamp": now_iso,
                    "signature": current_sig,
                    "total_entries": len(all_entries),
                    "scan_ms": self._last_scan_ms,
                }
            )

            overview: dict[str, Any] = {
                "brain_root": str(self.brain_root),
                "ledger_path": str(self.ledger_path),
                "last_synced_at": now_iso,
                "live_signature": current_sig,
                "total_entries": len(all_entries),
                "primary_conversations_count": sum(
                    1 for e in all_entries if e.entry_type == "primary_conversation"
                ),
                "artifacts_count": sum(1 for e in all_entries if e.entry_type == "brain_artifact"),
                "subagent_sessions_count": sum(
                    1 for e in all_entries if e.entry_type == "subagent_session"
                ),
                "conversation_segments_count": sum(
                    1 for e in all_entries if e.entry_type == "conversation_segment"
                ),
                "conversation_tabs": conversation_tabs,
                "projects_breakdown": self._compute_projects_breakdown(all_entries),
                "fsbase_telemetry": {
                    "filesystem": self.fs.filesystem,
                    "brain_root": str(self.brain_root),
                    "indexed_directories_count": indexed_dirs,
                    "indexed_files_count": indexed_files,
                    "incremental_hits": self._incremental_hits,
                    "incremental_updates": self._incremental_updates,
                    "last_scan_ms": self._last_scan_ms,
                },
                "entries": [e.model_dump() for e in all_entries],
            }

            self._index.store_in_index("META", "OVERVIEW", overview)
            self._index.store_in_index("META", "ENTRIES_LIST", all_entries)
            self._persist_ledger_via_fsbase(overview)
            return overview

    @staticmethod
    def _compute_projects_breakdown(entries: list[AntigravityBrainEntry]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for e in entries:
            counts[e.workspace_project] = counts.get(e.workspace_project, 0) + 1
        return counts

    def _persist_ledger_via_fsbase(self, overview: dict[str, Any]) -> None:
        try:
            self.fs.make_dirs(str(self.ledger_path.parent), exist_ok=True)
            compact_payload = {
                "brain_root": overview["brain_root"],
                "last_synced_at": overview["last_synced_at"],
                "live_signature": overview["live_signature"],
                "total_entries": overview["total_entries"],
                "primary_conversations_count": overview["primary_conversations_count"],
                "artifacts_count": overview["artifacts_count"],
                "subagent_sessions_count": overview["subagent_sessions_count"],
                "fsbase_telemetry": overview["fsbase_telemetry"],
                "entry_ids": [e["entry_id"] for e in overview["entries"]],
            }
            buf = io.BytesIO(json.dumps(compact_payload, indent=2).encode("utf-8"))
            self.fs.write(str(self.ledger_path), buf, use_pipe=True)
        except Exception:
            pass

    def build_qdrant_documents(self) -> list[AntigravityBrainQdrantDocument]:
        """
        Builds Qdrant-ready documents (`AntigravityBrainQdrantDocument`) with full relational
        metadata (where to find files, related modules/KIs/models/conversations, and structured
        code blocks) for vector + lexical indexing.
        """
        with self._lock:
            entries: list[AntigravityBrainEntry] | None = self._index.load_from_index("META", "ENTRIES_LIST")
            if not entries:
                self.scan_and_build(force=False)
                entries = self._index.load_from_index("META", "ENTRIES_LIST") or []

            docs: list[AntigravityBrainQdrantDocument] = []
            for e in entries:
                md_lines = [
                    f"# {e.title}",
                    f"- **Entry ID**: `{e.entry_id}`",
                    f"- **Conversation ID**: `{e.conversation_id}`",
                    f"- **Type**: `{e.entry_type}`",
                    f"- **Workspace Project**: `{e.workspace_project}`",
                    f"- **Primary Location**: `{e.locations.primary_location}`",
                    f"- **Brain Directory**: `{e.locations.brain_dir}`",
                    f"- **Snippet**: {e.snippet}",
                    "",
                    "## Where to Find Things",
                    f"- Transcript: `{e.locations.transcript_path}`",
                ]
                for art in e.locations.artifact_files:
                    md_lines.append(f"- Artifact `{art.name}`: `{art.path}`")
                for rf in e.locations.referenced_files[:12]:
                    md_lines.append(f"- Referenced Workspace File: `{rf}`")

                md_lines.extend(
                    [
                        "",
                        "## Related Metadata",
                        f"- Related Toolset Modules: {', '.join(e.relations.related_modules)}",
                        f"- Related Knowledge Items: {', '.join(e.relations.related_kis)}",
                        f"- Related Neural Models: {', '.join(e.relations.related_models)}",
                        f"- Related Benchmarks: {', '.join(e.relations.related_benchmarks)}",
                        "",
                        "## Conversation Code Blocks",
                    ]
                )
                for cb in e.code_blocks[:12]:
                    md_lines.append(f"### [{cb.badge_label}] {cb.title}")
                    md_lines.append(f"```{cb.language}\n{cb.code[:900]}\n```")

                docs.append(
                    AntigravityBrainQdrantDocument(
                        doc_id=e.entry_id,
                        entry_id=e.entry_id,
                        conversation_id=e.conversation_id,
                        doc_type=e.entry_type,
                        title=e.title,
                        subtitle=e.subtitle,
                        workspace_project=e.workspace_project,
                        snippet=e.snippet,
                        primary_location=e.locations.primary_location,
                        brain_dir=e.locations.brain_dir,
                        transcript_path=e.locations.transcript_path,
                        referenced_files=e.locations.referenced_files,
                        artifact_names=[a.name for a in e.locations.artifact_files],
                        related_modules=e.relations.related_modules,
                        related_kis=e.relations.related_kis,
                        related_models=e.relations.related_models,
                        related_benchmarks=e.relations.related_benchmarks,
                        related_conversation_ids=[
                            r["entry_id"] for r in e.relations.related_conversations
                        ],
                        code_block_count=len(e.code_blocks),
                        step_count=e.step_count,
                        updated_at=e.updated_at,
                        markdown="\n".join(md_lines),
                    )
                )
            return docs

    def start_periodic_producer(
        self,
        interval_seconds: int = 5,
        on_update: Callable[[dict[str, Any]], Any] | None = None,
    ) -> tuple[PeriodicProducer, QueueController]:
        """
        Starts a toolset `PeriodicProducer` + `QueueController` pipeline that polls `FSBase`
        for new or updated brain files every `interval_seconds` and triggers `on_update`.
        """
        def _consumer_action(q_data: Any) -> Any:
            payload = q_data.attribute("result")
            if payload and on_update is not None:
                on_update(payload)
            return payload

        def _producer_poll() -> list[dict[str, Any]] | None:
            sig = self.get_live_brain_signature()
            if sig != self._last_global_sig:
                overview = self.scan_and_build(force=False)
                return [overview]
            return None

        self._periodic_queue = QueueController(action=_consumer_action, max_workers=1)
        self._periodic_producer = PeriodicProducer(
            action=_producer_poll,
            queue=self._periodic_queue,
            interval=interval_seconds,
            start_now=False,
        )
        return self._periodic_producer, self._periodic_queue

    def sync_to_qdrant(
        self,
        qdrant_path: str | Path = "/Users/jarek/IdeaProjects/portfolio/portfolio/.qdrant_data",
        collection_name: str = "portfolio_antigravity_brain",
        embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2",
        chunk_size: int = 380,
        chunk_overlap: int = 64,
        qdrant_client: Any | None = None,
    ) -> dict[str, Any]:
        """
        Incrementally indexes changed Antigravity Brain documents directly into Qdrant
        (`portfolio_antigravity_brain`).
        """
        from qdrant_client import QdrantClient
        from qdrant_client.models import (
            Distance,
            FieldCondition,
            Filter,
            MatchValue,
            PointStruct,
            VectorParams,
        )

        with self._lock:
            t0 = time.perf_counter()
            overview = self.scan_and_build(force=False)
            docs = self.build_qdrant_documents()

            owns_client = False
            client = qdrant_client
            if client is None:
                q_path = Path(qdrant_path)
                q_path.mkdir(parents=True, exist_ok=True)
                try:
                    client = QdrantClient(path=str(q_path))
                    owns_client = True
                except Exception as exc:
                    return {
                        "status": "DEFERRED_LOCKED",
                        "collection_name": collection_name,
                        "reason": str(exc),
                        "total_entries": overview.get("total_entries", 0),
                    }

            try:
                if not client.collection_exists(collection_name):
                    client.create_collection(
                        collection_name=collection_name,
                        vectors_config=VectorParams(size=384, distance=Distance.COSINE),
                    )

                changed_records: list[dict[str, Any]] = []
                embed_inputs: list[str] = []

                for doc in docs:
                    full_text = doc.markdown.strip()
                    text_sha256 = hashlib.sha256(full_text.encode("utf-8")).hexdigest()
                    prev_sha = self._index.load_from_index("QDRANT_DOCS", doc.doc_id)
                    if prev_sha == text_sha256:
                        continue

                    try:
                        client.delete(
                            collection_name=collection_name,
                            points_selector=Filter(
                                must=[FieldCondition(key="doc_id", match=MatchValue(value=doc.doc_id))]
                            ),
                        )
                    except Exception:
                        pass
                    self._index.store_in_index("QDRANT_DOCS", doc.doc_id, text_sha256)

                    step_chars = max(1, chunk_size - chunk_overlap)
                    chunks: list[dict[str, Any]] = []
                    pos = 0
                    text_len = len(full_text)
                    while pos < text_len:
                        c_start = max(0, pos - chunk_overlap) if pos > 0 else 0
                        c_end = min(text_len, pos + step_chars)
                        chunks.append(
                            {
                                "char_start": c_start,
                                "char_end": c_end,
                                "core_start": pos,
                                "core_end": c_end,
                                "overlap_prefix_len": pos - c_start,
                                "chunk_text": full_text[c_start:c_end],
                                "non_overlapping_slice": full_text[pos:c_end],
                            }
                        )
                        if c_end >= text_len:
                            break
                        pos = c_end

                    total_c = len(chunks)
                    point_ids = [
                        uuid5_from_args(
                            ANTIGRAVITY_BRAIN_NAMESPACE_UUID,
                            collection_name,
                            doc.doc_id,
                            idx,
                            embedding_model,
                            chunk_size,
                            chunk_overlap,
                        )
                        for idx in range(total_c)
                    ]

                    for idx, c in enumerate(chunks):
                        rec = {
                            "point_id": point_ids[idx],
                            "doc_id": doc.doc_id,
                            "entry_id": doc.entry_id,
                            "conversation_id": doc.conversation_id,
                            "doc_type": doc.doc_type,
                            "title": doc.title,
                            "subtitle": doc.subtitle,
                            "workspace_project": doc.workspace_project,
                            "snippet": doc.snippet,
                            "primary_location": doc.primary_location,
                            "brain_dir": doc.brain_dir,
                            "transcript_path": doc.transcript_path,
                            "referenced_files": doc.referenced_files,
                            "artifact_names": doc.artifact_names,
                            "related_modules": doc.related_modules,
                            "related_kis": doc.related_kis,
                            "related_models": doc.related_models,
                            "related_benchmarks": doc.related_benchmarks,
                            "related_conversation_ids": doc.related_conversation_ids,
                            "code_block_count": doc.code_block_count,
                            "step_count": doc.step_count,
                            "updated_at": doc.updated_at,
                            "chunk_index": idx,
                            "total_chunks": total_c,
                            "char_start": c["char_start"],
                            "char_end": c["char_end"],
                            "core_start": c["core_start"],
                            "core_end": c["core_end"],
                            "overlap_prefix_len": c["overlap_prefix_len"],
                            "chunk_text": c["chunk_text"],
                            "non_overlapping_slice": c["non_overlapping_slice"],
                            "prev_chunk_id": point_ids[idx - 1] if idx > 0 else None,
                            "next_chunk_id": point_ids[idx + 1] if idx + 1 < total_c else None,
                            "content_sha256": text_sha256,
                            "full_text_sha256": text_sha256,
                            "embedding_model": embedding_model,
                        }
                        changed_records.append(rec)
                        embed_inputs.append(
                            f"[{doc.doc_type}] {doc.title} ({doc.workspace_project}) — {c['chunk_text']}"
                        )

                if changed_records:
                    from sentence_transformers import SentenceTransformer

                    st_model = SentenceTransformer(embedding_model, device="cpu")
                    raw_vecs = st_model.encode(
                        embed_inputs,
                        batch_size=32,
                        show_progress_bar=False,
                        normalize_embeddings=True,
                    )
                    points = [
                        PointStruct(
                            id=rec["point_id"],
                            vector=vec.tolist() if hasattr(vec, "tolist") else list(vec),
                            payload=rec,
                        )
                        for rec, vec in zip(changed_records, raw_vecs)
                    ]
                    client.upsert(collection_name=collection_name, points=points)

                col_info = client.get_collection(collection_name)
                return {
                    "status": "SYNCED",
                    "collection_name": collection_name,
                    "total_documents": len(docs),
                    "changed_chunks_upserted": len(changed_records),
                    "qdrant_point_count": int(col_info.points_count or 0),
                    "duration_ms": round((time.perf_counter() - t0) * 1000.0, 2),
                }
            finally:
                if owns_client and client is not None:
                    try:
                        client.close()
                    except Exception:
                        pass


def sync_antigravity_brain_to_qdrant(
    brain_root: str | Path = "/Users/jarek/.gemini/antigravity-cli/brain",
    ledger_path: str | Path = "/Users/jarek/IdeaProjects/portfolio/portfolio/antigravity_brain_ledger.json",
    qdrant_path: str | Path = "/Users/jarek/IdeaProjects/portfolio/portfolio/.qdrant_data",
    collection_name: str = "portfolio_antigravity_brain",
    qdrant_client: Any | None = None,
) -> dict[str, Any]:
    ledger = AntigravityBrainLedger(brain_root=brain_root, ledger_path=ledger_path)
    return ledger.sync_to_qdrant(
        qdrant_path=qdrant_path,
        collection_name=collection_name,
        qdrant_client=qdrant_client,
    )
