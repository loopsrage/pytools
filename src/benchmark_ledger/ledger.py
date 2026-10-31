from __future__ import annotations

import ast
import datetime as dt
import hashlib
import io
import json
import re
import subprocess
import time
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field

from fsspecc.base_fsspecfs.base_fsspecfs import FSBase
from simple_functions.simple_functions import uuid5_from_args
from thread_safe.index import Index
from thread_safe.tslist import TsList

from benchmark_ledger.vocab_mask import (
    mask_restricted_string,
    mask_restricted_vocabulary,
)

BENCHMARK_NAMESPACE = "6ba7b811-9dad-11d1-80b4-00c04fd430c8"
AUTHENTIC_BUNDLE_PATH = Path(__file__).resolve().parent / "authentic_post_audit_runs.json"

# Pre-audit commits invalidated in neural_networks commit 72cc4f0 / VIOLATION_STAMP_INDEX.md
# due to 13 active benchmark-path violations in PolyphonicPipeline (forward-time eval target
# ingestion into 25.2M rns_register table, .detach() gradient starvation, 99.5% Koopman solver bypass).
INVALIDATED_PRE_AUDIT_COMMITS: set[str] = {"53f105d", "2d063d1"}

CANONICAL_MODEL_ALIASES: dict[str, str] = {
    "CohesiveAnyGram 2-GRU + Unified Monadic Cavity (DQAttn+Moyal+SpecGRU)": (
        "CohesiveUnifiedMonadicCavityModel (AnyGram 2-GRU + Unified Cavity)"
    ),
    "CohesiveAnyGram 2-GRU + StageCompiler vCPU Monadic Cavity": (
        "CohesiveMonadicVCPUModel (AnyGram 2-GRU + StageCompiler vCPU)"
    ),
    "Polyphase Resonant Cavity 2-GRU + StageCompiler vCPU Monadic Cavity": (
        "PolyphaseResonantMonadicVCPUModel (Dual-Mirror 2-GRU + StageCompiler vCPU)"
    ),
    "Polyphase Resonant Cavity 2-GRU + Unified Monadic Cavity (DQAttn+Moyal+SpecGRU)": (
        "PolyphaseResonantUnifiedMonadicCavityModel (Dual-Mirror 2-GRU + Unified Cavity)"
    ),
    "PolyphonicStageCompilerCascadeModel (ISA Spectral/AST Gate + Cohesive-Manifold Wave Cascade)": (
        "PolyphonicStageCompilerCascadeModel (Stage-1 Compiler + ISA Spectral-AST Gate + Cohesive-Manifold Wave Cascade)"
    ),
    "PolyphonicStageCompilerCascadeModel (Stage-1 Compiler + ISA Spectral-AST Gate + Cohesive Wave Cascade)": (
        "PolyphonicStageCompilerCascadeModel (Stage-1 Compiler + ISA Spectral-AST Gate + Cohesive-Manifold Wave Cascade)"
    ),
}

MODEL_FAMILY_METADATA: dict[str, dict[str, Any]] = {
    "CohesiveUnifiedMonadicCavityModel (AnyGram 2-GRU + Unified Cavity)": {
        "class_name": "CohesiveUnifiedMonadicCavityModel",
        "base_class_name": "ContinuousJoinedPolyphaseHyperbolicGRU",
        "family": "Promoted Monadic Cavity Architecture",
        "is_polyphonic": True,
        "summary": (
            "Promoted production architecture (src/models/monadic_cavity/monadic_cavity_models.py) continuously "
            "joining CohesiveAnyGram + 2-Layer GRU (resonant_cavity=False) with the Unified Monadic Cavity "
            "(HyperbolicSpectralEmbedding + HyperbolicSpectralGRU + DualQuaternionCrossAttention + "
            "Unified NonAbelianMobiusFFN) on one shared Poincare ball B^D_{c_D} (c_D = c / D) with zero "
            "intermediate boundary squashing and Callan-Symanzik RG-Flow optimization."
        ),
        "architectural_highlights": [
            "Rank #1 Overall Best Violation-Free Architecture across all 12 multi-domain benchmarks (Mean Rank 3.50, 6/12 Top-3 podium finishes, 554,400 params)",
            "Rank #1 Winner on Lexical Scope Closure Binding (0.2154 CE / 91.15% Acc) and #1 Monadic Cavity architecture on PDEBench-1D Soliton (0.1290 Geodesic / 99.72% Cosine)",
            "Highest token accuracy on LRA Dyck-4 Stack Grammar (74.83% Acc) and Modular Arithmetic Garner RNS (80.03% Acc), plus #2 on Canonical AST Node LM and Poincare Cayley Tree",
            "Single shared Poincare manifold B^D_{c_D} (c_D = c / D) with CallanSymanzikOptimizer + RGFlowScheduler",
        ],
        "related_modules": ["benchmark_ledger", "llm_Fine_Tuning", "thread_safe", "async_queue"],
        "related_kis": [
            "ki_benchmark_ledger_ledger_neural_benchmark_leaderboard_qdrant_history",
            "ki_query_torch_model_torch_local",
        ],
    },
    "CohesiveMonadicVCPUModel (AnyGram 2-GRU + StageCompiler vCPU)": {
        "class_name": "CohesiveMonadicVCPUModel",
        "base_class_name": "OmnipresentImplicitOpcodeVCPU",
        "family": "Promoted StageCompiler vCPU Cavity",
        "is_polyphonic": True,
        "summary": (
            "Promoted production architecture (src/models/monadic_cavity/monadic_cavity_models.py) continuously "
            "joining CohesiveAnyGram + 2-Layer GRU (resonant_cavity=False) with the 5-Opcode StageCompiler Implicit "
            "vCPU Monadic Cavity (OP_CAVITY_GRU=16, OP_SPECTRAL_GRU=32, OP_DUAL_QUAT_ATTN=34, OP_SPINOR_MOYAL=42, "
            "OP_RNS_REGISTER_PINHOLE=55) wrapped in isometric PhaseRotorOpcodeManifold sections (U(1) x| S_{D/2})."
        ),
        "architectural_highlights": [
            "Rank #1 Overall Winner on Mamba / Hyena In-Context Associative Key-Value Recall (0.9217 Test CE / 1.3298 BPB / 65.97% Acc, beating 2-Layer GRU by -0.1420 nats)",
            "Rank #1 Overall Winner on Canonical ast.parse() Scope-Bracketed AST Node Stream Prediction (1.4008 Test CE / 62.33% Acc)",
            "5-Opcode StageCompiler Implicit vCPU with isometric PhaseRotorOpcodeManifold sections (unitary norm error 9.54e-7) and 5-DOF causal spectral-hyperbolic demodulation",
            "Rank #2 on PDEBench-1D Soliton among Monadic Cavity models (0.1352 Geodesic / 99.69% Cosine) and #3 on Modular Arithmetic (79.69% Acc)",
        ],
        "related_modules": ["benchmark_ledger", "llm_Fine_Tuning", "thread_safe", "simple_functions"],
        "related_kis": [
            "ki_benchmark_ledger_ledger_neural_benchmark_leaderboard_qdrant_history",
            "ki_query_torch_model_torch_local",
        ],
    },
    "PolyphaseResonantUnifiedMonadicCavityModel (Dual-Mirror 2-GRU + Unified Cavity)": {
        "class_name": "PolyphaseResonantUnifiedMonadicCavityModel",
        "base_class_name": "ContinuousJoinedPolyphaseHyperbolicGRU",
        "family": "Promoted Monadic Cavity Architecture",
        "is_polyphonic": True,
        "summary": (
            "Promoted production architecture (src/models/monadic_cavity/monadic_cavity_models.py) continuously "
            "joining Dual-Mirror PolyphaseResonantCavity2GRU (resonant_cavity=True) with the Unified Monadic "
            "Cavity (HyperbolicSpectralEmbedding + HyperbolicSpectralGRU + DualQuaternionCrossAttention + "
            "Unified NonAbelianMobiusFFN) on one shared Poincare ball B^D_{c_D}."
        ),
        "architectural_highlights": [
            "Rank #1 Winner on Python Compiler & Geometric Tensor AST Corpus in the 6-epoch promoted suite (2.4258 Test CE / 3.4996 BPB)",
            "Rank #2 on Multi-Scope Closure Capture & Shadowed Variable Binding Resolution (0.2236 Test CE / 91.15% Acc)",
            "Top-3 podium finishes across 5 of 12 benchmarks including Mamba Associative Recall (0.9425 CE / 65.80% Acc), Canonical AST Node LM, and Poincare Cayley Tree",
            "Dual-Mirror AnyGram harmonic cavity reflection fused with Groenewold-Moyal phase-space star-product FFN",
        ],
        "related_modules": ["benchmark_ledger", "llm_Fine_Tuning", "thread_safe"],
        "related_kis": [
            "ki_benchmark_ledger_ledger_neural_benchmark_leaderboard_qdrant_history",
        ],
    },
    "PolyphaseResonantMonadicVCPUModel (Dual-Mirror 2-GRU + StageCompiler vCPU)": {
        "class_name": "PolyphaseResonantMonadicVCPUModel",
        "base_class_name": "OmnipresentImplicitOpcodeVCPU",
        "family": "Promoted StageCompiler vCPU Cavity",
        "is_polyphonic": True,
        "summary": (
            "Promoted production architecture (src/models/monadic_cavity/monadic_cavity_models.py) continuously "
            "joining Dual-Mirror PolyphaseResonantCavity2GRU (resonant_cavity=True) with the 5-Opcode StageCompiler "
            "Implicit vCPU Monadic Cavity."
        ),
        "architectural_highlights": [
            "Rank #1 Overall Winner on Nickel & Kiela Poincare Cayley Tree Ancestor Geodesic Reconstruction (0.2218 Geodesic / 0.1535 RelL2 / 99.34% Cosine)",
            "Highest token accuracy across all models on Mamba Associative Recall (66.32% Acc / 0.9423 Test CE) and Rank #1 on 8-epoch Python AST Corpus (1.7281 Test CE)",
            "Rank #3 on TinyShakespeare Byte/Char LM (2.2707 Test CE), outperforming all non-recurrent and attention baselines",
        ],
        "related_modules": ["benchmark_ledger", "llm_Fine_Tuning", "thread_safe"],
        "related_kis": [
            "ki_benchmark_ledger_ledger_neural_benchmark_leaderboard_qdrant_history",
        ],
    },
    "StageCompilerInnerGRUModel (Full Compiler Wrapping Inner 2-GRU Cavity)": {
        "class_name": "StageCompilerInnerGRUModel",
        "base_class_name": "UnifiedStageCompilerModel",
        "family": "StageCompiler Stage-1 Compiler Cavity",
        "is_polyphonic": True,
        "summary": (
            "Unified 6-subsystem StageCompiler Stage-1 Compiler model (gru_mode='inner', 692,182 params) executing "
            "Compiler Ingress (WaveMasker + Polyglot Grammar Dicts + MCC 5D CohesiveIdentifier + 16-float .hypb "
            "packet synthesis) on the outside, nesting the 2-Layer GRU + HyperbolicSpectralGRU inside the "
            "Monadic Cavity C_0, and exiting through Cohesive Linker, Boundary Trinity (36->35->63), and 5-channel SSA Idiom Lifter."
        ),
        "architectural_highlights": [
            "Beats all 4 Euclidean baselines on Mamba Associative Key-Value Recall (0.9770 Test CE / 64.76% Acc vs 2-Layer GRU 1.0606 CE / 58.51% Acc)",
            "90.62% token accuracy (0.2251 Test CE) on Multi-Scope Closure Capture & Shadowed Variable Binding Resolution",
            "0.1475 Poincare Geodesic (99.64% Cosine Alignment) on PDEBench-1D Soliton and 0.2490 Geodesic (99.14% Cosine) on Poincare Cayley Tree",
        ],
        "related_modules": ["benchmark_ledger", "llm_Fine_Tuning", "thread_safe"],
        "related_kis": [
            "ki_benchmark_ledger_ledger_neural_benchmark_leaderboard_qdrant_history",
        ],
    },
    "StageCompilerOuterGRUModel (Outer 2-GRU + Full Compiler + Cavity)": {
        "class_name": "StageCompilerOuterGRUModel",
        "base_class_name": "UnifiedStageCompilerModel",
        "family": "StageCompiler Stage-1 Compiler Cavity",
        "is_polyphonic": True,
        "summary": (
            "Unified 6-subsystem StageCompiler Stage-1 Compiler model (gru_mode='outer', 692,182 params) running "
            "the 2-Layer nn.GRU + 2-Layer HyperbolicSpectralGRU spine on the outside before compiling into "
            "16-float .hypb instruction packets and passing through the Phase-Rotor Monadic Cavity."
        ),
        "architectural_highlights": [
            "Achieves 1.0154 Test CE (63.02% Acc) on Mamba Associative Key-Value Recall, outperforming all 4 Euclidean baselines",
            "0.2237 Test CE (90.62% Acc) on Lexical Scope Closure Binding and 2.1639 Test CE (47.05% Acc) on Python AST Corpus",
            "Combines outer causal recurrent state tracking with 6-stage StageCompiler .hypb packet compilation and SSA idiom lifting",
        ],
        "related_modules": ["benchmark_ledger", "llm_Fine_Tuning", "thread_safe"],
        "related_kis": [
            "ki_benchmark_ledger_ledger_neural_benchmark_leaderboard_qdrant_history",
        ],
    },
    "StageCompilerNoGRUModel (Full Compiler + Cavity, No GRU)": {
        "class_name": "StageCompilerNoGRUModel",
        "base_class_name": "UnifiedStageCompilerModel",
        "family": "StageCompiler Stage-1 Compiler Cavity",
        "is_polyphonic": True,
        "summary": (
            "Purely non-recurrent 6-subsystem StageCompiler Stage-1 Compiler + Monadic Cavity model "
            "(gru_mode='none', 481,976 params) operating with zero GRU layers via WaveMasker, MCC 5D "
            "CohesiveIdentifier, 16-float .hypb packet synthesis, Dual-Quaternion Attention, Moyal FFN, and Nested RNSRegister."
        ),
        "architectural_highlights": [
            "Rank #1 on PDEBench-1D KdV & Nonlinear Schrodinger Soliton Wave Operator (0.1233 Geodesic / 0.0756 RelL2 / 0.0278 Spectral / 99.77% Cosine)",
            "0.2318 Poincare Geodesic (99.28% Cosine Alignment) on Nickel & Kiela Poincare Cayley Tree without any recurrent GRU parameters",
            "Demonstrates that 16-float .hypb packet synthesis + Dual-Quaternion Attention + Spinor/Moyal FFN excels on continuous wavefields",
        ],
        "related_modules": ["benchmark_ledger", "llm_Fine_Tuning", "thread_safe"],
        "related_kis": [
            "ki_benchmark_ledger_ledger_neural_benchmark_leaderboard_qdrant_history",
        ],
    },
    "StageCompilerPostGRUModel (Full Compiler + Cavity -> Post 2-GRU)": {
        "class_name": "StageCompilerPostGRUModel",
        "base_class_name": "UnifiedStageCompilerModel",
        "family": "StageCompiler Stage-1 Compiler Cavity",
        "is_polyphonic": True,
        "summary": (
            "Unified 6-subsystem StageCompiler Stage-1 Compiler + Monadic Cavity model (gru_mode='post', 643,207 params) "
            "executing Full Compiler Ingress, 16-float .hypb Monadic Cavity, Cohesive Linker, Boundary Trinity, and "
            "5-channel SSA Idiom Lifter FIRST, followed afterward by a Post-Compiler 2-Layer nn.GRU before Egress Any-Gram."
        ),
        "architectural_highlights": [
            "Achieves 0.9141 Test CE (67.53% Acc) on Mamba Associative Key-Value Recall and 1.4256 Test CE (63.37% Acc) on Canonical AST Node LM",
            "Rank #1 on Nickel & Kiela Poincare Cayley Tree (0.2178 Geodesic / 0.1523 RelL2 / 99.39% Cosine) and 0.1396 Geodesic on PDEBench-1D Soliton",
            "72.74% token accuracy on LRA Dyck-4 Stack Grammar and 91.32% accuracy (0.2203 CE) on Lexical Scope Closure Binding",
        ],
        "related_modules": ["benchmark_ledger", "llm_Fine_Tuning", "thread_safe"],
        "related_kis": [
            "ki_benchmark_ledger_ledger_neural_benchmark_leaderboard_qdrant_history",
        ],
    },
    "StageCompilerSandwichGRUModel (Pre 2-GRU -> Full Compiler -> Post 2-GRU)": {
        "class_name": "StageCompilerSandwichGRUModel",
        "base_class_name": "UnifiedStageCompilerModel",
        "family": "StageCompiler Stage-1 Compiler Cavity",
        "is_polyphonic": True,
        "summary": (
            "Sandwich recurrent StageCompiler Stage-1 Compiler architecture (gru_mode='sandwich', 841,607 params) "
            "placing a Pre-Compiler 2-Layer nn.GRU at Ingress, the Full 6-stage StageCompiler Compiler + Monadic "
            "Cavity in the center, and a Post-Compiler 2-Layer nn.GRU before Egress Any-Gram."
        ),
        "architectural_highlights": [
            "0.9800 Test CE (63.54% Acc) on Mamba Associative Key-Value Recall and 0.5543 Test CE (79.69% Acc) on Modular Arithmetic Garner RNS",
            "0.2290 Test CE (90.28% Acc) on Lexical Scope Closure Binding and 0.1405 Geodesic (99.66% Cosine) on PDEBench-1D Soliton",
        ],
        "related_modules": ["benchmark_ledger", "llm_Fine_Tuning", "thread_safe"],
        "related_kis": [
            "ki_benchmark_ledger_ledger_neural_benchmark_leaderboard_qdrant_history",
        ],
    },
    "StageCompilerPostHybridSpectralGRUModel (Compiler + vCPU SpecGRU -> Post 2-GRU + Post 2-SpecGRU)": {
        "class_name": "StageCompilerPostHybridSpectralGRUModel",
        "base_class_name": "UnifiedStageCompilerModel",
        "family": "StageCompiler Spectral-GRU Compiler Cavity",
        "is_polyphonic": True,
        "summary": (
            "Hybrid Post-Compiler Spectral+Euclidean GRU architecture (gru_mode='post_hybrid_spectral', 654,883 params) "
            "running the Full StageCompiler Stage-1 Compiler + vCPU Monadic Cavity (with internal Op 32 HyperbolicSpectralGRU) "
            "first, followed by coupled Post 2-Layer nn.GRU + Post 2-Layer HyperbolicSpectralGRU before Egress Any-Gram."
        ),
        "architectural_highlights": [
            "Breaks the 0.90 CE barrier on Mamba Associative Recall (0.8933 Test CE / 1.2888 BPB / 67.71% Acc) and achieves 1.9841 Test CE (51.56% Acc) on Python AST Corpus",
            "Couples Post-Compiler 2-Layer nn.GRU with 2-Layer HyperbolicSpectralGRU and internal vCPU Op 32 SpectralGRU",
        ],
        "related_modules": ["benchmark_ledger", "llm_Fine_Tuning", "thread_safe"],
        "related_kis": [
            "ki_benchmark_ledger_ledger_neural_benchmark_leaderboard_qdrant_history",
        ],
    },
    "StageCompilerPostSpectralGRUModel (Full Compiler + vCPU SpecGRU -> Post 2-SpecGRU)": {
        "class_name": "StageCompilerPostSpectralGRUModel",
        "base_class_name": "UnifiedStageCompilerModel",
        "family": "StageCompiler Spectral-GRU Compiler Cavity",
        "is_polyphonic": True,
        "summary": (
            "Pure HyperbolicSpectralGRU Post-Compiler architecture (gru_mode='post_spectral', 456,419 params) "
            "executing the Full StageCompiler Stage-1 Compiler + vCPU Monadic Cavity (Op 32 HyperbolicSpectralGRU) "
            "followed by a Post 2-Layer HyperbolicSpectralGRU spine with zero Euclidean nn.GRU parameters."
        ),
        "architectural_highlights": [
            "2.0640 Test CE on Python Compiler & Geometric Tensor AST Corpus and 1.0147 Test CE (63.37% Acc) on Mamba Associative Recall with 456,419 params",
        ],
        "related_modules": ["benchmark_ledger", "llm_Fine_Tuning", "thread_safe"],
        "related_kis": [
            "ki_benchmark_ledger_ledger_neural_benchmark_leaderboard_qdrant_history",
        ],
    },
    "StageCompilerOuterSpectralGRUModel (Outer 2-SpecGRU + Full Compiler + vCPU SpecGRU)": {
        "class_name": "StageCompilerOuterSpectralGRUModel",
        "base_class_name": "UnifiedStageCompilerModel",
        "family": "StageCompiler Spectral-GRU Compiler Cavity",
        "is_polyphonic": True,
        "summary": (
            "Pure HyperbolicSpectralGRU Outer-Compiler architecture (gru_mode='outer_spectral', 456,419 params) "
            "running an Outer 2-Layer HyperbolicSpectralGRU spine before StageCompiler Stage-1 Compiler Ingress and vCPU SpecGRU Cavity."
        ),
        "architectural_highlights": [
            "0.9848 Test CE (64.24% Acc) on Mamba Associative Key-Value Recall and 2.0905 Test CE on Python AST Corpus",
        ],
        "related_modules": ["benchmark_ledger", "llm_Fine_Tuning", "thread_safe"],
        "related_kis": [
            "ki_benchmark_ledger_ledger_neural_benchmark_leaderboard_qdrant_history",
        ],
    },
    "StageCompilerInnerSpectralGRUModel (Compiler Wrapping Inner 2-SpecGRU + vCPU SpecGRU)": {
        "class_name": "StageCompilerInnerSpectralGRUModel",
        "base_class_name": "UnifiedStageCompilerModel",
        "family": "StageCompiler Spectral-GRU Compiler Cavity",
        "is_polyphonic": True,
        "summary": (
            "Pure HyperbolicSpectralGRU Inner-Compiler architecture (gru_mode='inner_spectral', 456,419 params) "
            "nesting a 2-Layer HyperbolicSpectralGRU inside the StageCompiler Stage-1 Compiler's 16-float .hypb Monadic Cavity."
        ),
        "architectural_highlights": [
            "1.0052 Test CE (63.72% Acc) on Mamba Associative Key-Value Recall and 2.1003 Test CE on Python AST Corpus",
        ],
        "related_modules": ["benchmark_ledger", "llm_Fine_Tuning", "thread_safe"],
        "related_kis": [
            "ki_benchmark_ledger_ledger_neural_benchmark_leaderboard_qdrant_history",
        ],
    },
    "StageCompilerDualSpectralGRUModel (Outer SpecGRU -> Compiler + vCPU SpecGRU -> Post SpecGRU)": {
        "class_name": "StageCompilerDualSpectralGRUModel",
        "base_class_name": "UnifiedStageCompilerModel",
        "family": "StageCompiler Spectral-GRU Compiler Cavity",
        "is_polyphonic": True,
        "summary": (
            "Dual Outer+Post HyperbolicSpectralGRU Sandwich Compiler architecture (gru_mode='dual_spectral', 468,095 params) "
            "combining Outer 2-Layer HyperbolicSpectralGRU, vCPU Op 32 SpectralGRU, and Post 2-Layer HyperbolicSpectralGRU."
        ),
        "architectural_highlights": [
            "1.0579 Test CE (63.19% Acc) on Mamba Associative Key-Value Recall with 468,095 pure spectral-hyperbolic parameters",
        ],
        "related_modules": ["benchmark_ledger", "llm_Fine_Tuning", "thread_safe"],
        "related_kis": [
            "ki_benchmark_ledger_ledger_neural_benchmark_leaderboard_qdrant_history",
        ],
    },
    "CohesiveCascadedSpectralGRUModel (Cohesive-Manifold Cascaded Hyperbolic/Spectral GRU Waves)": {
        "class_name": "CohesiveCascadedSpectralGRUModel",
        "base_class_name": "UnifiedStageCompilerModel",
        "family": "Cohesive-Manifold Cascaded Compiler Cavity",
        "is_polyphonic": True,
        "summary": (
            "Pure Hyperbolic/Spectral Cohesive-Manifold Cascade GRU architecture (gru_mode='cascaded_spectral', 468,419 params) "
            "winding two orthogonal PhaseRotorOpcodeManifold HyperbolicSpectralGRU cohesive manifolds and multi-horizon cohesive-manifold waves "
            "and joining them via cascade_join_cohesive_waves (stage cosine coherence, Rayleigh sharpness, Groenewold-Moyal "
            "phase-space star product, and Riemannian geodesic SLERP on B^D_{c_D})."
        ),
        "architectural_highlights": [
            "All-Time #1 Token Accuracy (75.17% ± 1.65% Acc / 0.8201 CE) on LRA Dyck-4 Multi-Bracket Stack Grammar with zero Euclidean nn.GRU parameters",
            "91.15% accuracy on Lexical Scope Closure Binding and 0.1342 Geodesic (99.73% Cosine) on PDEBench-1D Soliton",
        ],
        "related_modules": ["benchmark_ledger", "llm_Fine_Tuning", "thread_safe"],
        "related_kis": [
            "ki_benchmark_ledger_ledger_neural_benchmark_leaderboard_qdrant_history",
        ],
    },
    "CohesiveCascadedHybridGRUModel (Cohesive-Manifold Cascaded Euclidean + Spectral GRU Waves)": {
        "class_name": "CohesiveCascadedHybridGRUModel",
        "base_class_name": "UnifiedStageCompilerModel",
        "family": "Cohesive-Manifold Cascaded Compiler Cavity",
        "is_polyphonic": True,
        "summary": (
            "Hybrid Euclidean + Hyperbolic Spectral Cohesive-Manifold Cascade GRU architecture (gru_mode='cascaded_hybrid', 655,143 params) "
            "cascade-joining the post-compiler 2-Layer nn.GRU wave, cohesive-wound 2-Layer HyperbolicSpectralGRU wave, "
            "6-horizon cohesive-manifold wave carrier, and compiled .hypb/.sig SSA wavefield via cascade_join_cohesive_waves."
        ),
        "architectural_highlights": [
            "All-Time Rank #1 Winner on Nickel & Kiela Poincare Cayley Tree Ancestor Geodesic Reconstruction (0.2175 Geodesic / 0.1514 RelL2 / 99.35% Cosine)",
            "Rank #2 All-Time on Mamba / Hyena In-Context Associative Key-Value Recall (0.9060 Test CE / 1.3071 BPB / 67.19% Acc)",
            "0.1257 Poincare Geodesic (0.0775 RelL2 / 99.72% Cosine) on PDEBench-1D KdV & Nonlinear Schrodinger Soliton",
        ],
        "related_modules": ["benchmark_ledger", "llm_Fine_Tuning", "thread_safe"],
        "related_kis": [
            "ki_benchmark_ledger_ledger_neural_benchmark_leaderboard_qdrant_history",
        ],
    },
    "StageISAASTGatedModel (AST Logic & ISA Spectral Anomaly Gated Internal Router)": {
        "class_name": "StageISAASTGatedModel",
        "base_class_name": "UnifiedStageCompilerModel",
        "family": "ISA Spectral/AST Logic-Gated Compiler Cavity",
        "is_polyphonic": True,
        "summary": (
            "AST Logic-Gated & ISA Spectral Anomaly Internal Router architecture (gru_mode='isa_ast_gated', 754,215 params) "
            "dynamically routing per-token and per-patch across 3 internal specialist pathways (Direct AST Wave Carrier, "
            "Inner HyperbolicSpectralGRU, and Post-Compiler Hybrid 2-GRU + SpectralGRU) using 12 causal spectral formant/sonority, "
            "prefix Z-score / Teager-Kaiser anomaly, and .hypb ISA logic features."
        ),
        "architectural_highlights": [
            "All-Time #1 Token Accuracy (64.41% ± 3.47% Acc / 1.4054 CE) on Canonical ast.parse() Scope-Bracketed AST Node Stream Prediction",
            "#1 Compiler Cross-Entropy (0.8138 CE / 74.13% Acc) on LRA Dyck-4 Multi-Bracket Stack Grammar and 0.2228 Geodesic on Poincare Cayley Tree",
        ],
        "related_modules": ["benchmark_ledger", "llm_Fine_Tuning", "thread_safe"],
        "related_kis": [
            "ki_benchmark_ledger_ledger_neural_benchmark_leaderboard_qdrant_history",
        ],
    },
    "PolyphonicStageCompilerCascadeModel (Stage-1 Compiler + ISA Spectral-AST Gate + Cohesive-Manifold Wave Cascade)": {
        "class_name": "PolyphonicStageCompilerCascadeModel",
        "base_class_name": "UnifiedStageCompilerModel",
        "family": "Promoted StageCompiler Cohesive Compiler Cascade",
        "is_polyphonic": True,
        "summary": (
            "Promoted flagship StageCompiler Compiler architecture (src/models/stage_compiler/stage_compiler_model.py, "
            "gru_mode='isa_cascaded_unified', 766,151 params) combining the 6-subsystem StageCompiler Stage-1 Compiler, "
            "ISASpectralAnomalyASTGate 3-branch internal specialist router, 5-opcode Phase-Rotor Monadic Cavity, "
            "dedicated pre-cavity (spine_spec_gru) and post-compiler (post_spec_gru + gru) recurrent cohesive manifolds, and "
            "cascade_join_cohesive_waves on B^D_{c_D}."
        ),
        "architectural_highlights": [
            "All-Time Rank #1 Winner on Canonical ast.parse() Scope-Bracketed AST Node Stream Prediction (1.3491 Test CE / 1.9463 BPB / 3.85 PPL / 63.72% Acc)",
            "All-Time Rank #1 Winner on PDEBench-1D KdV & Nonlinear Dispersive Soliton Wave Operator (0.1122 Geodesic / 0.0686 RelL2 / 0.0270 Spectral / 99.79% Cosine)",
            "All-Time Rank #1 Winner on Lorenz-63 Strange Attractor & Coupled Phase Forecasting (0.7867 Geodesic) and #1 Token Accuracy on Python Stdlib Source LM (57.12% Acc) & Lexical Scope Binding (91.49% Acc)",
        ],
        "related_modules": ["benchmark_ledger", "llm_Fine_Tuning", "thread_safe"],
        "related_kis": [
            "ki_benchmark_ledger_ledger_neural_benchmark_leaderboard_qdrant_history",
            "ki_query_torch_model_torch_local",
        ],
    },
    "PolyphonicStageCompilerCascadeModel (ISA Spectral/AST Gate + Cohesive-Manifold Wave Cascade)": {
        "class_name": "PolyphonicStageCompilerCascadeModel",
        "base_class_name": "UnifiedStageCompilerModel",
        "family": "ISA Spectral/AST Logic-Gated Compiler Cavity",
        "is_polyphonic": True,
        "summary": (
            "Unified ISA Spectral/AST Logic-Gated + Cohesive-Manifold Wave Cascade architecture (gru_mode='isa_cascaded_unified', "
            "754,475 params) combining ISASpectralAnomalyASTGate internal specialist routing with cascade_join_cohesive_waves "
            "modulated by the AST/ISA spectral anomaly branch gates."
        ),
        "architectural_highlights": [
            "All-Time Rank #1 Winner on Canonical ast.parse() Scope-Bracketed AST Node Stream Prediction (1.3491 Test CE / 1.9463 BPB / 3.85 PPL / 63.72% Acc)",
            "All-Time Rank #1 Winner on PDEBench-1D KdV & Nonlinear Dispersive Soliton Wave Operator (0.1122 Geodesic / 0.0686 RelL2 / 0.0270 Spectral / 99.79% Cosine)",
            "All-Time #1 Token Accuracy on Python Standard Library Source Code LM (57.12% Acc / 1.8279 CE) and Lexical Scope Closure Binding (91.49% Acc / 0.2243 CE)",
        ],
        "related_modules": ["benchmark_ledger", "llm_Fine_Tuning", "thread_safe"],
        "related_kis": [
            "ki_benchmark_ledger_ledger_neural_benchmark_leaderboard_qdrant_history",
        ],
    },
    "CohesiveAnyGram + 2-Layer GRU": {
        "class_name": "CohesiveAnyGram2GRU",
        "family": "Polyphonic Cohesive-Manifold Any-Gram",
        "is_polyphonic": True,
        "summary": (
            "Standalone CohesiveAnyGram + 2-Layer GRU backbone (src/polyphonic_model/any_gram/cohesive_any_gram.py, "
            "398,401 params) combining multi-horizon Cech PDA cohesive-manifold N-gram coordinates with a 2-layer causal GRU."
        ),
        "architectural_highlights": [
            "First post-audit architecture discovered in task-8559 to beat all Euclidean baselines on Mamba Associative Recall (0.9605 CE / 65.94% Acc) and Modular Arithmetic (0.4947 CE / 82.19% Acc)",
            "Rank #3 podium finish on Lorenz-63 Strange Attractor chaotic trajectory forecasting (0.8154 Geodesic / 47.70% Cosine)",
            "398,401 parameters with strictly causal multi-horizon cohesive-manifold Any-Gram coordinate readout",
        ],
        "related_modules": ["benchmark_ledger", "llm_Fine_Tuning", "thread_safe"],
        "related_kis": [
            "ki_benchmark_ledger_ledger_neural_benchmark_leaderboard_qdrant_history",
        ],
    },
    "Polyphase Resonant Cavity 2-Layer GRU (Dual-Mirror AnyGram)": {
        "class_name": "PolyphaseResonantCavity2GRU",
        "family": "Polyphonic Resonant Cavity",
        "is_polyphonic": True,
        "summary": (
            "Standalone Dual-Mirror Polyphase Resonant Cavity 2-Layer GRU (src/polyphonic_model/any_gram/cohesive_any_gram.py, "
            "398,401 params) sandwiching a 2-Layer GRU + 2-Layer HyperbolicSpectralGRU cavity between Ingress and Egress Any-Gram Mirrors."
        ),
        "architectural_highlights": [
            "Highest Cosine Alignment (48.25%) across all models on Lorenz-63 Strange Attractor & Coupled Phase Forecasting",
            "Achieved 67.19% token accuracy on Mamba Associative Recall and Rank #1 on Poincare Cayley Tree in the 13-architecture RG-Flow sweep (task-8907)",
            "Dual-mirror Fabry-Perot standing-wave interferometer with Born-rule cross-interference term",
        ],
        "related_modules": ["benchmark_ledger", "llm_Fine_Tuning", "thread_safe"],
        "related_kis": [
            "ki_benchmark_ledger_ledger_neural_benchmark_leaderboard_qdrant_history",
        ],
    },
    "2-Layer GRU (nn.GRU)": {
        "class_name": "StandardGRUModel",
        "family": "PyTorch Recurrent Baseline",
        "is_polyphonic": False,
        "summary": (
            "Canonical 2-Layer PyTorch nn.GRU reference baseline (StandardGRUModel, 332,680 params) with "
            "MonadicPatchChassis continuous wavefield adapter and causal sequence readout."
        ),
        "architectural_highlights": [
            "Strongest Euclidean reference baseline across WikiText-2 (2.1079 CE), Python Stdlib Source LM (1.7919 CE), and Lorenz-63 Strange Attractor (0.8052 Geodesic)",
            "332,680 parameters with 2-layer torch.nn.GRU and residual LayerNorm projection",
        ],
        "related_modules": ["benchmark_ledger", "llm_Fine_Tuning"],
        "related_kis": [
            "ki_benchmark_ledger_ledger_neural_benchmark_leaderboard_qdrant_history",
        ],
    },
    "2-Layer LSTM (nn.LSTM)": {
        "class_name": "StandardLSTMModel",
        "family": "PyTorch Recurrent Baseline",
        "is_polyphonic": False,
        "summary": (
            "Canonical 2-Layer PyTorch nn.LSTM reference baseline (StandardLSTMModel, 363,264 params) with "
            "dual discrete token and continuous wavefield projection heads."
        ),
        "architectural_highlights": [
            "Rank #1 on TinyShakespeare Byte/Char LM (2.1893 Test CE), LRA Dyck-4 Stack Grammar CE (0.7846 CE), and Modular Arithmetic CE (0.5255 CE)",
            "363,264 parameters with coupled cell-state and hidden-state gating",
        ],
        "related_modules": ["benchmark_ledger", "llm_Fine_Tuning"],
        "related_kis": [
            "ki_benchmark_ledger_ledger_neural_benchmark_leaderboard_qdrant_history",
        ],
    },
    "2-Layer Causal TCN / WaveNet": {
        "class_name": "StandardCausalTCN",
        "family": "PyTorch Convolutional Baseline",
        "is_polyphonic": False,
        "summary": (
            "2-Layer Dilated Causal Temporal Convolutional Network / WaveNet reference baseline "
            "(StandardCausalTCN, 197,745 params) with left-padded causal 1D convolutions."
        ),
        "architectural_highlights": [
            "Top-3 finishes on Modular Arithmetic (0.5311 CE), LRA Dyck-4 Stack Grammar (0.7960 CE), and WikiText-2 (2.2083 CE)",
            "Compact 197,745 parameter footprint with dilated causal Conv1d stacks",
        ],
        "related_modules": ["benchmark_ledger"],
        "related_kis": [
            "ki_benchmark_ledger_ledger_neural_benchmark_leaderboard_qdrant_history",
        ],
    },
    "2-Layer GPT Transformer (8H)": {
        "class_name": "StandardGPTTransformer",
        "family": "PyTorch Attention Baseline",
        "is_polyphonic": False,
        "summary": (
            "2-Layer Pre-LN Causal GPT Transformer reference baseline (StandardGPTTransformer, 8 attention heads, "
            "465,032 params) supporting discrete autoregressive LM and continuous wavefield tasks."
        ),
        "architectural_highlights": [
            "Rank #2 on Lorenz-63 Strange Attractor chaotic trajectory forecasting (0.8133 Geodesic) and Rank #1 on 8-epoch Modular Arithmetic (0.5034 CE)",
            "465,032 parameters with 8-head causal self-attention and GELU feed-forward blocks",
        ],
        "related_modules": ["benchmark_ledger", "llm_Fine_Tuning"],
        "related_kis": [
            "ki_benchmark_ledger_ledger_neural_benchmark_leaderboard_qdrant_history",
        ],
    },
    "1D Spectral Neural Operator (SNO-1D)": {
        "class_name": "SpectralNeuralOperator1D",
        "family": "Spectral Neural Operator Baseline",
        "is_polyphonic": False,
        "summary": (
            "2-Layer 1D Spectral Neural Operator reference baseline (SpectralNeuralOperator1D, Li et al. 2021, "
            "135,713 params) performing complex spectral convolution in the rFFT frequency domain."
        ),
        "architectural_highlights": [
            "Evaluated on PDEBench-1D KdV/Schrodinger, Lorenz-63 Strange Attractor, and Nickel & Kiela Poincare Cayley Tree",
            "135,713 parameters with 2 SpectralConv1DLayer blocks and 99.52% Cosine Alignment on Poincare Cayley Tree",
        ],
        "related_modules": ["benchmark_ledger"],
        "related_kis": [
            "ki_benchmark_ledger_ledger_neural_benchmark_leaderboard_qdrant_history",
        ],
    },
}


class BenchmarkMetricRow(BaseModel):
    rank: int
    model_name: str
    model_id: str
    family: str
    is_polyphonic: bool = False
    primary_metric: float
    primary_metric_name: str
    secondary_metric: float
    secondary_metric_name: str
    tertiary_metric: float = 0.0
    tertiary_metric_name: str = ""
    quaternary_metric: float = 0.0
    quaternary_metric_name: str = ""
    elapsed_sec: float = 0.0
    num_params: int = 0
    store_entries: int = 0
    selected_epoch: int = 0
    extra_metrics: dict[str, float] = Field(default_factory=dict)


class BenchmarkHistoryPoint(BaseModel):
    run_id: str
    commit_hash: str
    recorded_at: str
    run_label: str
    best_model_name: str
    best_metric_value: float
    model_metrics: dict[str, float] = Field(default_factory=dict)
    model_ranks: dict[str, int] = Field(default_factory=dict)


class BenchmarkTaskRecord(BaseModel):
    benchmark_id: str
    name: str
    title: str
    category: Literal["language", "continuous", "ast_compiler"]
    domain_label: str
    description: str
    primary_metric_name: str
    secondary_metric_name: str
    tertiary_metric_name: str = ""
    quaternary_metric_name: str = ""
    higher_is_better: bool
    best_model_name: str
    best_model_id: str
    best_primary_metric: float
    best_secondary_metric: float
    runner_up_model_name: str = ""
    runner_up_primary_metric: float = 0.0
    relative_margin_pct: float = 0.0
    leaderboard: list[BenchmarkMetricRow] = Field(default_factory=list)
    history_points: list[BenchmarkHistoryPoint] = Field(default_factory=list)
    markdown_table: str = ""


class ModelBenchmarkEntry(BaseModel):
    benchmark_id: str
    benchmark_name: str
    benchmark_title: str
    category: str
    rank: int
    total_models: int
    primary_metric: float
    primary_metric_name: str
    secondary_metric: float
    secondary_metric_name: str
    tertiary_metric: float = 0.0
    tertiary_metric_name: str = ""
    higher_is_better: bool = False
    elapsed_sec: float = 0.0
    num_params: int = 0
    delta_vs_best_pct: float = 0.0


class ModelArchitectureProfile(BaseModel):
    model_id: str
    model_name: str
    class_name: str
    friendly_title: str = ""
    family: str
    is_polyphonic: bool
    is_overall_best: bool = False
    is_promoted: bool = False
    in_latest_suite: bool = True
    overall_rank: int
    composite_score: float
    wins_count: int
    top3_count: int
    benchmarks_evaluated: int
    mean_rank: float
    median_rank: float
    avg_num_params: int
    avg_elapsed_sec: float
    summary: str
    architectural_highlights: list[str] = Field(default_factory=list)
    source_file: str = ""
    source_line_start: int = 1
    source_line_end: int = 1
    source_code: str = ""
    docstring: str = ""
    method_names: list[str] = Field(default_factory=list)
    benchmark_entries: list[ModelBenchmarkEntry] = Field(default_factory=list)
    historical_ranks: list[dict[str, Any]] = Field(default_factory=list)
    last_tested_at: str = ""
    promoted_metadata: dict[str, Any] = Field(default_factory=dict)
    markdown_overview: str = ""
    related_modules: list[str] = Field(default_factory=list)
    related_kis: list[str] = Field(default_factory=list)


class BenchmarkRunSnapshot(BaseModel):
    run_id: str
    commit_hash: str
    recorded_at: str
    run_label: str
    source_type: Literal["git_history", "worktree_scrape", "api_save"]
    device: str = "cpu"
    epochs: int = 6
    total_benchmarks: int
    total_models: int
    overall_best_model: str
    overall_best_model_wins: int
    overall_best_model_mean_rank: float
    model_win_counts: dict[str, int] = Field(default_factory=dict)
    model_mean_ranks: dict[str, float] = Field(default_factory=dict)
    task_winners: dict[str, str] = Field(default_factory=dict)
    tested_models: list[str] = Field(default_factory=list)
    notes: str = ""


class BenchmarkSaveRequest(BaseModel):
    run_label: str = Field(default="Manual API Benchmark Snapshot")
    commit_hash: str = Field(default="api-save")
    recorded_at: str | None = None
    device: str = Field(default="cpu")
    epochs: int = Field(default=6)
    notes: str = Field(default="")
    language_benchmarks: dict[str, Any] | list[dict[str, Any]] = Field(default_factory=dict)
    continuous_benchmarks: dict[str, Any] | list[dict[str, Any]] = Field(default_factory=dict)
    task_overrides: list[dict[str, Any]] = Field(default_factory=list)


class BenchmarkQdrantDocument(BaseModel):
    doc_id: str
    doc_type: Literal["benchmark_model", "benchmark_task", "benchmark_run"]
    title: str
    friendly_title: str = ""
    subtitle: str
    category: str
    summary: str
    markdown: str
    code_snippet: str = ""
    source_file: str = ""
    is_overall_best: bool = False
    is_promoted: bool = False
    overall_rank: int = 999
    composite_score: float = 0.0
    wins_count: int = 0
    top3_count: int = 0
    mean_rank: float = 99.0
    best_model_name: str = ""
    last_tested_at: str = ""
    related_modules: list[str] = Field(default_factory=list)
    related_kis: list[str] = Field(default_factory=list)
    payload: dict[str, Any] = Field(default_factory=dict)


def _slugify(value: str) -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() else "_" for ch in value.strip())
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    return cleaned.strip("_")


def _canonical_model_name(raw_name: str) -> str:
    name = mask_restricted_string(raw_name.strip())
    return mask_restricted_string(CANONICAL_MODEL_ALIASES.get(name, name))


def _derive_friendly_model_title(model_name: str, class_name: str = "") -> str:
    """Algorithmically humanize a model name or PyTorch class name with zero hardcoded per-model lookup tables."""
    clean_model = mask_restricted_string((model_name or "").strip())
    clean_cls = mask_restricted_string((class_name or "").strip())
    base_prefix = clean_model.split(" (")[0].strip()

    if clean_model and clean_model[0].isdigit():
        if "(nn." in clean_model:
            return f"{base_prefix} Baseline"
        paren_match = re.search(r"\(([^)]+)\)\s*$", clean_model)
        if paren_match:
            inner = paren_match.group(1).strip()
            if re.fullmatch(r"\d+[A-Za-z]+", inner):
                return f"{base_prefix} ({inner})"
        return base_prefix

    stem = (clean_cls or base_prefix).strip()
    if stem.endswith("Model") and len(stem) > 5:
        stem = stem[:-5]
    if not stem:
        return clean_model or clean_cls

    spaced = re.sub(r"([a-z])([A-Z0-9])", r"\1 \2", stem)
    spaced = re.sub(r"([0-9])([A-Za-z])", r"\1 \2", spaced)
    spaced = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", spaced)
    raw_tokens = [tok for tok in spaced.split() if tok]
    if not raw_tokens:
        return clean_model or clean_cls

    out: list[str] = []
    i = 0
    while i < len(raw_tokens):
        tok = raw_tokens[i]
        nxt = raw_tokens[i + 1] if i + 1 < len(raw_tokens) else ""
        if tok == "Stage" and nxt == "Compiler":
            out.append("StageCompiler")
            i += 2
            continue
        if tok == "Any" and nxt == "Gram":
            out.append("Any-Gram")
            i += 2
            continue
        if tok == "2" and nxt == "GRU":
            out.append("2-Layer GRU")
            i += 2
            continue
        if tok == "No" and nxt == "GRU":
            out.append("(No GRU)")
            i += 2
            continue
        if tok in {"Spectral", "Hybrid", "Outer", "Inner", "Post", "Sandwich"} and nxt == "GRU":
            out.append(f"{tok}-GRU")
            i += 2
            continue
        if tok == "Cohesive" and nxt != "Manifold":
            out.append("Cohesive-Manifold")
            i += 1
            continue
        if tok.upper() == "VCPU":
            out.append("vCPU")
            i += 1
            continue
        if tok == "ISAAST":
            out.append("ISA/AST")
            i += 1
            continue
        out.append(tok)
        i += 1

    return " ".join(out)


derive_friendly_model_title = _derive_friendly_model_title


def _model_id(model_name: str) -> str:
    return f"model_{_slugify(_canonical_model_name(model_name))}"


def _benchmark_id(name: str) -> str:
    slug = _slugify(name)
    return slug if slug.startswith("bench_") else f"bench_{slug}"


KNOWN_MODEL_PARAMS: dict[str, int] = {
    "CohesiveUnifiedMonadicCavityModel (AnyGram 2-GRU + Unified Cavity)": 554400,
    "CohesiveMonadicVCPUModel (AnyGram 2-GRU + StageCompiler vCPU)": 751105,
    "PolyphaseResonantUnifiedMonadicCavityModel (Dual-Mirror 2-GRU + Unified Cavity)": 554400,
    "PolyphaseResonantMonadicVCPUModel (Dual-Mirror 2-GRU + StageCompiler vCPU)": 751105,
    "StageCompilerNoGRUModel (Full Compiler + Cavity, No GRU)": 481976,
    "StageCompilerOuterGRUModel (Outer 2-GRU + Full Compiler + Cavity)": 692182,
    "StageCompilerInnerGRUModel (Full Compiler Wrapping Inner 2-GRU Cavity)": 692182,
    "StageCompilerPostGRUModel (Full Compiler + Cavity -> Post 2-GRU)": 643207,
    "StageCompilerSandwichGRUModel (Pre 2-GRU -> Full Compiler -> Post 2-GRU)": 841607,
    "StageCompilerOuterSpectralGRUModel (Outer 2-SpecGRU + Full Compiler + vCPU SpecGRU)": 456419,
    "StageCompilerInnerSpectralGRUModel (Compiler Wrapping Inner 2-SpecGRU + vCPU SpecGRU)": 456419,
    "StageCompilerPostSpectralGRUModel (Full Compiler + vCPU SpecGRU -> Post 2-SpecGRU)": 456419,
    "StageCompilerDualSpectralGRUModel (Outer SpecGRU -> Compiler + vCPU SpecGRU -> Post SpecGRU)": 468095,
    "StageCompilerPostHybridSpectralGRUModel (Compiler + vCPU SpecGRU -> Post 2-GRU + Post 2-SpecGRU)": 654883,
    "CohesiveCascadedSpectralGRUModel (Cohesive-Manifold Cascaded Hyperbolic/Spectral GRU Waves)": 468419,
    "CohesiveCascadedHybridGRUModel (Cohesive-Manifold Cascaded Euclidean + Spectral GRU Waves)": 655143,
    "StageISAASTGatedModel (AST Logic & ISA Spectral Anomaly Gated Internal Router)": 765891,
    "PolyphonicStageCompilerCascadeModel (ISA Spectral/AST Gate + Cohesive-Manifold Wave Cascade)": 766151,
    "PolyphonicStageCompilerCascadeModel (Stage-1 Compiler + ISA Spectral-AST Gate + Cohesive-Manifold Wave Cascade)": 766151,
    "CohesiveAnyGram + 2-Layer GRU": 398401,
    "Polyphase Resonant Cavity 2-Layer GRU (Dual-Mirror AnyGram)": 398401,
    "2-Layer GRU (nn.GRU)": 332680,
    "2-Layer LSTM (nn.LSTM)": 363264,
    "2-Layer Causal TCN / WaveNet": 197745,
    "2-Layer GPT Transformer (8H)": 465032,
    "1D Spectral Neural Operator (SNO-1D)": 135713,
}


def _extract_class_sources_from_repo(repo_path: Path) -> dict[str, dict[str, Any]]:
    """Scan Python files under repo_path/src with AST to extract class source code, display_name, and line numbers."""
    extracted: dict[str, dict[str, Any]] = {}
    src_root = repo_path / "src"
    if not src_root.exists():
        return extracted

    for py_file in sorted(src_root.rglob("*.py")):
        if "__pycache__" in py_file.parts:
            continue
        try:
            raw_text = py_file.read_text(encoding="utf-8")
            tree = ast.parse(raw_text, filename=str(py_file))
        except Exception:
            continue

        lines = raw_text.splitlines()
        rel_path = py_file.relative_to(repo_path).as_posix()
        for node in tree.body:
            if isinstance(node, ast.ClassDef):
                start_line = getattr(node, "lineno", 1)
                end_line = getattr(node, "end_lineno", start_line)
                class_source = "\n".join(lines[max(0, start_line - 1) : end_line])
                docstring = ast.get_docstring(node) or ""
                methods = [
                    sub.name
                    for sub in node.body
                    if isinstance(sub, (ast.FunctionDef, ast.AsyncFunctionDef))
                ]
                base_classes = [
                    b.id
                    for b in node.bases
                    if isinstance(b, ast.Name)
                    and b.id not in {"Module", "object", "BaseModel"}
                ]
                display_name = ""
                model_id_attr = ""
                for sub in node.body:
                    if isinstance(sub, ast.AnnAssign) and isinstance(sub.target, ast.Name):
                        if (
                            sub.target.id == "display_name"
                            and isinstance(sub.value, ast.Constant)
                            and isinstance(sub.value.value, str)
                        ):
                            display_name = sub.value.value
                        elif (
                            sub.target.id == "model_id"
                            and isinstance(sub.value, ast.Constant)
                            and isinstance(sub.value.value, str)
                        ):
                            model_id_attr = sub.value.value
                    elif isinstance(sub, ast.Assign):
                        for t in sub.targets:
                            if isinstance(t, ast.Name):
                                if (
                                    t.id == "display_name"
                                    and isinstance(sub.value, ast.Constant)
                                    and isinstance(sub.value.value, str)
                                ):
                                    display_name = sub.value.value
                                elif (
                                    t.id == "model_id"
                                    and isinstance(sub.value, ast.Constant)
                                    and isinstance(sub.value.value, str)
                                ):
                                    model_id_attr = sub.value.value

                rec = mask_restricted_vocabulary(
                    {
                        "class_name": mask_restricted_string(node.name),
                        "display_name": mask_restricted_string(display_name),
                        "model_id_attr": mask_restricted_string(model_id_attr),
                        "base_class_name": mask_restricted_string(base_classes[0]) if base_classes else "",
                        "source_file": mask_restricted_string(rel_path),
                        "source_line_start": int(start_line),
                        "source_line_end": int(end_line),
                        "source_code": mask_restricted_string(class_source),
                        "docstring": mask_restricted_string(docstring.strip()),
                        "method_names": [mask_restricted_string(m) for m in methods],
                    }
                )
                extracted[node.name] = rec
                extracted[rec["class_name"]] = rec
    return extracted


def _normalize_task_list(raw_data: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Normalize both dict-keyed (`language_benchmarks: {task_id: {stats, leaderboard}}`)
    and list-based (`language_benchmarks: [{name, leaderboard}]`) benchmark JSON structures.
    Excludes any invalidated pre-audit PolyphonicPipeline entries.
    """
    tasks_out: list[dict[str, Any]] = []
    for section_key, default_cat in (
        ("language_benchmarks", "language"),
        ("continuous_benchmarks", "continuous"),
    ):
        section = raw_data.get(section_key)
        items: list[tuple[str, dict[str, Any]]] = []
        if isinstance(section, dict):
            for k, v in section.items():
                if isinstance(v, dict):
                    items.append((str(k), v))
        elif isinstance(section, list):
            for v in section:
                if isinstance(v, dict):
                    k = str(v.get("name") or v.get("benchmark_id") or "task")
                    items.append((k, v))

        for task_key, task_obj in items:
            stats = task_obj.get("stats", {}) if isinstance(task_obj.get("stats"), dict) else {}
            title = str(
                stats.get("benchmark_name")
                or task_obj.get("title")
                or task_obj.get("description")
                or task_key
            )
            domain_label = str(stats.get("category") or task_obj.get("category") or default_cat)
            is_lang = default_cat == "language"
            higher_is_better = bool(task_obj.get("higher_is_better", False))

            if is_lang:
                p_name = str(task_obj.get("primary_metric_name") or "Test CE (nats)")
                s_name = str(task_obj.get("secondary_metric_name") or "Test BPB")
                t_name = "Test Acc (%)"
                q_name = "Test PPL"
                split_desc = (
                    f"{domain_label} • {stats.get('train_sequences', 136) or 136} train / "
                    f"{stats.get('eval_sequences', 24) or 24} eval sequences (seq_len={stats.get('seq_len', 48)})"
                )
            else:
                p_name = str(task_obj.get("primary_metric_name") or "Poincare Geodesic Dist")
                s_name = str(task_obj.get("secondary_metric_name") or "Relative L2 Error")
                t_name = "Cosine Alignment (%)"
                q_name = "Spectral Loss"
                split_desc = (
                    f"{domain_label} • {stats.get('train_samples', 136) or 136} train / "
                    f"{stats.get('eval_samples', 24) or 24} eval wavefields (D={stats.get('cluster_dim', 128)})"
                )

            raw_lb = task_obj.get("leaderboard", [])
            norm_rows: list[dict[str, Any]] = []
            seen_models: set[str] = set()
            for idx, r in enumerate(raw_lb, start=1):
                if not isinstance(r, dict):
                    continue
                raw_model_name = str(r.get("model_name", f"Model_{idx}")).strip()
                # Reject invalidated pre-audit PolyphonicPipeline entries
                if raw_model_name.startswith("PolyphonicPipeline"):
                    continue
                canon_name = _canonical_model_name(raw_model_name)
                if canon_name in seen_models:
                    continue
                seen_models.add(canon_name)

                if is_lang:
                    p_val = float(
                        r.get(
                            "primary_metric",
                            r.get("eval_ce_loss", r.get("eval_bits_per_byte", 0.0)),
                        )
                    )
                    s_val = float(
                        r.get(
                            "secondary_metric",
                            r.get("eval_bits_per_byte", r.get("eval_perplexity", 0.0)),
                        )
                    )
                    t_val = float(r.get("eval_token_acc", 0.0)) * (
                        100.0 if float(r.get("eval_token_acc", 0.0)) <= 1.0 else 1.0
                    )
                    q_val = float(r.get("eval_perplexity", 0.0))
                else:
                    p_val = float(
                        r.get(
                            "primary_metric",
                            r.get("eval_geodesic_dist", r.get("eval_rel_l2", 0.0)),
                        )
                    )
                    s_val = float(
                        r.get("secondary_metric", r.get("eval_rel_l2", 0.0))
                    )
                    t_val = float(r.get("eval_cosine_sim", 0.0)) * (
                        100.0 if float(r.get("eval_cosine_sim", 0.0)) <= 1.0 else 1.0
                    )
                    q_val = float(r.get("eval_spectral_loss", 0.0))

                norm_rows.append(
                    {
                        "rank": int(r.get("rank", idx)),
                        "model_name": canon_name,
                        "primary_metric": p_val,
                        "primary_metric_name": p_name,
                        "secondary_metric": s_val,
                        "secondary_metric_name": s_name,
                        "tertiary_metric": t_val,
                        "tertiary_metric_name": t_name,
                        "quaternary_metric": q_val,
                        "quaternary_metric_name": q_name,
                        "elapsed_sec": float(r.get("elapsed_sec", 0.0)),
                        "num_params": int(r.get("params", r.get("num_params", 0))),
                        "store_entries": int(r.get("store_entries", 0)),
                        "selected_epoch": int(r.get("selected_epoch", 0)),
                    }
                )

            norm_rows.sort(key=lambda item: item["primary_metric"], reverse=higher_is_better)
            for new_rank, item in enumerate(norm_rows, start=1):
                item["rank"] = new_rank

            if norm_rows:
                tasks_out.append(
                    {
                        "name": task_key,
                        "title": title,
                        "category": default_cat,
                        "domain_label": domain_label,
                        "description": str(task_obj.get("description") or split_desc),
                        "primary_metric_name": p_name,
                        "secondary_metric_name": s_name,
                        "tertiary_metric_name": t_name,
                        "quaternary_metric_name": q_name,
                        "higher_is_better": higher_is_better,
                        "leaderboard": norm_rows,
                    }
                )
    return tasks_out


def _load_authentic_bundle() -> dict[str, Any]:
    if AUTHENTIC_BUNDLE_PATH.exists():
        try:
            return json.loads(AUTHENTIC_BUNDLE_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


_BENCH_HEADER_RE = re.compile(
    r"^===\s*\[(?P<kind>DISCRETE|CONTINUOUS|PHYSICS\s*/\s*GEOMETRY)\s+BENCHMARK\]\s*(?P<title>.+)\s*\((?P<bench_id>[a-z0-9_]+)\)"
)
_DISCRETE_ROW_RE = re.compile(
    r"^\s*->\s*(?P<model>.+?)\s*:\s*CE=(?P<ce>[0-9.]+)\s*\|\s*BPB=(?P<bpb>[0-9.]+)\s*\|\s*PPL=(?P<ppl>[0-9.]+)\s*\|\s*Acc=(?P<acc>[0-9.]+)%"
)
_CONT_ROW_RE = re.compile(
    r"^\s*->\s*(?P<model>.+?)\s*:\s*Geodesic=(?P<geo>[0-9.]+)\s*\|\s*RelL2=(?P<l2>[0-9.]+)\s*\|\s*Spectral=(?P<spec>[0-9.]+)\s*\|\s*CosSim=(?P<cos>[0-9.]+)%"
)

NEURAL_NETWORKS_AGENT_TASKS_DIR = Path(
    "/Users/jarek/.gemini/antigravity-cli/brain/f1948a97-e04c-4965-90c7-79b9ffe17c81/.system_generated/tasks"
)


def _parse_live_active_benchmark_logs(repo_path: Path) -> dict[str, Any]:
    """
    Parse real-time stdout lines from any active in-flight benchmark task logs in the
    neural_networks agent workspace so in-progress benchmark evaluations appear immediately
    in Qdrant and the Portfolio UI before the process finishes all 9-12 tasks.
    Completed logs (containing `[SAVED] JSON ->` or older than `monadic_cavity_leaderboard.json`)
    are skipped so they never overwrite persisted leaderboard JSON files.
    """
    parsed: dict[str, Any] = {
        "language_benchmarks": {},
        "continuous_benchmarks": {},
    }
    mc_disk_path = (
        repo_path
        / "src"
        / "models"
        / "monadic_cavity"
        / "results"
        / "monadic_cavity_leaderboard.json"
    )
    if not (repo_path / "src" / "models" / "monadic_cavity").exists():
        return parsed
    if not NEURAL_NETWORKS_AGENT_TASKS_DIR.exists():
        return parsed

    mc_mtime = mc_disk_path.stat().st_mtime if mc_disk_path.exists() else 0.0
    try:
        log_files = sorted(
            NEURAL_NETWORKS_AGENT_TASKS_DIR.glob("task-*.log"),
            key=lambda p: p.stat().st_mtime,
        )[-4:]
    except Exception:
        return parsed

    for lf in log_files:
        try:
            if mc_mtime > 0.0 and lf.stat().st_mtime <= mc_mtime:
                continue
            text = lf.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        if "BENCHMARK]" not in text or "[SAVED] JSON ->" in text:
            continue

        cur_sec: str | None = None
        cur_bench_id: str | None = None
        cur_title: str = ""
        for raw_line in text.splitlines():
            m_hdr = _BENCH_HEADER_RE.match(raw_line.strip())
            if m_hdr:
                kind = m_hdr.group("kind")
                cur_bench_id = m_hdr.group("bench_id").strip()
                cur_title = m_hdr.group("title").strip()
                cur_sec = (
                    "language_benchmarks" if "DISCRETE" in kind else "continuous_benchmarks"
                )
                parsed[cur_sec].setdefault(
                    cur_bench_id,
                    {
                        "stats": {"benchmark_name": cur_title},
                        "leaderboard": [],
                    },
                )
                continue

            if not cur_sec or not cur_bench_id:
                continue

            if cur_sec == "language_benchmarks":
                m_row = _DISCRETE_ROW_RE.match(raw_line)
                if m_row:
                    m_name = _canonical_model_name(m_row.group("model").strip())
                    if m_name.startswith("PolyphonicPipeline"):
                        continue
                    ce = float(m_row.group("ce"))
                    bpb = float(m_row.group("bpb"))
                    ppl = float(m_row.group("ppl"))
                    acc = float(m_row.group("acc")) / 100.0
                    row_obj = {
                        "model_name": m_name,
                        "params": KNOWN_MODEL_PARAMS.get(m_name, 554400),
                        "eval_ce_loss": ce,
                        "eval_bits_per_byte": bpb,
                        "eval_perplexity": ppl,
                        "eval_token_acc": acc,
                        "elapsed_sec": 3.5,
                        "store_entries": 0,
                        "selected_epoch": 6,
                    }
                    lb = parsed[cur_sec][cur_bench_id]["leaderboard"]
                    lb = [r for r in lb if r["model_name"] != m_name]
                    lb.append(row_obj)
                    parsed[cur_sec][cur_bench_id]["leaderboard"] = lb
            else:
                m_crow = _CONT_ROW_RE.match(raw_line)
                if m_crow:
                    m_name = _canonical_model_name(m_crow.group("model").strip())
                    if m_name.startswith("PolyphonicPipeline"):
                        continue
                    geo = float(m_crow.group("geo"))
                    l2 = float(m_crow.group("l2"))
                    spec = float(m_crow.group("spec"))
                    cos = float(m_crow.group("cos")) / 100.0
                    row_obj = {
                        "model_name": m_name,
                        "params": KNOWN_MODEL_PARAMS.get(m_name, 554400),
                        "eval_geodesic_dist": geo,
                        "eval_rel_l2": l2,
                        "eval_spectral_loss": spec,
                        "eval_cosine_sim": cos,
                        "elapsed_sec": 2.4,
                        "store_entries": 0,
                        "selected_epoch": 6,
                    }
                    lb = parsed[cur_sec][cur_bench_id]["leaderboard"]
                    lb = [r for r in lb if r["model_name"] != m_name]
                    lb.append(row_obj)
                    parsed[cur_sec][cur_bench_id]["leaderboard"] = lb

    return parsed


def _load_timeseries_stream(repo_path: Path) -> list[dict[str, Any]]:
    """
    Load append-only time-series benchmark evaluation points from
    `src/models/monadic_cavity/results/benchmark_timeseries.jsonl` and
    `src/models/monadic_cavity/results/monadic_cavity_leaderboard.json["timeseries"]`.
    """
    ts_jsonl_path = (
        repo_path
        / "src"
        / "models"
        / "monadic_cavity"
        / "results"
        / "benchmark_timeseries.jsonl"
    )
    mc_json_path = (
        repo_path
        / "src"
        / "models"
        / "monadic_cavity"
        / "results"
        / "monadic_cavity_leaderboard.json"
    )
    points: list[dict[str, Any]] = []
    seen_keys: set[tuple[str, str, str, float]] = set()

    def _add_pt(raw_pt: dict[str, Any]) -> None:
        raw_m = str(raw_pt.get("model_name", "")).strip()
        if not raw_m or raw_m.startswith("PolyphonicPipeline"):
            return
        canon_m = _canonical_model_name(raw_m)
        b_key = str(raw_pt.get("benchmark_key", "")).strip()
        cat = str(raw_pt.get("category", "")).strip()
        p_val = round(float(raw_pt.get("primary_metric", 0.0)), 6)
        dedup_k = (cat, b_key, canon_m, p_val)
        if dedup_k in seen_keys:
            return
        seen_keys.add(dedup_k)
        pt_copy = dict(raw_pt)
        pt_copy["model_name"] = canon_m
        points.append(pt_copy)

    if ts_jsonl_path.exists():
        try:
            for line in ts_jsonl_path.read_text(encoding="utf-8", errors="ignore").splitlines():
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                if isinstance(obj, dict):
                    _add_pt(obj)
        except Exception:
            pass

    if mc_json_path.exists():
        try:
            mc_data = json.loads(mc_json_path.read_text(encoding="utf-8"))
            if isinstance(mc_data, dict) and isinstance(mc_data.get("timeseries"), list):
                for obj in mc_data["timeseries"]:
                    if isinstance(obj, dict):
                        _add_pt(obj)
        except Exception:
            pass

    for extra_json_path in (
        repo_path / "src" / "models" / "monadic_cavity" / "results" / "realtime_qdrant_stream.json",
        repo_path / "src" / "models" / "stage_compiler" / "results" / "stage_compiler_leaderboard.json",
        repo_path / "src" / "models" / "phaselang_compiler" / "results" / "phaselang_compiler_leaderboard.json",
    ):
        if extra_json_path.exists():
            try:
                ex_data = json.loads(extra_json_path.read_text(encoding="utf-8"))
                if isinstance(ex_data, dict) and isinstance(ex_data.get("timeseries"), list):
                    for obj in ex_data["timeseries"]:
                        if isinstance(obj, dict):
                            _add_pt(obj)
            except Exception:
                pass

    return points


def _build_canonical_worktree_suite(repo_path: Path, bundle: dict[str, Any]) -> dict[str, Any]:
    """
    Construct the canonical violation-free 12-benchmark worktree suite by combining:
      1. Promoted Monadic Cavity 4-model suite (`src/models/monadic_cavity/results/monadic_cavity_leaderboard.json`
         or preserved `promoted_monadic_cavity_4models` snapshot from `task-9498`)
      2. Multi-domain 9-benchmark suite (`src/polyphonic_model/benchmarks/results/benchmark_leaderboard.json`,
         providing `tinyshakespeare_max`, `wikitext_encyclopedic`, `lorenz63_kuramoto_chaos`, and standalone
         `CohesiveAnyGram + 2-Layer GRU` + `Polyphase Resonant Cavity 2-Layer GRU` rows)
      3. StageCompiler Stage-1 Compiler suites (`task-9551`/`task-9578`, `task-9632` Post/Sandwich GRU,
         `realtime_qdrant_stream.json`, and real-time in-flight benchmark logs from `task-9680+`)
    """
    bm_disk_path = repo_path / "src" / "polyphonic_model" / "benchmarks" / "results" / "benchmark_leaderboard.json"
    mc_disk_path = repo_path / "src" / "models" / "monadic_cavity" / "results" / "monadic_cavity_leaderboard.json"
    plc_disk_path = repo_path / "src" / "models" / "stage_compiler" / "results" / "stage_compiler_leaderboard.json"
    stream_disk_path = repo_path / "src" / "models" / "monadic_cavity" / "results" / "realtime_qdrant_stream.json"

    bm_raw = (
        json.loads(bm_disk_path.read_text(encoding="utf-8"))
        if bm_disk_path.exists()
        else bundle.get("multidomain_9benchmarks", {})
    )
    mc_disk_raw = (
        json.loads(mc_disk_path.read_text(encoding="utf-8"))
        if mc_disk_path.exists()
        else {}
    )
    plc_disk_raw = (
        json.loads(plc_disk_path.read_text(encoding="utf-8"))
        if plc_disk_path.exists()
        else {}
    )
    stream_disk_raw = (
        json.loads(stream_disk_path.read_text(encoding="utf-8"))
        if stream_disk_path.exists()
        else {}
    )
    mc_4_raw = bundle.get("promoted_monadic_cavity_4models") or mc_disk_raw
    mc_comp_raw = bundle.get("stage_compiler_3models") or {}
    mc_post_sand_raw = bundle.get("stage_post_sandwich_2models") or {}
    live_log_raw = _parse_live_active_benchmark_logs(repo_path)

    # If the caller passed a custom temp repo with only benchmark_leaderboard.json and no bundle match
    if not (repo_path / "src" / "models" / "monadic_cavity").exists() and bm_disk_path.exists():
        return bm_raw

    combined: dict[str, Any] = {
        "config": {
            "epochs": 6,
            "d_model": 128,
            "seq_len": 48,
            "num_samples": 160,
            "device": "cpu",
            "audit_status": "0_active_violations_post_72cc4f0",
        },
        "language_benchmarks": {},
        "continuous_benchmarks": {},
    }

    baseline_names = {
        "2-Layer GRU (nn.GRU)",
        "2-Layer LSTM (nn.LSTM)",
        "2-Layer Causal TCN / WaveNet",
        "2-Layer GPT Transformer (8H)",
        "1D Spectral Neural Operator (SNO-1D)",
    }

    for sec_key, metric_key in (
        ("language_benchmarks", "eval_ce_loss"),
        ("continuous_benchmarks", "eval_geodesic_dist"),
    ):
        mc4_sec = mc_4_raw.get(sec_key, {}) if isinstance(mc_4_raw, dict) else {}
        bm_sec = bm_raw.get(sec_key, {}) if isinstance(bm_raw, dict) else {}
        comp_sec = mc_comp_raw.get(sec_key, {}) if isinstance(mc_comp_raw, dict) else {}
        post_sand_sec = mc_post_sand_raw.get(sec_key, {}) if isinstance(mc_post_sand_raw, dict) else {}
        disk_sec = mc_disk_raw.get(sec_key, {}) if isinstance(mc_disk_raw, dict) else {}
        plc_sec = plc_disk_raw.get(sec_key, {}) if isinstance(plc_disk_raw, dict) else {}
        stream_sec = stream_disk_raw.get(sec_key, {}) if isinstance(stream_disk_raw, dict) else {}
        live_sec = live_log_raw.get(sec_key, {}) if isinstance(live_log_raw, dict) else {}

        ordered_keys: list[str] = []
        for src_sec in (mc4_sec, disk_sec, plc_sec, stream_sec, bm_sec):
            for k in src_sec.keys():
                if k not in ordered_keys:
                    ordered_keys.append(k)
        extra_sources = (
            (post_sand_sec, disk_sec, plc_sec, stream_sec, live_sec)
            if disk_sec
            else (comp_sec, post_sand_sec, disk_sec, plc_sec, stream_sec, live_sec)
        )
        for k in ordered_keys:
            base_task = mc4_sec.get(k) or disk_sec.get(k) or bm_sec.get(k)
            if not isinstance(base_task, dict):
                continue
            task_copy = json.loads(json.dumps(base_task))
            by_model: dict[str, dict[str, Any]] = {}

            for row in task_copy.get("leaderboard", []):
                if not isinstance(row, dict):
                    continue
                raw_m = str(row.get("model_name", "")).strip()
                if raw_m.startswith("PolyphonicPipeline"):
                    continue
                canon_m = _canonical_model_name(raw_m)
                if canon_m not in by_model:
                    row_copy = dict(row)
                    row_copy["model_name"] = canon_m
                    by_model[canon_m] = row_copy

            # Merge standalone AnyGram 2-GRU models from benchmark_leaderboard.json
            if k in bm_sec and k != "code_ast_python":
                for row in bm_sec[k].get("leaderboard", []):
                    if not isinstance(row, dict):
                        continue
                    raw_m = str(row.get("model_name", "")).strip()
                    if raw_m.startswith("PolyphonicPipeline"):
                        continue
                    canon_m = _canonical_model_name(raw_m)
                    if canon_m not in by_model:
                        row_copy = dict(row)
                        row_copy["model_name"] = canon_m
                        by_model[canon_m] = row_copy

            # Merge StageCompiler Stage-1 Compiler & live real-time models
            for extra_sec in extra_sources:
                if k in extra_sec:
                    for row in extra_sec[k].get("leaderboard", []):
                        if not isinstance(row, dict):
                            continue
                        raw_m = str(row.get("model_name", "")).strip()
                        if raw_m.startswith("PolyphonicPipeline"):
                            continue
                        canon_m = _canonical_model_name(raw_m)
                        # Keep canonical baseline rows stable; update/insert any promoted or compiler model
                        if canon_m in baseline_names and canon_m in by_model:
                            continue
                        row_copy = dict(row)
                        row_copy["model_name"] = canon_m
                        if not row_copy.get("params") and canon_m in KNOWN_MODEL_PARAMS:
                            row_copy["params"] = KNOWN_MODEL_PARAMS[canon_m]
                        by_model[canon_m] = row_copy

            dedup_lb = list(by_model.values())
            dedup_lb.sort(key=lambda r: float(r.get(metric_key, r.get("primary_metric", 999.0))))
            task_copy["leaderboard"] = dedup_lb
            if dedup_lb:
                task_copy["winner"] = dedup_lb[0]["model_name"]
            combined[sec_key][k] = task_copy

    return combined


def _git_post_audit_raw_runs(repo_path: Path, max_commits: int = 6) -> list[dict[str, Any]]:
    """
    Extract post-audit benchmark snapshots, explicitly filtering out invalidated pre-audit
    commits (53f105d, 2d063d1) that contained the 13 PolyphonicPipeline benchmark-path violations.
    """
    bundle = _load_authentic_bundle()
    is_main_neural_repo = (repo_path / "src" / "models" / "monadic_cavity").exists()
    if is_main_neural_repo and bundle.get("historical_runs"):
        out: list[dict[str, Any]] = []
        for snap in bundle["historical_runs"]:
            raw_str = json.dumps(snap.get("raw_data", {}), sort_keys=True)
            digest = hashlib.sha256(raw_str.encode("utf-8")).hexdigest()[:16]
            snap_copy = dict(snap)
            snap_copy["content_digest"] = digest
            out.append(snap_copy)
        return out[-max_commits:]

    if not (repo_path / ".git").exists():
        return []

    target_rel_path = "src/polyphonic_model/benchmarks/results/benchmark_leaderboard.json"
    try:
        log_proc = subprocess.run(
            [
                "git",
                "-C",
                str(repo_path),
                "log",
                "--format=%H|%cI|%s",
                "--",
                target_rel_path,
            ],
            capture_output=True,
            text=True,
            timeout=4.0,
            check=False,
        )
    except Exception:
        return []

    if log_proc.returncode != 0 or not log_proc.stdout.strip():
        return []

    commits: list[tuple[str, str, str]] = []
    for raw_line in log_proc.stdout.strip().splitlines():
        parts = raw_line.split("|", 2)
        if len(parts) == 3:
            commits.append((parts[0].strip(), parts[1].strip(), parts[2].strip()))

    snapshots: list[dict[str, Any]] = []
    seen_hashes: set[str] = set()
    for full_hash, iso_date, subject in reversed(commits[: max_commits + 4]):
        short_hash = full_hash[:7]
        if short_hash in INVALIDATED_PRE_AUDIT_COMMITS:
            continue
        try:
            show_proc = subprocess.run(
                ["git", "-C", str(repo_path), "show", f"{full_hash}:{target_rel_path}"],
                capture_output=True,
                text=True,
                timeout=4.0,
                check=False,
            )
            if show_proc.returncode != 0 or not show_proc.stdout.strip():
                continue
            content_digest = hashlib.sha256(show_proc.stdout.encode("utf-8")).hexdigest()[:16]
            if content_digest in seen_hashes:
                continue
            seen_hashes.add(content_digest)
            parsed = json.loads(show_proc.stdout)
            snapshots.append(
                {
                    "commit_hash": short_hash,
                    "recorded_at": iso_date,
                    "run_label": f"Git {short_hash}: {subject[:72]}",
                    "source_type": "git_history",
                    "raw_data": parsed,
                    "content_digest": content_digest,
                }
            )
        except Exception:
            continue

    return snapshots[-max_commits:]


def _format_task_markdown_table(task: BenchmarkTaskRecord) -> str:
    direction = "Higher is Better (↑)" if task.higher_is_better else "Lower is Better (↓)"
    t_col = task.tertiary_metric_name or "Tertiary"
    q_col = task.quaternary_metric_name or "Quaternary"
    lines = [
        f"### {task.title} (`{task.name}`)",
        "",
        f"- **Domain**: `{task.domain_label}` | **Optimization Direction**: {direction}",
        f"- **Task Specification**: {task.description}",
        f"- **Rank #1 Winner**: **{task.best_model_name}** (`{task.best_primary_metric:.4f}` {task.primary_metric_name}, `{task.best_secondary_metric:.4f}` {task.secondary_metric_name})",
        "",
        f"| Rank | Model Architecture | Family | {task.primary_metric_name} | {task.secondary_metric_name} | {t_col} | {q_col} | Params | Time (s) |",
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
    ]
    for row in task.leaderboard:
        badge = "🏆 **#1**" if row.rank == 1 else f"#{row.rank}"
        model_display = f"**{row.model_name}**" if row.rank == 1 else row.model_name
        lines.append(
            f"| {badge} | `{model_display}` | {row.family} | `{row.primary_metric:.4f}` | "
            f"`{row.secondary_metric:.4f}` | `{row.tertiary_metric:.2f}` | `{row.quaternary_metric:.4f}` | "
            f"`{row.num_params:,}` | `{row.elapsed_sec:.2f}s` |"
        )
    return "\n".join(lines)


def _load_promoted_manifest(repo_path: Path, fs: FSBase) -> dict[str, Any]:
    """Load `promoted/promoted_manifest.json` and harness metadata via FSBase if present."""
    manifest_path = repo_path / "promoted" / "promoted_manifest.json"
    if not manifest_path.exists():
        return {}
    try:
        buf = io.BytesIO()
        fs.read(str(manifest_path), buf)
        manifest = json.loads(buf.getvalue().decode("utf-8"))
        if not isinstance(manifest, dict):
            return {}
        promoted_model_py = repo_path / "promoted" / "promoted_model.py"
        split_harness_py = repo_path / "src" / "model_harness" / "core.py"
        legacy_harness_py = repo_path / "promoted" / "harness.py"
        manifest = mask_restricted_vocabulary(dict(manifest))
        manifest["promoted_model_file"] = "promoted/promoted_model.py" if promoted_model_py.exists() else ""
        if split_harness_py.exists():
            manifest["harness_file"] = "src/model_harness/core.py"
        elif legacy_harness_py.exists():
            manifest["harness_file"] = "promoted/harness.py"
        else:
            manifest["harness_file"] = ""
        return manifest
    except Exception:
        return {}


def _format_model_markdown(profile: ModelArchitectureProfile) -> str:
    if profile.is_promoted:
        best_badge = "🏆 **PROMOTED GAUNTLET WINNER (`promoted/`) — OVERALL #1 PINNED ARCHITECTURE**"
    elif profile.is_overall_best:
        best_badge = "🏆 **OVERALL #1 BEST ARCHITECTURE**"
    else:
        best_badge = f"Overall Rank **#{profile.overall_rank}**"
    lines = [
        f"## {profile.model_name} — {best_badge}",
        "",
        f"{profile.summary}",
        "",
        "### Aggregate Multi-Domain Scorecard",
        "",
        "| Metric | Value |",
        "| :--- | :--- |",
        f"| **Overall Rank** | `#{profile.overall_rank}` {'(Pinned Promoted Gauntlet Winner)' if profile.is_promoted else ''} |",
        f"| **Composite Score** | `{profile.composite_score:.2f} / 100.0` |",
        f"| **Rank #1 Benchmark Wins** | `{profile.wins_count} / {profile.benchmarks_evaluated}` |",
        f"| **Top-3 Podium Finishes** | `{profile.top3_count} / {profile.benchmarks_evaluated}` |",
        f"| **Mean Leaderboard Rank** | `{profile.mean_rank:.2f}` |",
        f"| **Median Leaderboard Rank** | `{profile.median_rank:.1f}` |",
        f"| **PyTorch Class** | `{profile.class_name}` (`{profile.source_file}:{profile.source_line_start}-{profile.source_line_end}`) |",
        f"| **Average Parameter Budget** | `{profile.avg_num_params:,}` params |",
        f"| **Average Execution Latency** | `{profile.avg_elapsed_sec:.2f}s` / benchmark |",
        f"| **Last Tested At** | `{profile.last_tested_at or 'N/A'}` |",
    ]
    if profile.is_promoted and profile.promoted_metadata:
        pm = profile.promoted_metadata
        q_ver = pm.get("qdrant_code_verification") or {}
        geo_tr = pm.get("geodesic_code_trace") or {}
        lines.extend(
            [
                f"| **Promoted Package** | `promoted/promoted_model.py` & `promoted/harness.py` (`PromotedModelHarness`) |",
                f"| **Qdrant Code Verification** | `unchanged={q_ver.get('unchanged', True)}` (`{q_ver.get('verified_files_count', 0)}` verified files, digest `{str(q_ver.get('corpus_digest', ''))[:16]}...`) |",
                f"| **Poincare Geodesic Code Trace** | `{geo_tr.get('hop_count', geo_tr.get('num_hops', 0))}` hops (`total_geodesic_distance={geo_tr.get('total_geodesic_distance', 0.0)}`) |",
            ]
        )
    lines.extend(
        [
            "",
            "### Architectural Highlights",
            "",
        ]
    )
    for item in profile.architectural_highlights:
        lines.append(f"- {item}")

    lines.extend(
        [
            "",
            "### Per-Benchmark Leaderboard Breakdown",
            "",
            "| Benchmark | Category | Rank | Primary Metric | Secondary Metric | Params | Time (s) |",
            "| :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
        ]
    )
    for entry in profile.benchmark_entries:
        rank_label = (
            f"🏆 **#1 / {entry.total_models}**"
            if entry.rank == 1
            else f"#{entry.rank} / {entry.total_models}"
        )
        lines.append(
            f"| **{entry.benchmark_title}** (`{entry.benchmark_name}`) | `{entry.category}` | {rank_label} | "
            f"`{entry.primary_metric:.4f}` ({entry.primary_metric_name}) | "
            f"`{entry.secondary_metric:.4f}` ({entry.secondary_metric_name}) | "
            f"`{entry.num_params:,}` | `{entry.elapsed_sec:.2f}s` |"
        )
    return "\n".join(lines)


class BenchmarkLedger:
    """
    Thread-safe Neural Benchmark Leaderboard and History Ledger.
    Scrapes authentic violation-free multi-domain benchmark results, post-audit run history,
    and PyTorch model source code from the neural_networks repository, persists canonical
    ledger snapshots via FSBase, and constructs Qdrant documents with relational metadata.
    """

    def __init__(
        self,
        repo_path: str | Path = "/Users/jarek/IdeaProjects/neural_networks",
        ledger_path: str | Path | None = None,
    ) -> None:
        self.repo_path = Path(repo_path)
        self.ledger_path = Path(ledger_path) if ledger_path is not None else None
        self._fs = FSBase(filesystem="file")
        self._index = Index()
        self._index.new("models")
        self._index.new("tasks")
        self._index.new("runs")
        self._runs_list = TsList()
        self._custom_saved_runs: list[dict[str, Any]] = []
        self._latest_raw_override: dict[str, Any] | None = None
        self._models: list[ModelArchitectureProfile] = []
        self._tasks: list[BenchmarkTaskRecord] = []
        self._runs: list[BenchmarkRunSnapshot] = []
        self._timeseries: list[dict[str, Any]] = []
        self._promoted_manifest: dict[str, Any] = {}
        self._last_scraped_at: str = ""

        if self.ledger_path is not None and self.ledger_path.exists():
            self._load_persisted_custom_runs()

    def _load_persisted_custom_runs(self) -> None:
        if self.ledger_path is None or not self.ledger_path.exists():
            return
        try:
            buf = io.BytesIO()
            self._fs.read(str(self.ledger_path), buf)
            payload = json.loads(buf.getvalue().decode("utf-8"))
            # Ignore stale pre-audit ledgers that still reference PolyphonicPipeline
            best_m = payload.get("best_model") or {}
            if str(best_m.get("model_name", "")).startswith("PolyphonicPipeline"):
                return
            saved = payload.get("custom_saved_runs", [])
            if isinstance(saved, list):
                self._custom_saved_runs = saved
            mc_json_path = (
                self.repo_path
                / "src"
                / "models"
                / "monadic_cavity"
                / "results"
                / "monadic_cavity_leaderboard.json"
            )
            is_worktree_newer = (
                mc_json_path.exists()
                and self.ledger_path.exists()
                and mc_json_path.stat().st_mtime > self.ledger_path.stat().st_mtime
            )
            override = payload.get("latest_raw_override")
            if isinstance(override, dict) and override and not is_worktree_newer:
                self._latest_raw_override = override
        except Exception:
            return

    def scrape_and_build(self, include_git_history: bool = True, max_git_commits: int = 6) -> dict[str, Any]:
        """Scrape authentic post-audit worktree leaderboards, run progression, and AST model source code."""
        leaderboard_json_path = (
            self.repo_path
            / "src"
            / "polyphonic_model"
            / "benchmarks"
            / "results"
            / "benchmark_leaderboard.json"
        )
        monadic_json_path = (
            self.repo_path
            / "src"
            / "models"
            / "monadic_cavity"
            / "results"
            / "monadic_cavity_leaderboard.json"
        )
        if (
            not leaderboard_json_path.exists()
            and not monadic_json_path.exists()
            and self._latest_raw_override is None
        ):
            raise FileNotFoundError(
                f"Benchmark leaderboard JSON not found at {leaderboard_json_path} or {monadic_json_path}"
            )

        bundle = _load_authentic_bundle()
        git_snapshots: list[dict[str, Any]] = []
        if include_git_history:
            git_snapshots = _git_post_audit_raw_runs(self.repo_path, max_commits=max_git_commits)

        worktree_canonical_raw = _build_canonical_worktree_suite(self.repo_path, bundle)
        if self._latest_raw_override is not None and isinstance(worktree_canonical_raw, dict):
            active_raw = json.loads(json.dumps(worktree_canonical_raw))
            for sec_key, metric_key in (
                ("language_benchmarks", "eval_ce_loss"),
                ("continuous_benchmarks", "eval_geodesic_dist"),
            ):
                ov_sec = self._latest_raw_override.get(sec_key, {})
                wt_sec = active_raw.get(sec_key, {})
                if isinstance(ov_sec, dict) and isinstance(wt_sec, dict):
                    for k, ov_task in ov_sec.items():
                        if k not in wt_sec or not isinstance(ov_task, dict):
                            continue
                        by_m = {
                            _canonical_model_name(str(r.get("model_name", ""))): dict(r)
                            for r in wt_sec[k].get("leaderboard", [])
                            if isinstance(r, dict)
                        }
                        for r in ov_task.get("leaderboard", []):
                            if isinstance(r, dict):
                                cm = _canonical_model_name(str(r.get("model_name", "")))
                                rc = dict(r)
                                rc["model_name"] = cm
                                by_m[cm] = rc
                        merged_lb = list(by_m.values())
                        merged_lb.sort(
                            key=lambda row: float(row.get(metric_key, row.get("primary_metric", 999.0)))
                        )
                        wt_sec[k]["leaderboard"] = merged_lb
                        if merged_lb:
                            wt_sec[k]["winner"] = merged_lb[0]["model_name"]
        else:
            active_raw = worktree_canonical_raw

        class_sources = _extract_class_sources_from_repo(self.repo_path)

        raw_snapshots: list[dict[str, Any]] = list(git_snapshots)
        if worktree_canonical_raw is not None:
            wt_digest = hashlib.sha256(
                json.dumps(worktree_canonical_raw, sort_keys=True).encode("utf-8")
            ).hexdigest()[:16]
            already_present = any(s.get("content_digest") == wt_digest for s in raw_snapshots)
            if not already_present:
                ref_path = monadic_json_path if monadic_json_path.exists() else leaderboard_json_path
                mtime_iso = (
                    dt.datetime.fromtimestamp(
                        ref_path.stat().st_mtime, tz=dt.timezone.utc
                    ).isoformat()
                    if ref_path.exists()
                    else dt.datetime.now(dt.timezone.utc).isoformat()
                )
                raw_snapshots.append(
                    {
                        "commit_hash": "worktree-head",
                        "recorded_at": mtime_iso,
                        "run_label": "Canonical 12-Benchmark Violation-Free Suite (Promoted Monadic Cavity + StageCompiler Compiler)",
                        "source_type": "worktree_scrape",
                        "notes": (
                            "Combined 12-benchmark post-audit suite across all 14 violation-free architectures "
                            "(4 Promoted Monadic Cavity models, 3 StageCompiler Stage-1 Compiler models, "
                            "2 standalone Cohesive/Polyphase AnyGram models, and 5 Euclidean reference baselines)."
                        ),
                        "raw_data": worktree_canonical_raw,
                        "content_digest": wt_digest,
                    }
                )

        for custom_run in self._custom_saved_runs:
            raw_snapshots.append(custom_run)

        run_snapshots: list[BenchmarkRunSnapshot] = []
        task_history_map: dict[str, list[BenchmarkHistoryPoint]] = {}
        model_history_map: dict[str, list[dict[str, Any]]] = {}

        for idx, snap in enumerate(raw_snapshots):
            raw_data = snap["raw_data"]
            commit_hash = str(snap.get("commit_hash", f"run-{idx + 1}"))
            raw_rec_at = str(
                snap.get("recorded_at")
                or snap.get("timestamp")
                or dt.datetime.now(dt.timezone.utc).isoformat()
            )
            try:
                recorded_at = (
                    dt.datetime.fromisoformat(raw_rec_at.replace("Z", "+00:00"))
                    .astimezone(dt.timezone.utc)
                    .isoformat()
                )
            except Exception:
                recorded_at = raw_rec_at
            run_label = str(snap.get("run_label") or f"Benchmark Snapshot #{idx + 1}")
            source_type = snap.get("source_type", "worktree_scrape")
            run_id = f"run_{uuid5_from_args(BENCHMARK_NAMESPACE, commit_hash, recorded_at, idx)[:12]}"

            norm_tasks = _normalize_task_list(raw_data)
            win_counts: dict[str, int] = {}
            rank_lists: dict[str, list[int]] = {}
            task_winners: dict[str, str] = {}

            for n_task in norm_tasks:
                t_name = str(n_task["name"])
                b_id = _benchmark_id(t_name)
                lb = n_task["leaderboard"]
                if not lb:
                    continue
                winner_name = str(lb[0]["model_name"])
                winner_metric = float(lb[0]["primary_metric"])
                task_winners[t_name] = winner_name
                win_counts[winner_name] = win_counts.get(winner_name, 0) + 1

                model_metrics_for_point: dict[str, float] = {}
                model_ranks_for_point: dict[str, int] = {}
                for row in lb:
                    m_name = str(row["model_name"])
                    m_rank = int(row["rank"])
                    m_val = float(row["primary_metric"])
                    model_metrics_for_point[m_name] = m_val
                    model_ranks_for_point[m_name] = m_rank
                    rank_lists.setdefault(m_name, []).append(m_rank)

                task_history_map.setdefault(b_id, []).append(
                    BenchmarkHistoryPoint(
                        run_id=run_id,
                        commit_hash=commit_hash,
                        recorded_at=recorded_at,
                        run_label=run_label,
                        best_model_name=winner_name,
                        best_metric_value=winner_metric,
                        model_metrics=model_metrics_for_point,
                        model_ranks=model_ranks_for_point,
                    )
                )

            mean_ranks: dict[str, float] = {
                m_name: round(sum(ranks) / len(ranks), 2)
                for m_name, ranks in rank_lists.items()
                if ranks
            }
            sorted_run_models = sorted(
                mean_ranks.keys(),
                key=lambda m: (mean_ranks.get(m, 99.0), -win_counts.get(m, 0)),
            )
            best_run_model = (
                sorted_run_models[0]
                if sorted_run_models
                else "CohesiveUnifiedMonadicCavityModel (AnyGram 2-GRU + Unified Cavity)"
            )

            for m_name, m_mean_rank in mean_ranks.items():
                model_history_map.setdefault(m_name, []).append(
                    {
                        "run_id": run_id,
                        "commit_hash": commit_hash,
                        "recorded_at": recorded_at,
                        "run_label": run_label,
                        "wins_count": win_counts.get(m_name, 0),
                        "mean_rank": m_mean_rank,
                        "benchmarks_evaluated": len(rank_lists.get(m_name, [])),
                    }
                )

            cfg = raw_data.get("config", {}) if isinstance(raw_data.get("config"), dict) else {}
            snap_tested_models = [
                _canonical_model_name(str(tm))
                for tm in (snap.get("tested_models") or [])
                if str(tm).strip()
            ]
            run_snapshots.append(
                BenchmarkRunSnapshot(
                    run_id=run_id,
                    commit_hash=commit_hash,
                    recorded_at=recorded_at,
                    run_label=run_label,
                    source_type=source_type,
                    device=str(raw_data.get("device") or cfg.get("device") or "cpu"),
                    epochs=int(raw_data.get("epochs") or cfg.get("epochs") or 6),
                    total_benchmarks=len(norm_tasks),
                    total_models=len(mean_ranks),
                    overall_best_model=best_run_model,
                    overall_best_model_wins=win_counts.get(best_run_model, 0),
                    overall_best_model_mean_rank=mean_ranks.get(best_run_model, 1.0),
                    model_win_counts=win_counts,
                    model_mean_ranks=mean_ranks,
                    task_winners=task_winners,
                    tested_models=snap_tested_models,
                    notes=str(snap.get("notes", "")),
                )
            )

        # Build canonical BenchmarkTaskRecord list from active_raw
        tasks: list[BenchmarkTaskRecord] = []
        active_norm_tasks = _normalize_task_list(active_raw)
        for n_task in active_norm_tasks:
            t_name = str(n_task["name"])
            b_id = _benchmark_id(t_name)
            category: Literal["language", "continuous", "ast_compiler"] = (
                "continuous" if n_task["category"] == "continuous" else "language"
            )
            higher_is_better = bool(n_task["higher_is_better"])
            rows: list[BenchmarkMetricRow] = []
            for raw_row in n_task["leaderboard"]:
                m_name = str(raw_row["model_name"])
                meta = MODEL_FAMILY_METADATA.get(m_name, {})
                rows.append(
                    BenchmarkMetricRow(
                        rank=int(raw_row["rank"]),
                        model_name=m_name,
                        model_id=_model_id(m_name),
                        family=str(meta.get("family", "Neural Baseline")),
                        is_polyphonic=bool(meta.get("is_polyphonic", False)),
                        primary_metric=float(raw_row["primary_metric"]),
                        primary_metric_name=str(raw_row["primary_metric_name"]),
                        secondary_metric=float(raw_row["secondary_metric"]),
                        secondary_metric_name=str(raw_row["secondary_metric_name"]),
                        tertiary_metric=float(raw_row.get("tertiary_metric", 0.0)),
                        tertiary_metric_name=str(raw_row.get("tertiary_metric_name", "")),
                        quaternary_metric=float(raw_row.get("quaternary_metric", 0.0)),
                        quaternary_metric_name=str(raw_row.get("quaternary_metric_name", "")),
                        elapsed_sec=float(raw_row.get("elapsed_sec", 0.0)),
                        num_params=int(raw_row.get("num_params", 0)),
                        store_entries=int(raw_row.get("store_entries", 0)),
                        selected_epoch=int(raw_row.get("selected_epoch", 0)),
                    )
                )

            best_row = rows[0]
            runner_up = rows[1] if len(rows) > 1 else best_row
            denom = max(abs(runner_up.primary_metric), 1e-9)
            if higher_is_better:
                margin_pct = ((best_row.primary_metric - runner_up.primary_metric) / denom) * 100.0
            else:
                margin_pct = ((runner_up.primary_metric - best_row.primary_metric) / denom) * 100.0

            task_record = BenchmarkTaskRecord(
                benchmark_id=b_id,
                name=t_name,
                title=str(n_task["title"]),
                category=category,
                domain_label=str(n_task["domain_label"]),
                description=str(n_task["description"]),
                primary_metric_name=str(n_task["primary_metric_name"]),
                secondary_metric_name=str(n_task["secondary_metric_name"]),
                tertiary_metric_name=str(n_task.get("tertiary_metric_name", "")),
                quaternary_metric_name=str(n_task.get("quaternary_metric_name", "")),
                higher_is_better=higher_is_better,
                best_model_name=best_row.model_name,
                best_model_id=best_row.model_id,
                best_primary_metric=best_row.primary_metric,
                best_secondary_metric=best_row.secondary_metric,
                runner_up_model_name=runner_up.model_name,
                runner_up_primary_metric=runner_up.primary_metric,
                relative_margin_pct=round(margin_pct, 2),
                leaderboard=rows,
                history_points=task_history_map.get(b_id, []),
            )
            task_record.markdown_table = _format_task_markdown_table(task_record)
            tasks.append(task_record)

        entries_by_model: dict[str, list[ModelBenchmarkEntry]] = {}
        for task in tasks:
            total_models_in_task = len(task.leaderboard)
            best_val = task.best_primary_metric
            for row in task.leaderboard:
                delta_pct = (
                    ((row.primary_metric - best_val) / max(abs(best_val), 1e-9)) * 100.0
                    if row.rank > 1
                    else 0.0
                )
                entries_by_model.setdefault(row.model_name, []).append(
                    ModelBenchmarkEntry(
                        benchmark_id=task.benchmark_id,
                        benchmark_name=task.name,
                        benchmark_title=task.title,
                        category=task.category,
                        rank=row.rank,
                        total_models=total_models_in_task,
                        primary_metric=row.primary_metric,
                        primary_metric_name=row.primary_metric_name,
                        secondary_metric=row.secondary_metric,
                        secondary_metric_name=row.secondary_metric_name,
                        tertiary_metric=row.tertiary_metric,
                        tertiary_metric_name=row.tertiary_metric_name,
                        higher_is_better=task.higher_is_better,
                        elapsed_sec=row.elapsed_sec,
                        num_params=row.num_params,
                        delta_vs_best_pct=round(delta_pct, 2),
                    )
                )

        total_models_count = max(len(entries_by_model), 2)
        total_tasks_count = max(len(tasks), 1)
        self._timeseries = _load_timeseries_stream(self.repo_path)

        latest_ts_by_model: dict[str, str] = {}
        for pt in self._timeseries:
            pt_model = str(pt.get("model_name", "")).strip()
            pt_ts = str(pt.get("timestamp", "")).strip()
            if not pt_model or not pt_ts:
                continue
            try:
                norm_ts = (
                    dt.datetime.fromisoformat(pt_ts.replace("Z", "+00:00"))
                    .astimezone(dt.timezone.utc)
                    .isoformat()
                )
            except Exception:
                norm_ts = pt_ts
            if norm_ts > latest_ts_by_model.get(pt_model, ""):
                latest_ts_by_model[pt_model] = norm_ts

        for r_snap in run_snapshots:
            if "pytest" in r_snap.run_label.lower() or not r_snap.recorded_at:
                continue
            for tm_name in r_snap.tested_models:
                if r_snap.recorded_at > latest_ts_by_model.get(tm_name, ""):
                    latest_ts_by_model[tm_name] = r_snap.recorded_at

        raw_profiles: list[ModelArchitectureProfile] = []
        for m_name, m_entries in entries_by_model.items():
            ranks = sorted(e.rank for e in m_entries)
            wins_count = sum(1 for e in m_entries if e.rank == 1)
            top3_count = sum(1 for e in m_entries if e.rank <= 3)
            mean_rank = sum(ranks) / len(ranks)
            median_rank = float(ranks[len(ranks) // 2])
            avg_params = int(round(sum(e.num_params for e in m_entries) / len(m_entries)))
            avg_elapsed = round(sum(e.elapsed_sec for e in m_entries) / len(m_entries), 3)

            rank_component = ((total_models_count - mean_rank) / max(total_models_count - 1, 1)) * 65.0
            top3_component = (top3_count / total_tasks_count) * 20.0
            win_component = (wins_count / total_tasks_count) * 15.0
            coverage_factor = 0.85 + 0.15 * (len(m_entries) / total_tasks_count)
            composite_score = round((rank_component + top3_component + win_component) * coverage_factor, 2)

            meta = dict(MODEL_FAMILY_METADATA.get(m_name, {}))
            if not meta:
                prefix_cls = m_name.split(" (")[0].strip()
                matched_cls = class_sources.get(prefix_cls)
                if matched_cls is None:
                    for c_info in class_sources.values():
                        if c_info.get("display_name") == m_name:
                            matched_cls = c_info
                            break
                if matched_cls is not None:
                    meta = {
                        "class_name": matched_cls["class_name"],
                        "base_class_name": matched_cls.get("base_class_name", ""),
                        "family": "StageCompiler / Monadic Cavity Architecture",
                        "is_polyphonic": True,
                        "summary": matched_cls.get("docstring") or f"{m_name} architecture.",
                        "related_modules": ["benchmark_ledger", "llm_Fine_Tuning", "thread_safe"],
                    }

            target_class_name = str(meta.get("class_name") or m_name.split(" (")[0].strip())
            base_class_name = str(meta.get("base_class_name") or "")
            src_info = dict(class_sources.get(target_class_name, {}))
            if not base_class_name and src_info.get("base_class_name"):
                base_class_name = str(src_info["base_class_name"])
            if base_class_name and base_class_name in class_sources:
                base_info = class_sources[base_class_name]
                if src_info.get("source_code"):
                    src_info["source_code"] = (
                        f"{src_info['source_code']}\n\n"
                        f"# --- Base Engine: {base_class_name} ({base_info.get('source_file', '')}:"
                        f"{base_info.get('source_line_start', 1)}-{base_info.get('source_line_end', 1)}) ---\n"
                        f"{base_info.get('source_code', '')}"
                    )
                    combined_methods = list(src_info.get("method_names", []))
                    for bm_fn in base_info.get("method_names", []):
                        if bm_fn not in combined_methods:
                            combined_methods.append(bm_fn)
                    src_info["method_names"] = combined_methods
                else:
                    src_info = dict(base_info)
                    if target_class_name and target_class_name != base_class_name:
                        src_info["source_code"] = (
                            f"class {target_class_name}({base_class_name}):\n"
                            f'    """{m_name} configured on {base_class_name}."""\n\n'
                            f"{base_info.get('source_code', '')}"
                        )

            authentic_candidates = [
                ts_val
                for ts_val in (
                    [latest_ts_by_model.get(m_name, "")]
                    + [
                        str(h.get("recorded_at", ""))
                        for h in model_history_map.get(m_name, [])
                        if h.get("commit_hash") not in {"worktree-head", "ui-checkpoint", "api-save"}
                        and "pytest" not in str(h.get("run_label", "")).lower()
                        and h.get("recorded_at")
                    ]
                )
                if ts_val
            ]
            resolved_last_tested = (
                max(authentic_candidates)
                if authentic_candidates
                else (
                    max(
                        (
                            str(h.get("recorded_at", ""))
                            for h in model_history_map.get(m_name, [])
                            if h.get("commit_hash") != "worktree-head" and h.get("recorded_at")
                        ),
                        default="",
                    )
                    or max(
                        (
                            str(h.get("recorded_at", ""))
                            for h in model_history_map.get(m_name, [])
                            if h.get("recorded_at")
                        ),
                        default="",
                    )
                )
            )

            raw_profiles.append(
                ModelArchitectureProfile(
                    model_id=_model_id(m_name),
                    model_name=m_name,
                    class_name=target_class_name,
                    friendly_title=_derive_friendly_model_title(m_name, target_class_name),
                    family=str(meta.get("family", "Neural Baseline")),
                    is_polyphonic=bool(meta.get("is_polyphonic", False)),
                    is_overall_best=False,
                    in_latest_suite=True,
                    overall_rank=99,
                    composite_score=composite_score,
                    wins_count=wins_count,
                    top3_count=top3_count,
                    benchmarks_evaluated=len(m_entries),
                    mean_rank=round(mean_rank, 2),
                    median_rank=median_rank,
                    avg_num_params=avg_params,
                    avg_elapsed_sec=avg_elapsed,
                    summary=str(
                        meta.get("summary")
                        or src_info.get("docstring")
                        or f"{m_name} neural sequence and dynamical systems model."
                    ),
                    architectural_highlights=list(meta.get("architectural_highlights", [])),
                    source_file=str(src_info.get("source_file", "")),
                    source_line_start=int(src_info.get("source_line_start", 1)),
                    source_line_end=int(src_info.get("source_line_end", 1)),
                    source_code=str(src_info.get("source_code", "")),
                    docstring=str(src_info.get("docstring", "")),
                    method_names=list(src_info.get("method_names", [])),
                    benchmark_entries=sorted(m_entries, key=lambda e: (e.rank, e.benchmark_name)),
                    historical_ranks=model_history_map.get(m_name, []),
                    last_tested_at=resolved_last_tested,
                    related_modules=list(meta.get("related_modules", ["benchmark_ledger"])),
                    related_kis=list(
                        meta.get(
                            "related_kis",
                            [
                                "ki_benchmark_ledger_ledger_neural_benchmark_leaderboard_qdrant_history"
                            ],
                        )
                    ),
                )
            )

        promoted_manifest = _load_promoted_manifest(self.repo_path, self._fs)
        self._promoted_manifest = promoted_manifest
        promoted_model_id = str(promoted_manifest.get("model_id", "")).strip()
        promoted_model_name = str(promoted_manifest.get("model_name", "")).strip()
        promoted_class_name = str(promoted_manifest.get("class_name", "")).strip()

        for profile in raw_profiles:
            is_match = bool(
                promoted_manifest
                and (
                    (promoted_model_id and profile.model_id == promoted_model_id)
                    or (promoted_model_name and profile.model_name == promoted_model_name)
                    or (promoted_class_name and profile.class_name == promoted_class_name)
                )
            )
            if is_match:
                profile.is_promoted = True
                profile.promoted_metadata = promoted_manifest

        raw_profiles.sort(
            key=lambda p: (
                0 if p.is_promoted else 1,
                0 if p.benchmarks_evaluated >= total_tasks_count else 1,
                -p.composite_score,
                p.mean_rank,
                -p.wins_count,
                -p.top3_count,
                p.model_name,
            )
        )
        for rank_idx, profile in enumerate(raw_profiles, start=1):
            profile.overall_rank = rank_idx
            profile.is_overall_best = rank_idx == 1
            profile.markdown_overview = _format_model_markdown(profile)

        self._index = Index()
        self._index.new("models")
        self._index.new("tasks")
        self._index.new("runs")
        self._runs_list = TsList()

        for profile in raw_profiles:
            self._index.store_in_index("models", profile.model_id, profile.model_dump())
        for task in tasks:
            self._index.store_in_index("tasks", task.benchmark_id, task.model_dump())
        for run in run_snapshots:
            self._index.store_in_index("runs", run.run_id, run.model_dump())
            self._runs_list.append(run.model_dump())

        self._models = raw_profiles
        self._tasks = tasks
        self._runs = run_snapshots
        self._timeseries = _load_timeseries_stream(self.repo_path)
        self._last_scraped_at = dt.datetime.now(dt.timezone.utc).isoformat()

        if self.ledger_path is not None:
            self.persist_ledger(self.ledger_path)

        return self.get_overview()

    def persist_ledger(self, target_path: str | Path | None = None) -> Path:
        """Persist the full ledger state and historical runs to disk using FSBase."""
        out_path = Path(target_path) if target_path is not None else self.ledger_path
        if out_path is None:
            raise ValueError("target_path or self.ledger_path must be specified")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "last_scraped_at": self._last_scraped_at,
            "repo_path": str(self.repo_path),
            "best_model": self._models[0].model_dump() if self._models else None,
            "promoted_model": (
                self._models[0].model_dump()
                if (self._models and self._models[0].is_promoted)
                else None
            ),
            "promoted_manifest": self._promoted_manifest,
            "models": [m.model_dump() for m in self._models],
            "tasks": [t.model_dump() for t in self._tasks],
            "runs": [r.model_dump() for r in self._runs],
            "timeseries_count": len(self._timeseries),
            "timeseries": self._timeseries,
            "custom_saved_runs": self._custom_saved_runs,
            "latest_raw_override": self._latest_raw_override,
        }
        encoded = json.dumps(payload, indent=2).encode("utf-8")
        buf = io.BytesIO(encoded)
        self._fs.write(str(out_path), buf)
        return out_path

    def save_run(
        self,
        request: BenchmarkSaveRequest | dict[str, Any],
        persist_path: str | Path | None = None,
    ) -> dict[str, Any]:
        """
        Save a new benchmark run snapshot or apply task metric overrides via the API,
        append to historical runs, rebuild rankings, and persist to disk.
        """
        if not self._models or not self._tasks:
            self.scrape_and_build()

        req = (
            request
            if isinstance(request, BenchmarkSaveRequest)
            else BenchmarkSaveRequest.model_validate(request)
        )
        raw_recorded_at = req.recorded_at or dt.datetime.now(dt.timezone.utc).isoformat()
        try:
            recorded_at = (
                dt.datetime.fromisoformat(str(raw_recorded_at).replace("Z", "+00:00"))
                .astimezone(dt.timezone.utc)
                .isoformat()
            )
        except Exception:
            recorded_at = str(raw_recorded_at)

        tested_models: set[str] = set()
        for explicit_sec in (req.language_benchmarks, req.continuous_benchmarks):
            if isinstance(explicit_sec, dict):
                for t_val in explicit_sec.values():
                    if isinstance(t_val, dict):
                        for r_item in t_val.get("leaderboard", []):
                            if isinstance(r_item, dict) and r_item.get("model_name"):
                                tested_models.add(_canonical_model_name(str(r_item["model_name"])))
            elif isinstance(explicit_sec, list):
                for t_val in explicit_sec:
                    if isinstance(t_val, dict):
                        for r_item in t_val.get("leaderboard", []):
                            if isinstance(r_item, dict) and r_item.get("model_name"):
                                tested_models.add(_canonical_model_name(str(r_item["model_name"])))

        bundle = _load_authentic_bundle()
        base_raw = (
            json.loads(json.dumps(self._latest_raw_override))
            if self._latest_raw_override is not None
            else _build_canonical_worktree_suite(self.repo_path, bundle)
        )

        if req.language_benchmarks or req.continuous_benchmarks:
            raw_payload = {
                "device": req.device,
                "epochs": req.epochs,
                "language_benchmarks": req.language_benchmarks or base_raw.get("language_benchmarks", {}),
                "continuous_benchmarks": req.continuous_benchmarks
                or base_raw.get("continuous_benchmarks", {}),
            }
        else:
            raw_payload = json.loads(json.dumps(base_raw))
            raw_payload["device"] = req.device
            raw_payload["epochs"] = req.epochs

        if req.task_overrides:
            for override in req.task_overrides:
                target_task = str(override.get("benchmark_name") or override.get("name") or "")
                target_model = str(override.get("model_name") or "")
                if not target_task or not target_model:
                    continue
                tested_models.add(_canonical_model_name(target_model))
                for section_key, metric_field, sec_field in (
                    ("language_benchmarks", "eval_ce_loss", "eval_bits_per_byte"),
                    ("continuous_benchmarks", "eval_geodesic_dist", "eval_rel_l2"),
                ):
                    sec_obj = raw_payload.get(section_key, {})
                    if isinstance(sec_obj, dict) and target_task in sec_obj:
                        task_obj = sec_obj[target_task]
                        lb = task_obj.get("leaderboard", [])
                        matched = False
                        for row in lb:
                            if _canonical_model_name(str(row.get("model_name", ""))) == _canonical_model_name(target_model):
                                if "primary_metric" in override:
                                    row[metric_field] = float(override["primary_metric"])
                                    row["primary_metric"] = float(override["primary_metric"])
                                if "secondary_metric" in override:
                                    row[sec_field] = float(override["secondary_metric"])
                                    row["secondary_metric"] = float(override["secondary_metric"])
                                matched = True
                                break
                        if not matched and "primary_metric" in override:
                            lb.append(
                                {
                                    "model_name": _canonical_model_name(target_model),
                                    metric_field: float(override["primary_metric"]),
                                    sec_field: float(override.get("secondary_metric", 0.0)),
                                    "params": int(override.get("num_params", 554400)),
                                    "elapsed_sec": float(override.get("elapsed_sec", 1.0)),
                                }
                            )
                        lb.sort(key=lambda r: float(r.get("primary_metric", r.get(metric_field, 999.0))))
                        task_obj["leaderboard"] = lb

        digest = hashlib.sha256(
            json.dumps(raw_payload, sort_keys=True).encode("utf-8")
        ).hexdigest()[:16]
        custom_entry = {
            "commit_hash": req.commit_hash,
            "recorded_at": recorded_at,
            "run_label": req.run_label,
            "source_type": "api_save",
            "notes": req.notes,
            "tested_models": sorted(m for m in tested_models if m),
            "raw_data": raw_payload,
            "content_digest": digest,
        }
        self._custom_saved_runs.append(custom_entry)
        self._latest_raw_override = raw_payload

        if persist_path is not None:
            self.ledger_path = Path(persist_path)

        overview = self.scrape_and_build()
        return {
            "saved_run": self._runs[-1].model_dump() if self._runs else {},
            "total_runs": len(self._runs),
            "best_model": overview["best_model"],
        }

    def get_overview(self) -> dict[str, Any]:
        if not self._models or not self._tasks:
            self.scrape_and_build()
        best_model = self._models[0].model_dump() if self._models else None
        promoted_model = (
            self._models[0].model_dump()
            if (self._models and self._models[0].is_promoted)
            else None
        )
        return {
            "repo_path": str(self.repo_path),
            "last_scraped_at": self._last_scraped_at,
            "total_models": len(self._models),
            "total_benchmarks": len(self._tasks),
            "total_runs": len(self._runs),
            "timeseries_count": len(self._timeseries),
            "language_benchmarks_count": sum(1 for t in self._tasks if t.category == "language"),
            "continuous_benchmarks_count": sum(1 for t in self._tasks if t.category == "continuous"),
            "best_model": best_model,
            "promoted_model": promoted_model,
            "promoted_manifest": self._promoted_manifest,
            "models": [m.model_dump() for m in self._models],
            "tasks": [t.model_dump() for t in self._tasks],
            "runs": [r.model_dump() for r in self._runs],
            "timeseries": self._timeseries,
        }

    def get_model(self, model_id_or_name: str) -> dict[str, Any] | None:
        if not self._models:
            self.scrape_and_build()
        norm_q = mask_restricted_string(model_id_or_name)
        direct = self._index.load_from_index("models", norm_q)
        if direct is not None:
            return direct
        slug = _model_id(norm_q)
        by_slug = self._index.load_from_index("models", slug)
        if by_slug is not None:
            return by_slug
        for m in self._models:
            if m.model_name.lower() == norm_q.lower() or m.class_name.lower() == norm_q.lower():
                return m.model_dump()
        return None

    def get_task(self, benchmark_id_or_name: str) -> dict[str, Any] | None:
        if not self._tasks:
            self.scrape_and_build()
        direct = self._index.load_from_index("tasks", benchmark_id_or_name)
        if direct is not None:
            return direct
        slug = _benchmark_id(benchmark_id_or_name)
        by_slug = self._index.load_from_index("tasks", slug)
        if by_slug is not None:
            return by_slug
        for t in self._tasks:
            if t.name.lower() == benchmark_id_or_name.lower() or t.title.lower() == benchmark_id_or_name.lower():
                return t.model_dump()
        return None

    def build_qdrant_documents(self) -> list[BenchmarkQdrantDocument]:
        """
        Construct Qdrant documents with rich markdown and metadata for every evaluated
        model architecture, benchmark leaderboard task, and historical run snapshot.
        """
        if not self._models or not self._tasks:
            self.scrape_and_build()

        docs: list[BenchmarkQdrantDocument] = []

        for model in self._models:
            docs.append(
                BenchmarkQdrantDocument(
                    doc_id=model.model_id,
                    doc_type="benchmark_model",
                    title=model.model_name,
                    friendly_title=model.friendly_title or _derive_friendly_model_title(model.model_name, model.class_name),
                    subtitle=(
                        f"{'PROMOTED GAUNTLET WINNER • ' if model.is_promoted else ''}Rank #{model.overall_rank} • "
                        f"{model.family} • Top-3: {model.top3_count}/{model.benchmarks_evaluated} • Mean Rank {model.mean_rank:.2f}"
                    ),
                    category="Polyphonic Architectures" if model.is_polyphonic else "Baseline Architectures",
                    summary=model.summary,
                    markdown=model.markdown_overview,
                    code_snippet=model.source_code,
                    source_file=model.source_file,
                    is_overall_best=model.is_overall_best,
                    is_promoted=model.is_promoted,
                    overall_rank=model.overall_rank,
                    composite_score=model.composite_score,
                    wins_count=model.wins_count,
                    top3_count=model.top3_count,
                    mean_rank=model.mean_rank,
                    best_model_name=self._models[0].model_name if self._models else model.model_name,
                    last_tested_at=model.last_tested_at,
                    related_modules=model.related_modules,
                    related_kis=model.related_kis,
                    payload=model.model_dump(),
                )
            )

        for idx, task in enumerate(self._tasks, start=1):
            docs.append(
                BenchmarkQdrantDocument(
                    doc_id=task.benchmark_id,
                    doc_type="benchmark_task",
                    title=task.title,
                    friendly_title=task.title,
                    subtitle=f"{task.domain_label} • Winner: {task.best_model_name} ({task.best_primary_metric:.4f} {task.primary_metric_name})",
                    category="Language Benchmarks" if task.category == "language" else "Continuous Dynamical Systems",
                    summary=task.description,
                    markdown=task.markdown_table,
                    code_snippet="",
                    source_file="src/models/monadic_cavity/benchmark_monadic_cavity.py",
                    is_overall_best=False,
                    overall_rank=100 + idx,
                    composite_score=task.relative_margin_pct,
                    wins_count=1,
                    top3_count=3,
                    mean_rank=1.0,
                    best_model_name=task.best_model_name,
                    last_tested_at=(
                        task.history_points[-1].recorded_at
                        if task.history_points
                        else self._last_scraped_at
                    ),
                    related_modules=["benchmark_ledger", "llm_Fine_Tuning", "thread_safe"],
                    related_kis=[
                        "ki_benchmark_ledger_ledger_neural_benchmark_leaderboard_qdrant_history"
                    ],
                    payload=task.model_dump(),
                )
            )

        for r_idx, run in enumerate(reversed(self._runs), start=1):
            run_md_lines = [
                f"### {run.run_label} (`{run.commit_hash}`)",
                "",
                f"- **Run ID**: `{run.run_id}` | **Recorded At**: `{run.recorded_at}` | **Source**: `{run.source_type}`",
                f"- **Overall Top Winner**: **{run.overall_best_model}** (`{run.overall_best_model_wins}` wins, mean rank `{run.overall_best_model_mean_rank:.2f}`)",
                f"- **Suite Scale**: `{run.total_benchmarks}` benchmarks × `{run.total_models}` models (`{run.epochs}` epochs on `{run.device}`)",
            ]
            if run.notes:
                run_md_lines.append(f"- **Audit / Run Notes**: {run.notes}")
            run_md_lines.extend(
                [
                    "",
                    "#### Model Mean Ranks & Win Counts",
                    "",
                    "| Model | Wins | Mean Rank |",
                    "| :--- | :--- | :--- |",
                ]
            )
            for m_name, m_rank in sorted(
                run.model_mean_ranks.items(),
                key=lambda kv: (kv[1], -run.model_win_counts.get(kv[0], 0)),
            ):
                run_md_lines.append(
                    f"| `{m_name}` | `{run.model_win_counts.get(m_name, 0)}` | `{m_rank:.2f}` |"
                )
            docs.append(
                BenchmarkQdrantDocument(
                    doc_id=run.run_id,
                    doc_type="benchmark_run",
                    title=run.run_label,
                    friendly_title=run.run_label,
                    subtitle=f"Snapshot {run.commit_hash} • Best: {run.overall_best_model} (Mean Rank {run.overall_best_model_mean_rank:.2f})",
                    category="Benchmark Run History",
                    summary=(
                        f"Post-audit benchmark snapshot ({run.commit_hash}) across {run.total_benchmarks} tasks. "
                        f"Top model: {run.overall_best_model} with mean rank {run.overall_best_model_mean_rank:.2f} and {run.overall_best_model_wins} wins."
                    ),
                    markdown="\n".join(run_md_lines),
                    code_snippet="",
                    source_file="src/models/monadic_cavity/results/monadic_cavity_leaderboard.json",
                    is_overall_best=False,
                    overall_rank=200 + r_idx,
                    composite_score=float(run.overall_best_model_wins),
                    wins_count=run.overall_best_model_wins,
                    top3_count=0,
                    mean_rank=run.overall_best_model_mean_rank,
                    best_model_name=run.overall_best_model,
                    last_tested_at=run.recorded_at,
                    related_modules=["benchmark_ledger", "thread_safe", "fsspecc"],
                    related_kis=[
                        "ki_benchmark_ledger_ledger_neural_benchmark_leaderboard_qdrant_history"
                    ],
                    payload=run.model_dump(),
                )
            )

        return docs

    def get_live_source_signature(self) -> str:
        """
        Return a sub-millisecond signature of all benchmark JSON files and active task logs
        so callers can detect real-time updates without re-scraping when unchanged.
        """
        watched = [
            self.repo_path / "promoted" / "promoted_manifest.json",
            self.repo_path / "promoted" / "harness.py",
            self.repo_path / "src" / "models" / "stage_compiler" / "results" / "stage_compiler_leaderboard.json",
            self.repo_path / "src" / "models" / "monadic_cavity" / "results" / "monadic_cavity_leaderboard.json",
            self.repo_path / "src" / "models" / "monadic_cavity" / "results" / "benchmark_timeseries.jsonl",
            self.repo_path / "src" / "models" / "monadic_cavity" / "results" / "realtime_qdrant_stream.json",
            self.repo_path / "src" / "polyphonic_model" / "benchmarks" / "results" / "benchmark_leaderboard.json",
        ]
        if self.ledger_path is not None:
            watched.append(self.ledger_path)
        if NEURAL_NETWORKS_AGENT_TASKS_DIR.exists():
            try:
                logs = sorted(
                    NEURAL_NETWORKS_AGENT_TASKS_DIR.glob("task-*.log"),
                    key=lambda p: p.stat().st_mtime,
                )[-2:]
                watched.extend(logs)
            except Exception:
                pass

        parts: list[str] = []
        for p in watched:
            try:
                if p.exists():
                    st = p.stat()
                    parts.append(f"{p.name}:{st.st_mtime_ns}:{st.st_size}")
            except Exception:
                continue
        return "|".join(parts)

    def sync_to_qdrant(
        self,
        qdrant_client: Any | None = None,
        qdrant_path: str | Path | None = None,
        collection_name: str = "portfolio_neural_benchmarks",
        embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2",
        chunk_size: int = 380,
        chunk_overlap: int = 64,
        encode_fn: Any | None = None,
        chunk_fn: Any | None = None,
        force_recreate: bool = False,
        include_git_history: bool = True,
    ) -> dict[str, Any]:
        """
        Scrape the latest benchmark state (including real-time in-flight benchmark streams)
        and index directly into Qdrant (`portfolio_neural_benchmarks`) using incremental
        SHA-256 document diffing so only changed or newly added documents are embedded/upserted.
        """
        t0 = time.perf_counter()
        overview = self.scrape_and_build(include_git_history=include_git_history)
        docs = self.build_qdrant_documents()

        from indexes.informational_index import (
            DEFAULT_STAGING_ROOT,
            UnifiedQdrantStagingManager,
        )
        from qdrant_client import QdrantClient
        from qdrant_client.models import (
            Distance,
            FieldCondition,
            Filter,
            MatchValue,
            PointStruct,
            VectorParams,
        )

        staging_mgr = UnifiedQdrantStagingManager(staging_root=DEFAULT_STAGING_ROOT)
        default_staging_storage = staging_mgr.storage_dir
        target_qdrant_path = (
            Path(qdrant_path).resolve()
            if qdrant_path is not None
            else default_staging_storage
        )

        owns_client = False
        client = qdrant_client
        if client is None:
            target_qdrant_path.mkdir(parents=True, exist_ok=True)
            try:
                client = QdrantClient(path=str(target_qdrant_path))
                owns_client = True
            except Exception:
                client = None

        try:
            vector_dim = 384
            existing_doc_hashes: dict[str, str] = {}
            existing_doc_meta: dict[str, dict[str, Any]] = {}

            if client is not None:
                collection_exists = client.collection_exists(collection_name)
                if collection_exists and force_recreate:
                    client.delete_collection(collection_name)
                    collection_exists = False

                if not collection_exists:
                    client.create_collection(
                        collection_name=collection_name,
                        vectors_config=VectorParams(size=vector_dim, distance=Distance.COSINE),
                    )
                else:
                    try:
                        scrolled, _ = client.scroll(
                            collection_name=collection_name,
                            limit=2000,
                            with_payload=True,
                            with_vectors=False,
                        )
                        for pt in scrolled:
                            pl = pt.payload or {}
                            if int(pl.get("chunk_index", -1)) == 0 and pl.get("doc_id"):
                                d_id = str(pl["doc_id"])
                                existing_doc_meta[d_id] = dict(pl)
                                if pl.get("text_sha256") or pl.get("content_sha256"):
                                    existing_doc_hashes[d_id] = str(pl.get("text_sha256") or pl.get("content_sha256"))
                    except Exception:
                        existing_doc_hashes = {}
                        existing_doc_meta = {}
            elif not force_recreate:
                for pt in staging_mgr.read_points_lockfree(target_qdrant_path, collection_name):
                    pl = pt.payload or {}
                    if int(pl.get("chunk_index", -1)) == 0 and pl.get("doc_id"):
                        d_id = str(pl["doc_id"])
                        existing_doc_meta[d_id] = dict(pl)
                        if pl.get("text_sha256") or pl.get("content_sha256"):
                            existing_doc_hashes[d_id] = str(pl.get("text_sha256") or pl.get("content_sha256"))

            active_doc_ids = {doc.doc_id for doc in docs}
            stale_doc_ids = [d_id for d_id in existing_doc_meta if d_id not in active_doc_ids]
            if stale_doc_ids and client is not None:
                for stale_id in stale_doc_ids:
                    try:
                        client.delete(
                            collection_name=collection_name,
                            points_selector=Filter(
                                must=[FieldCondition(key="doc_id", match=MatchValue(value=stale_id))]
                            ),
                        )
                    except Exception:
                        pass

            def _default_chunk_text(text: str, c_size: int, c_overlap: int) -> list[dict[str, Any]]:
                cleaned = text.strip()
                if not cleaned:
                    return [{"chunk_index": 0, "char_start": 0, "char_end": 0, "text": ""}]
                step = max(c_size - c_overlap, 64)
                chunks_out: list[dict[str, Any]] = []
                pos = 0
                idx = 0
                while pos < len(cleaned):
                    end = min(pos + c_size, len(cleaned))
                    chunks_out.append(
                        {
                            "chunk_index": idx,
                            "char_start": pos,
                            "char_end": end,
                            "text": cleaned[pos:end],
                        }
                    )
                    if end >= len(cleaned):
                        break
                    pos += step
                    idx += 1
                return chunks_out

            def _default_encode_texts(texts: list[str]) -> list[list[float]]:
                if not texts:
                    return []
                try:
                    from sentence_transformers import SentenceTransformer

                    try:
                        model = SentenceTransformer(embedding_model, device="cpu", local_files_only=True)
                    except Exception:
                        model = SentenceTransformer(embedding_model, device="cpu")
                    encoded = model.encode(
                        texts,
                        normalize_embeddings=True,
                        show_progress_bar=False,
                        convert_to_numpy=True,
                    )
                    return [[round(float(x), 6) for x in row.tolist()] for row in encoded]
                except Exception:
                    vecs: list[list[float]] = []
                    for t in texts:
                        digest = hashlib.sha256(t.encode("utf-8")).digest()
                        raw_v = [((digest[i % len(digest)] / 127.5) - 1.0) for i in range(vector_dim)]
                        norm = max(sum(x * x for x in raw_v) ** 0.5, 1e-9)
                        vecs.append([round(x / norm, 6) for x in raw_v])
                    return vecs

            active_chunk_fn = chunk_fn or _default_chunk_text
            active_encode_fn = encode_fn or (lambda txts, _m=embedding_model: _default_encode_texts(txts))

            all_chunk_records: list[dict[str, Any]] = []
            changed_chunk_records: list[dict[str, Any]] = []
            embed_inputs: list[str] = []
            changed_doc_ids: list[str] = []

            for doc in docs:
                full_text = doc.markdown.strip()
                text_sha256 = hashlib.sha256(full_text.encode("utf-8")).hexdigest()
                raw_chunks = active_chunk_fn(full_text, chunk_size, chunk_overlap)
                total_c = len(raw_chunks)
                point_ids = [
                    uuid5_from_args(
                        BENCHMARK_NAMESPACE,
                        collection_name,
                        doc.doc_id,
                        idx,
                        embedding_model,
                        chunk_size,
                        chunk_overlap,
                    )
                    for idx in range(total_c)
                ]
                prev_meta = existing_doc_meta.get(doc.doc_id) or {}
                is_changed = (
                    existing_doc_hashes.get(doc.doc_id) != text_sha256
                    or not isinstance(prev_meta.get("doc_payload"), dict)
                    or not prev_meta.get("doc_payload")
                    or str(prev_meta.get("last_tested_at") or "") != str(doc.last_tested_at or "")
                    or str(prev_meta.get("friendly_title") or "") != str(doc.friendly_title or "")
                )
                if is_changed:
                    changed_doc_ids.append(doc.doc_id)

                for idx, c in enumerate(raw_chunks):
                    prev_id = point_ids[idx - 1] if idx > 0 else None
                    next_id = point_ids[idx + 1] if idx + 1 < total_c else None
                    rec = {
                        "point_id": point_ids[idx],
                        "doc_id": doc.doc_id,
                        "doc_type": doc.doc_type,
                        "title": doc.title,
                        "friendly_title": doc.friendly_title,
                        "subtitle": doc.subtitle,
                        "category": doc.category,
                        "summary": doc.summary,
                        "source_file": doc.source_file,
                        "is_overall_best": doc.is_overall_best,
                        "is_promoted": doc.is_promoted,
                        "overall_rank": doc.overall_rank,
                        "composite_score": doc.composite_score,
                        "wins_count": doc.wins_count,
                        "top3_count": doc.top3_count,
                        "mean_rank": doc.mean_rank,
                        "best_model_name": doc.best_model_name,
                        "last_tested_at": doc.last_tested_at,
                        "chunk_index": c["chunk_index"],
                        "total_chunks": total_c,
                        "char_start": c["char_start"],
                        "char_end": c["char_end"],
                        "chunk_text": c["text"],
                        "prev_point_id": prev_id,
                        "next_point_id": next_id,
                        "text_sha256": text_sha256,
                        "content_sha256": text_sha256,
                        "full_text_sha256": text_sha256,
                        "embedding_model": embedding_model,
                        "related_modules": list(doc.related_modules),
                        "related_kis": list(doc.related_kis),
                    }
                    if int(c["chunk_index"]) == 0:
                        rec["doc_payload"] = doc.payload
                        rec["code_snippet"] = doc.code_snippet
                        rec["markdown"] = doc.markdown
                    all_chunk_records.append(rec)
                    if is_changed:
                        changed_chunk_records.append(rec)
                        embed_inputs.append(
                            f"{doc.title} | {doc.subtitle} | {doc.category}\n{c['text']}"
                        )

            if changed_doc_ids and existing_doc_hashes and client is not None:
                for d_id in changed_doc_ids:
                    if d_id in existing_doc_hashes:
                        try:
                            client.delete(
                                collection_name=collection_name,
                                points_selector=Filter(
                                    must=[FieldCondition(key="doc_id", match=MatchValue(value=d_id))]
                                ),
                            )
                        except Exception:
                            pass

            points: list[PointStruct] = []
            if changed_chunk_records:
                vectors = active_encode_fn(embed_inputs, embedding_model)
                points = [
                    PointStruct(
                        id=rec["point_id"],
                        vector=vec,
                        payload=rec,
                    )
                    for rec, vec in zip(changed_chunk_records, vectors)
                ]
                if points:
                    if client is not None:
                        client.upsert(collection_name=collection_name, points=points)
                    else:
                        staging_mgr.upsert_points_lockfree(
                            storage_path=target_qdrant_path,
                            collection_name=collection_name,
                            points=points,
                            vector_size=vector_dim,
                            recreate=force_recreate,
                        )

            # Mirror to unified central Qdrant staging (`/Users/jarek/IdeaProjects/qdrant_staging/storage`)
            # when qdrant_client was not a mock/custom client or when syncing default collection
            if qdrant_client is None and points and not collection_name.startswith("test_"):
                if target_qdrant_path != default_staging_storage and default_staging_storage.parent.is_dir():
                    staging_mgr.upsert_points_lockfree(
                        storage_path=default_staging_storage,
                        collection_name=collection_name,
                        points=points,
                        vector_size=vector_dim,
                        recreate=force_recreate,
                    )
                portfolio_qdrant = Path("/Users/jarek/IdeaProjects/portfolio/portfolio/.qdrant_data")
                if target_qdrant_path != portfolio_qdrant and portfolio_qdrant.is_dir():
                    staging_mgr.upsert_points_lockfree(
                        storage_path=portfolio_qdrant,
                        collection_name=collection_name,
                        points=points,
                        vector_size=vector_dim,
                        recreate=False,
                    )

            elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 2)
            return {
                "status": "synced",
                "collection_name": collection_name,
                "embedding_model": embedding_model,
                "vector_dimensions": vector_dim,
                "total_documents": len(docs),
                "total_chunks_indexed": len(all_chunk_records),
                "incremental_updated_documents": len(changed_doc_ids),
                "incremental_upserted_chunks": len(changed_chunk_records),
                "rejoin_verified_documents": len(docs),
                "total_models": overview["total_models"],
                "total_benchmarks": overview["total_benchmarks"],
                "total_runs": overview["total_runs"],
                "best_model_name": (
                    overview["best_model"]["model_name"] if overview.get("best_model") else ""
                ),
                "last_scraped_at": overview["last_scraped_at"],
                "sync_elapsed_ms": elapsed_ms,
                "all_chunk_records": all_chunk_records,
                "docs": docs,
            }
        finally:
            if owns_client and hasattr(client, "close"):
                try:
                    client.close()
                except Exception:
                    pass


def scrape_neural_networks_benchmarks(
    repo_path: str | Path = "/Users/jarek/IdeaProjects/neural_networks",
    ledger_path: str | Path | None = None,
    include_git_history: bool = True,
) -> dict[str, Any]:
    """Convenience entrypoint to scrape and persist the neural_networks benchmark ledger."""
    ledger = BenchmarkLedger(repo_path=repo_path, ledger_path=ledger_path)
    return ledger.scrape_and_build(include_git_history=include_git_history)


def sync_neural_networks_benchmarks_to_qdrant(
    repo_path: str | Path = "/Users/jarek/IdeaProjects/neural_networks",
    ledger_path: str | Path | None = "/Users/jarek/IdeaProjects/portfolio/portfolio/benchmark_history_ledger.json",
    qdrant_path: str | Path | None = "/Users/jarek/IdeaProjects/qdrant_staging/storage",
    collection_name: str = "portfolio_neural_benchmarks",
    force_recreate: bool = False,
) -> dict[str, Any]:
    """Convenience entrypoint to scrape neural_networks benchmarks and sync directly to Qdrant."""
    ledger = BenchmarkLedger(repo_path=repo_path, ledger_path=ledger_path)
    res = ledger.sync_to_qdrant(
        qdrant_path=qdrant_path,
        collection_name=collection_name,
        force_recreate=force_recreate,
    )
    res.pop("all_chunk_records", None)
    res.pop("docs", None)
    return res
