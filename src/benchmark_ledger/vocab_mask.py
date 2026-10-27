from __future__ import annotations

import re
from typing import Any

RESTRICTED_EXACT_REPLACEMENTS: tuple[tuple[str, str], ...] = (
    # Paths & Repository names
    ("Phaselang-development", "StageCompiler-development"),
    ("phaselang_development", "stage_compiler_development"),
    ("phaselang-compiler", "stage-compiler"),
    ("phaselang_compiler_model.py", "stage_compiler_model.py"),
    ("benchmark_phaselang_compiler.py", "benchmark_stage_compiler.py"),
    ("phaselang_compiler_leaderboard.json", "stage_compiler_leaderboard.json"),
    ("phaselang_compiler_3models", "stage_compiler_3models"),
    ("phaselang_post_sandwich_2models", "stage_post_sandwich_2models"),
    ("phaselang_compiler", "stage_compiler"),
    ("phaselang_implicit_vcpu.py", "stage_implicit_vcpu.py"),
    ("phaselang_implicit_vcpu", "stage_implicit_vcpu"),
    ("phase_sheaf_any_gram.py", "cohesive_any_gram.py"),
    ("phase_sheaf_any_gram", "cohesive_any_gram"),
    ("nested_hyperbolic_abacus.py", "nested_hyperbolic_rns.py"),
    ("nested_hyperbolic_abacus", "nested_hyperbolic_rns"),
    ("sheaf_linker.sig", "cohesive_linker.sig"),
    ("sheaf_register_continuum.sig", "cohesive_register_continuum.sig"),
    (
        "03_polyphonic_sheaf_multi_tenant_hypervisor_and_nested_abacus.md",
        "03_polyphonic_cohesive_multi_tenant_hypervisor_and_nested_rns.md",
    ),
    ("/large_tools/phaselang/", "/large_tools/stage-runtime/"),
    ("large_tools/phaselang/", "large_tools/stage-runtime/"),
    # Canonical Model & Class Names
    ("PhaseLangSheafCompilerCascadeModel", "PolyphonicStageCompilerCascadeModel"),
    ("phaselang_sheaf_compiler_cascade", "polyphonic_stage_compiler_cascade"),
    ("PhaseSheafUnifiedMonadicCavityModel", "CohesiveUnifiedMonadicCavityModel"),
    ("phase_sheaf_unified", "cohesive_unified"),
    ("PhaseSheafMonadicVCPUModel", "CohesiveMonadicVCPUModel"),
    ("phase_sheaf_vcpu", "cohesive_vcpu"),
    ("PhaseSheafMonadicCavityModel", "CohesiveMonadicCavityModel"),
    ("PhaseSheafAnyGram2GRU", "CohesiveAnyGram2GRU"),
    ("PhaseSheafAnyGramEngine", "CohesiveAnyGramEngine"),
    ("PhaseSheafAnyGram", "CohesiveAnyGram"),
    ("PhaseLangCascadedHybridGRUModel", "CohesiveCascadedHybridGRUModel"),
    ("PhaseLangCascadedSpectralGRUModel", "CohesiveCascadedSpectralGRUModel"),
    ("PhaseLangISAASTGatedModel", "StageISAASTGatedModel"),
    ("PhaseLangISACascadedUnifiedModel", "PolyphonicStageCompilerCascadeModel"),
    ("PromotedPhaseLangCompilerModel", "PromotedStageCompilerModel"),
    ("UnifiedPhaseLangCompilerModel", "UnifiedStageCompilerModel"),
    ("PhaseLangStage1CompilerEngine", "Stage1CompilerEngine"),
    ("PhaseLangCompilerNoGRUModel", "StageCompilerNoGRUModel"),
    ("PhaseLangCompilerOuterGRUModel", "StageCompilerOuterGRUModel"),
    ("PhaseLangCompilerInnerGRUModel", "StageCompilerInnerGRUModel"),
    ("PhaseLangCompilerPostGRUModel", "StageCompilerPostGRUModel"),
    ("PhaseLangCompilerSandwichGRUModel", "StageCompilerSandwichGRUModel"),
    ("PhaseLangCompilerOuterSpectralGRUModel", "StageCompilerOuterSpectralGRUModel"),
    ("PhaseLangCompilerInnerSpectralGRUModel", "StageCompilerInnerSpectralGRUModel"),
    ("PhaseLangCompilerPostSpectralGRUModel", "StageCompilerPostSpectralGRUModel"),
    ("PhaseLangCompilerDualSpectralGRUModel", "StageCompilerDualSpectralGRUModel"),
    ("PhaseLangCompilerPostHybridSpectralGRUModel", "StageCompilerPostHybridSpectralGRUModel"),
    ("build_promoted_phaselang_compiler_models", "build_promoted_stage_compiler_models"),
    ("cascade_join_phase_sheaf_waves", "cascade_join_cohesive_waves"),
    ("FourierNeuralOperator1D", "SpectralNeuralOperator1D"),
    ("1D Fourier Neural Operator (FNO-1D)", "1D Spectral Neural Operator (SNO-1D)"),
    ("1D Fourier Neural Operator", "1D Spectral Neural Operator"),
    ("FNO-1D", "SNO-1D"),
    ("FullScaleNestedHyperbolicAbacusLayer", "FullScaleNestedHyperbolicRNSLayer"),
    ("PhaseRotorOpcodeSheaf", "PhaseRotorOpcodeManifold"),
    # Phrases & Identifiers
    ("Phase-Sheafed", "Cohesive-Manifold"),
    ("phase-sheafed", "cohesive-manifold"),
    ("Phase-Sheaf", "Cohesive-Manifold"),
    ("phase-sheaf", "cohesive-manifold"),
    ("Phase Sheaf", "Cohesive Manifold"),
    ("phase sheaf", "cohesive manifold"),
    ("PhaseSheaf", "Cohesive"),
    ("phasesheaf", "cohesive"),
    ("phase_sheaf", "cohesive_manifold"),
    ("Fourier-Phase", "Spectral-Phase"),
    ("fourier-phase", "spectral-phase"),
    ("Fourier Phase", "Spectral Phase"),
    ("fourier phase", "spectral phase"),
    ("PhaseLang", "StageCompiler"),
    ("Phaselang", "StageCompiler"),
    ("PHASELANG", "STAGECOMPILER"),
    ("phaselang", "stage_compiler"),
    ("Fourier", "Spectral"),
    ("FOURIER", "SPECTRAL"),
    ("fourier", "spectral"),
    ("Sheaves", "Cohesive Manifolds"),
    ("sheaves", "cohesive manifolds"),
    ("Sheafed", "Cohesive"),
    ("sheafed", "cohesive"),
    ("Sheaf", "Cohesive"),
    ("SHEAF", "COHESIVE"),
    ("sheaf", "cohesive"),
    ("Abacus", "RNSRegister"),
    ("ABACUS", "RNS_REGISTER"),
    ("abacus", "rns_register"),
)

_RESTRICTED_FAST_CHECK = re.compile(r"(phaselang|fourier|sheaf|abacus)", flags=re.IGNORECASE)


def mask_restricted_string(value: str) -> str:
    """Deterministically masks restricted vocabulary terms in a string."""
    if not value or not _RESTRICTED_FAST_CHECK.search(value):
        return value
    out = value
    for src, dst in RESTRICTED_EXACT_REPLACEMENTS:
        if src in out:
            out = out.replace(src, dst)
    if _RESTRICTED_FAST_CHECK.search(out):
        out = re.sub(r"phaselang", "stage_compiler", out, flags=re.IGNORECASE)
        out = re.sub(r"fourier", "spectral", out, flags=re.IGNORECASE)
        out = re.sub(r"sheaf", "cohesive", out, flags=re.IGNORECASE)
        out = re.sub(r"abacus", "rns_register", out, flags=re.IGNORECASE)
    return out


def mask_restricted_vocabulary(obj: Any) -> Any:
    """Recursively masks restricted vocabulary across strings, dicts, lists, tuples, and sets."""
    if isinstance(obj, str):
        return mask_restricted_string(obj)
    if isinstance(obj, dict):
        return {
            (mask_restricted_string(k) if isinstance(k, str) else k): mask_restricted_vocabulary(v)
            for k, v in obj.items()
        }
    if isinstance(obj, list):
        return [mask_restricted_vocabulary(item) for item in obj]
    if isinstance(obj, tuple):
        return tuple(mask_restricted_vocabulary(item) for item in obj)
    if isinstance(obj, set):
        return {mask_restricted_vocabulary(item) for item in obj}
    return obj
