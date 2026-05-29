import rapidfuzz
from rapidfuzz import fuzz


def fuzz_one_against_choices(target: str, choices: list[str], threshold: float):
    choice_strings = [c for c in choices]
    target_strings = [target for _ in range(len(choices))]
    results = rapidfuzz.process.cpdist(
        target_strings,
        choice_strings,
        scorer=fuzz.partial_token_sort_ratio,
        workers=-1,
        score_cutoff=float(threshold)
    )
    return dict(zip(choices, results.tolist()))
