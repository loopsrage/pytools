import json

import yaml
from mlx_lm import stream_generate, load
from mlx_lm.sample_utils import make_sampler

def user_string(prompt, tables):
    for v in tables["tables"]:
        prompt = f"{prompt}:\n\n{yaml.dump(json.loads(tables["tables"][v]))}\n\n"
    return prompt

def developer_string():
    return (f"Check for similarities between description strings.\n"
            f"Provide an detailed analysis and audit of the difference.\n")

async def unfuzed_parse_response( query, adapter, model, verbose=False, max_tokens=2048, temp = 0.0):

    if adapter == model or adapter is None:
        actual_adapter = None
    else:
        actual_adapter = adapter

    model, tokenizer = load(
        model,
        adapter_path=actual_adapter
    )

    if not query:
        return None

    prompt = (
        f"<|im_start|>developer\n"
        f"{developer_string()}<|im_end|>\n"
        f"<|im_start|>user\n"
        f"{query}<|im_end|>\n"
        f"<|im_start|>assistant\n"
        f"<think>\n"
    )

    sampler = make_sampler(temp=temp)
    full_response = ""
    stop_tokens = ["<|endoftext|>", "<|im_end|>"]
    for res in stream_generate(model, tokenizer, prompt, max_tokens=max_tokens, sampler=sampler):
        full_response += res.text
        if verbose:
            print(res.text, end="", flush=True)

        if res.finish_reason is not None:
            break

        if any(stop in full_response for stop in stop_tokens):
            for stop in stop_tokens:
                full_response = full_response.split(stop)[0]
            break

    return "<think>\n" + full_response