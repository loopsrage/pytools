import asyncio
import threading

from mlx_lm import stream_generate, load
from mlx_lm.sample_utils import make_sampler
from thread_safe.onceler import Onceler


one_time = Onceler()
MODEL_LOCK = threading.Lock()
ASYNC_MODEL_LOCK = asyncio.Lock()

def unfuzed_parse_response( query, adapter, model, developer_string, verbose=False, max_tokens=2048, temp = 0.0):

    if adapter == model or adapter is None:
        actual_adapter = None
    else:
        actual_adapter = adapter

    def x(m, a ):
        def y():
            return load(
                m,
                adapter_path=a
            )
        return y

    with MODEL_LOCK:
        model, tokenizer = one_time.store_once("models", model, x(model, actual_adapter))
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


async def async_unfuzed_parse_response(*args, **kwargs):
    async with ASYNC_MODEL_LOCK:
        return await asyncio.to_thread(unfuzed_parse_response, *args, **kwargs)

