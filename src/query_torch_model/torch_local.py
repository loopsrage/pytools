import asyncio
import threading

import torch
from transformers import AutoModelForCausalLM, AutoTokenizer, TextIteratorStreamer, BitsAndBytesConfig
from peft import PeftModel
from thread_safe.onceler import Onceler


one_time = Onceler()
model_lock = threading.Lock()

def query_torch_model(query, adapter, dev_str, model, verbose=False, max_tokens=2048, temp=0.0, dtype = None, quantized: bool=False):

    if adapter == model or adapter is None:
        actual_adapter = None
    else:
        actual_adapter = adapter

    if dtype is None:
        dtype = torch.bfloat16

    def load_torch_model(m, a):
        def loader():
            tokenizer = AutoTokenizer.from_pretrained(m)

            if quantized:
                cfg = BitsAndBytesConfig(
                    load_in_4bit=True,
                    bnb_4bit_quant_type="nf4",
                    bnb_4bit_compute_dtype=dtype,
                    bnb_4bit_use_double_quant=True,
                )
                loader_kwargs = {"quantization_config": cfg, "low_cpu_mem_usage": True}
            else:
                loader_kwargs = {"dtype": dtype}

            base_model = AutoModelForCausalLM.from_pretrained(m, device_map="auto", attn_implementation="sdpa", **loader_kwargs)

            if a:
                model = PeftModel.from_pretrained(base_model, a)
            else:
                model = base_model

            if not a:
                model = torch.compile(model)

            return model, tokenizer
        return loader

    model, tokenizer = one_time.store_once("models", model, load_torch_model(model, actual_adapter))

    if not query:
        return None

    with model_lock:
        prompt = (
            f"<|im_start|>developer\n"
            f"{dev_str()}<|im_end|>\n"
            f"<|im_start|>user\n"
            f"{query}<|im_end|>\n"
            f"<|im_start|>assistant\n"
            f"<think>\n"
        )

        inputs = tokenizer(prompt, return_tensors="pt").to(model.device)
        streamer = TextIteratorStreamer(tokenizer, skip_prompt=True, skip_special_tokens=False)

        generation_kwargs = {
            **inputs,
            "streamer": streamer,
            "max_new_tokens": max_tokens,
            "do_sample": temp > 0.0,
            "use_cache": True,
            "temperature": temp if temp > 0.0 else None,
            "eos_token_id": [tokenizer.eos_token_id, tokenizer.convert_tokens_to_ids("<|im_end|>")]
        }

        generation_thread = threading.Thread(target=model.generate, kwargs=generation_kwargs)
        generation_thread.start()

        full_response = ""
        stop_tokens = ["<|endoftext|>", "<|im_end|>"]

        for new_text in streamer:
            full_response += new_text
            if verbose:
                print(new_text, end="", flush=True)

            if any(stop in full_response for stop in stop_tokens):
                for stop in stop_tokens:
                    full_response = full_response.split(stop)[0]
                break

        generation_thread.join()
        return "<think>\n" + full_response

async def async_query_torch_model(*args, **kwargs):
    return await asyncio.to_thread(query_torch_model, *args, **kwargs)
