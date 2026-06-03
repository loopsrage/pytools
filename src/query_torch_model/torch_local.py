import asyncio
import queue
import threading

import torch
from transformers import AutoModelForCausalLM, AutoTokenizer, BitsAndBytesConfig
from transformers.generation.streamers import BaseStreamer
from peft import PeftModel
from thread_safe.onceler import Onceler


one_time = Onceler()
model_lock = threading.Lock()

class FastQueueStreamer(BaseStreamer):
    def __init__(self, tokenizer, skip_prompt=True):
        self.tokenizer = tokenizer
        self.skip_prompt = skip_prompt
        self.token_queue = queue.Queue()
        self.prompt_skipped = False
        self.prompt_tokens_to_skip = 0

    def put(self, value):
        if len(value.shape) > 1:
            token_id = value[0][-1].item()
        else:
            token_id = value[-1].item()

        if self.skip_prompt and not self.prompt_tokens_to_skip > 0:
            self.prompt_tokens_to_skip -= 1
            return

        self.token_queue.put(token_id)

    def end(self):
        self.token_queue.put(None)

    def set_prompt_length(self, length):
        self.prompt_tokens_to_skip = length


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
        streamer = FastQueueStreamer(tokenizer, skip_prompt=True)
        streamer.set_prompt_length(inputs["input_ids"].shape[1])

        generation_kwargs = {
            **inputs,
            "streamer": streamer,
            "max_tokens": max_tokens,
            "do_sample": temp > 0.0,
            "use_cache": True,
            "temperature": temp if temp > 0.0 else None,
            "eos_token_id": [tokenizer.eos_token_id, tokenizer.convert_tokens_to_ids("<|im_end|>")]
        }

        generation_thread = threading.Thread(target=model.generate, kwargs=generation_kwargs)
        generation_thread.start()

        tokens_acc = []
        full_response = ""
        stop_tokens = ["<|endoftext|>", "<|im_end|>"]
        stop_ids = {tokenizer.eos_token_id, tokenizer.convert_tokens_to_ids("<|im_end|>")}

        while True:
            token_id = streamer.token_queue.get()
            if token_id is None or token_id in stop_ids:
                break

            tokens_acc.append(token_id)

            if len(tokens_acc) >= 3:
                new_text = tokenizer.decode(tokens_acc, skip_special_tokens=False)
                full_response += new_text
                if verbose:
                    print(new_text, end="", flush=True)
                tokens_acc = []

                if any(stop in full_response for stop in stop_tokens):
                    break

        if tokens_acc:
            full_response += tokenizer.decode(tokens_acc, skip_special_tokens=False)

        for stop in stop_tokens:
            full_response = full_response.split(stop)[0]

        generation_thread.join()
        return "<think>\n" + full_response

async def async_query_torch_model(*args, **kwargs):
    return await asyncio.to_thread(query_torch_model, *args, **kwargs)
