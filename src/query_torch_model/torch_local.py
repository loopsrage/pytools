import asyncio
import threading
from transformers import AutoModelForCausalLM, AutoTokenizer, TextIteratorStreamer
from peft import PeftModel
from thread_safe.onceler import Onceler


one_time = Onceler()

def query_torch_model(query, adapter, dev_str, model, verbose=False, max_tokens=2048, temp=0.0):

    if adapter == model or adapter is None:
        actual_adapter = None
    else:
        actual_adapter = adapter

    def load_torch_model(m, a):
        def loader():
            tokenizer = AutoTokenizer.from_pretrained(m)
            base_model = AutoModelForCausalLM.from_pretrained(m, device_map="auto")
            if a:
                model = PeftModel.from_pretrained(base_model, a)
            else:
                model = base_model

            return model, tokenizer
        return loader

    model, tokenizer = one_time.store_once("models", model, load_torch_model(model, actual_adapter))

    if not query:
        return None

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
