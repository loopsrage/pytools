import json

def format_grpo(prompt_msgs, truth):
    return json.dumps({"prompt": prompt_msgs, "answer": str(truth)})

def format_dpo(pos, neg, system_prompt, user_prompt):
    return json.dumps({
        "prompt": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        "chosen": pos,
        "rejected": neg
    })

def prompt_msg(dev_str, query):
    return [{"role": "developer", "content": dev_str},
        {"role": "user", "content": query}]

def prompt_messages(prompt_msgs, content):
    return json.dumps({"messages": prompt_msgs + [{"role": "assistant", "content": content}]})

def assistant_msg(dev_str, query, response):
    return json.dumps({"messages": prompt_msg(dev_str, query) + [{"role": "assistant", "content": response}]})
