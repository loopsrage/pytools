import json
import unittest

from training_message_formats.message_formats import (
    assistant_msg,
    format_dpo,
    format_grpo,
    prompt_messages,
    prompt_msg,
)


class TestMessageFormats(unittest.TestCase):

    def test_all_message_formatters(self):
        p_msgs = prompt_msg("sys_inst", "user_q")
        assert len(p_msgs) == 2
        assert p_msgs[0] == {"role": "developer", "content": "sys_inst"}
        assert p_msgs[1] == {"role": "user", "content": "user_q"}

        grpo = json.loads(format_grpo(p_msgs, 42))
        assert grpo["prompt"] == p_msgs and grpo["answer"] == "42"

        dpo = json.loads(format_dpo("good", "bad", "sys", "usr"))
        assert dpo["chosen"] == "good" and dpo["rejected"] == "bad"
        assert len(dpo["prompt"]) == 2

        pm = json.loads(prompt_messages(p_msgs, "ans"))
        assert len(pm["messages"]) == 3
        assert pm["messages"][-1] == {"role": "assistant", "content": "ans"}

        am = json.loads(assistant_msg("dev", "q", "resp"))
        assert len(am["messages"]) == 3
        assert am["messages"][-1]["content"] == "resp"


if __name__ == "__main__":
    unittest.main()
