import unittest
import torch

from query_torch_model.torch_local import FastQueueStreamer


class TestTorchLocal(unittest.TestCase):

    def test_fast_queue_streamer_prompt_skip_and_tokens(self):
        streamer = FastQueueStreamer(tokenizer=None, skip_prompt=True)
        streamer.set_prompt_length(2)

        # First 2 puts should be skipped as prompt tokens
        streamer.put(torch.tensor([[10, 11]]))
        streamer.put(torch.tensor([12]))
        assert streamer.token_queue.empty()

        # Subsequent puts should enqueue token IDs
        streamer.put(torch.tensor([[99]]))
        streamer.put(torch.tensor([100]))
        streamer.end()

        out = []
        while True:
            tok = streamer.token_queue.get_nowait()
            if tok is None:
                break
            out.append(tok)
        assert out == [99, 100]


if __name__ == "__main__":
    unittest.main()
