import re


def chunk_text(text: str, size: int = 500, overlap: int = 100) -> list[str]:
    if not text:
        return []
    text = re.sub(r"\s+", " ", text).strip()
    if size <= 0:
        return [text]
    chunks = []
    start = 0
    n = len(text)
    while start < n:
        end = min(start + size, n)
        chunks.append(text[start:end])
        if end >= n:
            break
        start = end - overlap
        if start < 0:
            start = 0
    return chunks
