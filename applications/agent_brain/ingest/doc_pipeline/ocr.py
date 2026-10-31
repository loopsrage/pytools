import io

import numpy as np
import pypdfium2 as pdfium
import pytesseract
from PIL import Image

from pycv.effects import (
    GaussianBlurEffect,
    GrayScaleEffect,
    AdaptiveThresholdEffect,
    DilateEffect,
    ErodeEffect,
)
from pycv.effectchain import chain


def _preprocess(img) -> np.ndarray:
    order = [
        GaussianBlurEffect(kernel_x=3, kernel_y=3, sig_x=0, sig_y=0),
        GrayScaleEffect(),
        AdaptiveThresholdEffect(max_val=255, c=0.8, block_size=3),
        DilateEffect(x=3, y=1),
        ErodeEffect(x=4, y=3),
    ]
    return chain(np.array(img), order)


def ocr_image(image: Image.Image) -> str:
    image = image.convert("RGB")
    processed = _preprocess(image)
    text = pytesseract.image_to_string(processed).strip()
    return text


def extract_text_from_pdf(data: bytes) -> str:
    doc = pdfium.PdfDocument(io.BytesIO(data))
    parts = []
    for page in doc:
        render = page.render(scale=2.0)
        pil_image = render.to_pil()
        parts.append(ocr_image(pil_image))
    doc.close()
    return "\n\n".join(parts)


def extract_text_from_image(data: bytes) -> str:
    image = Image.open(io.BytesIO(data))
    return ocr_image(image)


def extract_text(data: bytes, filename: str) -> str:
    name = filename.lower()
    if name.endswith(".pdf"):
        return extract_text_from_pdf(data)
    if name.endswith((".png", ".jpg", ".jpeg", ".tif", ".tiff", ".bmp", ".webp")):
        return extract_text_from_image(data)
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return ""
