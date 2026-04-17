import asyncio
from asyncio import QueueEmpty, CancelledError

import cv2
import numpy as np
from typing import List

import pytesseract

from src.pycv.effects import ErodeEffect, AdaptiveThresholdEffect, GaussianBlurEffect, GrayScaleEffect, DilateEffect, \
    Effect
from src.pycv.helpers import get_horizontal_and_vertical_lines, extract_cell_bounds, sort_and_filter_rects
from src.queue_controller.helpers import new_controller, start_pipeline, stop_pipeline
from src.queue_controller.queueController import QueueController
from src.queue_controller.queueData import QueueData
from src.thread_safe.index import Index


def chain(img: np.ndarray, order: List[Effect], debug=None) -> np.ndarray:
    p = img
    if debug is None:
        debug = False

    for effect in order:
        try:
            out = effect.apply(p)

            if debug:
                cv2.imshow("Debug", out)
                cv2.waitKey(0)

            p = out
        except Exception as e:
            print(f"Error applying effect: {e}")
            raise e

    return p

async def image_word_isolation_pipeline(tbd: Index):
    def queue_action_2(queue_data: QueueData) -> None:
        # Read Attributes (Rect is (x, y, w, h))
        img_mat = queue_data['Image']
        x, y, w, h = queue_data.attribute('Rect')

        # Region Crop (Slicing: [y:y+h, x:x+w])
        cell_img = img_mat[y:y+h, x:x+w]

        if cell_img.size == 0:
            raise Exception("empty image")

        # Note: .copy() ensures the chain doesn't mutate the original page Mat
        cp = cell_img.copy()
        processed_img = chain(cp, word_denoise_chain())

        if processed_img is None or processed_img.size == 0:
            raise Exception("empty image after processing")

        # We can pass the NumPy array directly to pytesseract without re-encoding to PNG
        output_text = pytesseract.image_to_string(processed_img).strip()

        if not output_text:
            return

        path = queue_data.attribute_from_derivative("path", "")
        # If you still need the PNG bytes for the queue/storage:
        _, buffer = cv2.imencode(".png", processed_img)
        queue_data.set_attribute("FileBytes", buffer.tobytes())
        queue_data.set_attribute("OCRText", output_text)
        y_axis, _ = tbd.load_or_store_in_index(path, str(y), [])
        y_axis.append(output_text)

    def accum_node(after: QueueController):

        async def queue_action(queue_data: QueueData) -> None:
            pil_image = queue_data.attribute("Image")
            pil_image.convert('RGB')
            # 5. Convert PIL to OpenCV Mat (BGR)
            img_mat = cv2.cvtColor(np.array(pil_image), cv2.COLOR_RGB2BGR)

            # 6. Process Image
            gray = cv2.cvtColor(img_mat, cv2.COLOR_BGR2GRAY)
            grid_mask = get_horizontal_and_vertical_lines(gray, 1)

            contours, _ = cv2.findContours(
                grid_mask,
                cv2.RETR_TREE,
                cv2.CHAIN_APPROX_TC89_L1
            )

            rects = extract_cell_bounds(contours)
            sorted_rects = sort_and_filter_rects(rects, min_area=900, max_area=-1)

            print(f"contours {len(contours)} rects {len(rects)} sorted {len(sorted_rects)}")

            # 10. Queue Derivative Actions
            for i, rect in enumerate(sorted_rects):
                der = queue_data.copy_derivative(str(i))
                der.set_attribute("Rect", rect)

                der.set_attribute("Image", img_mat)
                await after.enqueue(der)

        return new_controller(action=queue_action)

    rect_node = new_controller(action=queue_action_2)
    return [accum_node(rect_node), rect_node]


def word_denoise_chain() -> list[Effect]:
    return [
        # 1. Blur to smooth out paper texture
        GaussianBlurEffect(kernel_x=3, kernel_y=3, sig_x=0, sig_y=0, border_type=cv2.BORDER_DEFAULT),

        # 2. Convert to Grayscale
        GrayScaleEffect(),

        # 3. Adaptive Thresholding (Note: gocv .8 C mapping)
        AdaptiveThresholdEffect(
            max_val=255,
            c=0.8,
            block_size=3,
            threshold_type=cv2.THRESH_BINARY,
            adaptive_method=cv2.ADAPTIVE_THRESH_GAUSSIAN_C
        ),

        # 4. Dilate (3x1) - Stretch horizontally to connect characters into a word
        DilateEffect(x=3, y=1),

        # 5. Erode (4x3) - Clean up edges and slightly thin the resulting blocks
        ErodeEffect(x=4, y=3)
    ]

def word_isolation_chain() -> list[Effect]:
    return [
        # 1. Start with Grayscale
        GrayScaleEffect(),

        # 2. Heavier Blur (5x5) to remove finer details
        GaussianBlurEffect(
            kernel_x=5,
            kernel_y=5,
            sig_x=0,
            sig_y=0,
            border_type=cv2.BORDER_DEFAULT
        ),

        # 3. Adaptive Threshold with C=3 (slightly more conservative than the 0.8 in Denoise)
        AdaptiveThresholdEffect(
            max_val=255,
            c=3.0,
            block_size=3,
            threshold_type=cv2.THRESH_BINARY,
            adaptive_method=cv2.ADAPTIVE_THRESH_GAUSSIAN_C
        ),

        # 4. Massive Erosion (15x3)
        # This will eliminate any white objects thinner than 15 pixels horizontally
        ErodeEffect(x=15, y=3)
    ]