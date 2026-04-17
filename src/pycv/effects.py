from abc import ABC, abstractmethod
from typing import Optional

import cv2
import numpy as np


class Effect(ABC):
    @abstractmethod
    def apply(self, img: np.ndarray) -> np.ndarray:
        pass

class Standard:
    def __init__(self):
        self.original: Optional[np.ndarray] = None
        self.cpy: Optional[np.ndarray] = None

    def copy(self, from_img: np.ndarray):
        # In Python, .copy() performs a deep copy of the underlying C-buffer
        # This is the direct equivalent of from.CopyTo(s.cpy)
        self.cpy = from_img.copy()

class MorphEffect(Effect, Standard):
    def __init__(self, kernel_size: int, morph_type: int = cv2.MORPH_OPEN):
        super().__init__()
        self.kernel_size = kernel_size
        self.morph_type = morph_type

    def apply(self, img: np.ndarray) -> np.ndarray:
        return morph_effect(img, self.kernel_size, self.morph_type)

def morph_effect(img: np.ndarray, kernel_size: int, morph_type: int) -> np.ndarray:
    kernel = cv2.getStructuringElement(
        cv2.MORPH_RECT,
        (kernel_size, kernel_size)
    )
    out_image = cv2.morphologyEx(img, morph_type, kernel)
    return out_image

class DilateEffect(Effect, Standard):
    def __init__(self, x: int, y: int):
        super().__init__()
        self.x = x
        self.y = y

    def apply(self, input_img: np.ndarray) -> np.ndarray:
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (self.x, self.y))
        dilated_img = cv2.dilate(input_img, kernel, iterations=1)
        return dilated_img

class AdaptiveThresholdEffect(Effect, Standard):
    def __init__(
            self,
            max_val: float = 255.0,
            c: float = 2.0,
            block_size: int = 11,
            threshold_type: int = cv2.THRESH_BINARY,
            adaptive_method: int = cv2.ADAPTIVE_THRESH_GAUSSIAN_C
    ):
        super().__init__()
        self.max_val = max_val
        self.c = c
        # BlockSize must be an odd number (3, 5, 7, etc.)
        self.block_size = block_size
        self.threshold_type = threshold_type
        self.adaptive_method = adaptive_method

    def apply(self, input_img: np.ndarray) -> np.ndarray:
        # Ensure image is single-channel (grayscale)
        if len(input_img.shape) == 3:
            input_img = cv2.cvtColor(input_img, cv2.COLOR_BGR2GRAY)

        return cv2.adaptiveThreshold(
            input_img,
            self.max_val,
            self.adaptive_method,
            self.threshold_type,
            self.block_size,
            self.c
        )

class ThresholdBinaryEffect(Effect, Standard):
    def __init__(self, min_val: float, max_val: float = 255.0):
        super().__init__()
        self.min_val = min_val
        self.max_val = max_val

    def apply(self, input_img: np.ndarray) -> np.ndarray:
        # Ensure grayscale for thresholding
        if len(input_img.shape) == 3:
            input_img = cv2.cvtColor(input_img, cv2.COLOR_BGR2GRAY)

        # cv2.threshold returns a tuple: (ret_threshold, thresholded_image)
        _, thresh_img = cv2.threshold(
            input_img,
            self.min_val,
            self.max_val,
            cv2.THRESH_BINARY
        )

        return thresh_img


class CannyEffect(Effect, Standard):
    def __init__(self, threshold: float, max_val: float):
        super().__init__()
        self.threshold = threshold
        self.max_val = max_val

    def apply(self, input_img: np.ndarray) -> np.ndarray:
        # Canny works best on grayscale images
        if len(input_img.shape) == 3:
            input_img = cv2.cvtColor(input_img, cv2.COLOR_BGR2GRAY)

        # Apply Canny Edge Detection
        # threshold = first threshold for the hysteresis procedure
        # max_val = second threshold for the hysteresis procedure
        edges = cv2.Canny(
            input_img,
            threshold1=self.threshold,
            threshold2=self.max_val
        )

        return edges

class ErodeEffect(Effect, Standard):
    def __init__(self, x: int, y: int):
        super().__init__()
        self.x = x
        self.y = y

    def apply(self, input_img: np.ndarray) -> np.ndarray:
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (self.x, self.y))

        # Apply erosion
        eroded_img = cv2.erode(input_img, kernel, iterations=1)

        return eroded_img

class GaussianBlurEffect(Effect, Standard):
    def __init__(
            self,
            kernel_x: int,
            kernel_y: int,
            sig_x: float = 0,
            sig_y: float = 0,
            border_type: int = cv2.BORDER_DEFAULT
    ):
        super().__init__()
        self.kernel = (kernel_x, kernel_y)
        self.sig_x = sig_x
        self.sig_y = sig_y
        self.border_type = border_type

    def apply(self, input_img: np.ndarray) -> np.ndarray:
        # Apply Gaussian Blur
        blurred_img = cv2.GaussianBlur(
            input_img,
            self.kernel,
            sigmaX=self.sig_x,
            sigmaY=self.sig_y,
            borderType=self.border_type
        )

        return blurred_img


class GrayScaleEffect(Effect, Standard):
    def __init__(self):
        super().__init__()

    def apply(self, input_img: np.ndarray) -> np.ndarray:
        # Check if the image is already grayscale
        if len(input_img.shape) == 2:
            return input_img

        # gocvf.ConvertToGrayScale -> cv2.COLOR_BGR2GRAY
        gray_img = cv2.cvtColor(input_img, cv2.COLOR_BGR2GRAY)

        return gray_img

class InvertEffect(Effect, Standard):
    def __init__(self):
        super().__init__()

    def apply(self, input_img: np.ndarray) -> np.ndarray:
        # For uint8 images, this flips 0 to 255 and 255 to 0
        return cv2.bitwise_not(input_img)


class ThresholdOtsuEffect(Effect, Standard):
    def __init__(self):
        super().__init__()

    def apply(self, input_img: np.ndarray) -> np.ndarray:
        # Otsu requires a single-channel grayscale image
        if len(input_img.shape) == 3:
            input_img = cv2.cvtColor(input_img, cv2.COLOR_BGR2GRAY)

        # 255 is the max value assigned to pixels above the threshold.
        _, otsu_img = cv2.threshold(
            input_img,
            0,
            255,
            cv2.THRESH_BINARY + cv2.THRESH_OTSU
        )

        return otsu_img

class ResizeEffect(Effect, Standard):
    def __init__(
            self,
            width: int = 0,
            height: int = 0,
            fx: float = 0.0,
            fy: float = 0.0,
            interpolation: int = cv2.INTER_LINEAR
    ):
        super().__init__()
        self.sz = (width, height)  # image.Point equivalent
        self.fx = fx
        self.fy = fy
        self.interpolation = interpolation

    def apply(self, input_img: np.ndarray) -> np.ndarray:
        # If fx/fy are provided, dsize (self.sz) must be (0,0) or None
        dsize = self.sz if (self.sz[0] > 0 and self.sz[1] > 0) else None

        # gocvf.Resize -> cv2.resize
        resized_img = cv2.resize(
            input_img,
            dsize=dsize,
            fx=self.fx,
            fy=self.fy,
            interpolation=self.interpolation
        )

        return resized_img

class ThinningEffect(Effect, Standard):
    def __init__(self, thinning_type: int = cv2.THINNING_ZHANGSUEN):
        super().__init__()
        self.thinning_type = thinning_type

    def apply(self, input_img: np.ndarray) -> np.ndarray:
        # Thinning strictly requires a binary 8-bit image (0 and 255)
        if len(input_img.shape) == 3:
            input_img = cv2.cvtColor(input_img, cv2.COLOR_BGR2GRAY)

        # Apply the thinning algorithm
        # Common types: cv2.ximgproc.THINNING_ZHANGSUEN (default)
        # or cv2.ximgproc.THINNING_GUOHALL
        thinned_img = cv2.ximgproc.thinning(input_img, thinningType=self.thinning_type)

        return thinned_img