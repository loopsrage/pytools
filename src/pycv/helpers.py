import math
import random
import secrets

import cv2
import numpy as np


def random_rect(img: np.ndarray, point_x: int, point_y: int) -> tuple:
    """Calculates a random rectangle within a bounds defined by point."""
    # img.shape is (height, width, channels)
    target_height, target_width = img.shape[:2]

    max_x = point_x - target_width
    max_y = point_y - target_height

    # Using random.randint for inclusivity like Intn(maxX + 1)
    rand_x = random.randint(0, max_x)
    rand_y = random.randint(0, max_y)

    # Returns (x1, y1, x2, y2)
    return (rand_x, rand_y, rand_x + target_width, rand_y + target_height)

def full_rect(img: np.ndarray) -> tuple:
    """Returns the full rectangle dimensions of the image."""
    height, width = img.shape[:2]
    return (0, 0, width, height)

def rb() -> int:
    """Returns a random uint8 byte using cryptographically strong random."""
    return secrets.token_bytes(1)[0]

def rand_color() -> list[int]:
    """Returns a random BGR color as uint8."""
    return [rb(), rb(), rb()]

def rand_color_float64() -> list[float]:
    """Returns a random BGR color as float64."""
    return [float(c) for c in rand_color()]

def extract_cell_bounds(contours: list[np.ndarray]) -> list[tuple]:
    """
    Replicates gocv.BoundingRect for a vector of points.
    Returns a list of rectangles in (x, y, w, h) format.
    """
    cell_rects = []

    for contour in contours:
        # cv2.boundingRect returns (x, y, width, height)
        rect = cv2.boundingRect(contour)
        cell_rects.append(rect)

    return cell_rects


def sort_and_filter_rects(rects, min_area, max_area=-1):
    """
    Filters rects by area and sorts them by rows, then columns.
    rects: List of (x, y, w, h) tuples
    """
    # 1. Filter by Area
    filtered = []
    for (x, y, w, h) in rects:
        area = w * h
        if max_area != -1:
            if min_area < area < max_area:
                filtered.append((x, y, w, h))
        else:
            if area > min_area:
                filtered.append((x, y, w, h))

    filtered.sort(key=lambda r: (r[1] // 10, r[0]))
    return filtered

def get_horizontal_and_vertical_lines(binary_img: np.ndarray, kernel_length: int) -> np.ndarray:
    """
    Detects and combines horizontal and vertical lines in a binary image.
    Replicates the gocv Erode/Dilate/AddWeighted logic.
    """
    # 1. Create Kernels (Structuring Elements)
    # Horizontal: (kernel_length x 1), Vertical: (1 x kernel_length)
    horiz_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (kernel_length, 1))
    vert_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, kernel_length))

    # 2. Detect Horizontal Lines
    # Morphological Opening: Erosion then Dilation
    horizontal = cv2.erode(binary_img, horiz_kernel, iterations=1)
    horizontal = cv2.dilate(horizontal, horiz_kernel, iterations=1)

    # 3. Detect Vertical Lines
    vertical = cv2.erode(binary_img, vert_kernel, iterations=1)
    vertical = cv2.dilate(vertical, vert_kernel, iterations=1)

    # 4. Combine both using Weighted Sum
    # Replicates gocv.AddWeighted(horizontal, 0.5, vertical, 0.5, 0.0, &combined)
    combined = cv2.addWeighted(horizontal, 0.5, vertical, 0.5, 0.0)

    # Optional: Threshold the result to make it a pure binary mask again
    _, combined = cv2.threshold(combined, 0, 255, cv2.THRESH_BINARY | cv2.THRESH_OTSU)

    return combined

def draw_line(img: np.ndarray, pt1: tuple, pt2: tuple, col: tuple, thickness: int):
    """
    Draws a line on the image.
    col should be a BGR tuple like (255, 0, 0)
    """
    # cv2.line modifies the image in-place
    cv2.line(img, pt1, pt2, col, thickness)

def draw_angle_line(img: np.ndarray, angle_rad: float, col: tuple, thickness: int):
    """
    Draws a line representing the dominant angle, centered on the image.
    """
    height, width = img.shape[:2]
    cx, cy = width / 2.0, height / 2.0

    # Calculate a point on the line far from the center
    line_length = min(width, height) / 2.0

    # Cos/Sin math is identical to Go
    x1 = cx + line_length * math.cos(angle_rad)
    y1 = cy + line_length * math.sin(angle_rad)
    x2 = cx - line_length * math.cos(angle_rad)
    y2 = cy - line_length * math.sin(angle_rad)

    # Call draw_line with integer coordinates
    draw_line(
        img,
        (int(x1), int(y1)),
        (int(x2), int(y2)),
        col,
        thickness
    )

def draw_hough_lines(img: np.ndarray, lines: np.ndarray, col: tuple, thickness: int):
    """
    Draws lines detected by HoughLinesP.
    lines: NumPy array of shape (N, 1, 4)
    """
    if lines is not None:
        for line in lines:
            # Flatten the nested array to get x1, y1, x2, y2
            x1, y1, x2, y2 = line[0]
            cv2.line(img, (x1, y1), (x2, y2), col, thickness)

def get_rotation_matrix(img: np.ndarray, angle: float, scale: float) -> np.ndarray:
    """
    Calculates the 2D rotation matrix around the image center.
    """
    rows, cols = img.shape[:2]
    center = (cols / 2, rows / 2)
    # Replicates gocv.GetRotationMatrix2D
    return cv2.getRotationMatrix2D(center, angle, scale)

def warp_affine(img: np.ndarray, rotation_matrix: np.ndarray) -> np.ndarray:
    """
    Applies the rotation matrix to the image.
    """
    rows, cols = img.shape[:2]
    # Replicates gocv.WarpAffine
    # Note: dsize is (width, height) in OpenCV
    return cv2.warpAffine(img, rotation_matrix, (cols, rows))


def line_angles(lines: np.ndarray) -> tuple[float, float]:
    """
    Calculates the dominant angle from detected Hough lines using a circular average.
    """
    sum_sin, sum_cos = 0.0, 0.0

    if lines is None:
        return 0.0, 0.0

    for line in lines:
        x1, y1, x2, y2 = map(float, line[0])

        # Calculate angle and length
        angle = math.atan2(y2 - y1, x2 - x1)

        # Wrap to [0, π)
        if angle < 0:
            angle += math.pi

        length = math.sqrt((x2 - x1)**2 + (y2 - y1)**2)

        # Weighted circular average (doubling angle for line orientation)
        sum_sin += length * math.sin(2 * angle)
        sum_cos += length * math.cos(2 * angle)

    if sum_cos == 0 and sum_sin == 0:
        dominant_rad = 0.0
    else:
        # Divide by 2 to return to line orientation
        dominant_rad = 0.5 * math.atan2(sum_sin, sum_cos)
        if dominant_rad < 0:
            dominant_rad += math.pi

    dominant_deg = math.degrees(dominant_rad)
    return dominant_deg, dominant_rad

def deskew(img: np.ndarray, dominant_rad: float) -> np.ndarray:
    """
    Corrects image skew based on the dominant angle.
    """
    dominant_deg = math.degrees(dominant_rad)

    # Logic matches Go version: normalize to horizontal
    deskew_angle = dominant_deg - 90
    if dominant_deg > 90:
        deskew_angle = dominant_deg - 180

    print(f"Detected dominant angle (radians): {dominant_rad:.2f}")
    print(f"Deskewing angle (degrees): {deskew_angle:.2f}")

    # Use existing helper functions
    matrix = get_rotation_matrix(img, deskew_angle, 1.0)
    return warp_affine(img, matrix)

def find_median_angle(lines: np.ndarray) -> float:
    """
    Calculates the median angle of near-horizontal lines to reduce outlier noise.
    lines: NumPy array of shape (N, 1, 4) from HoughLinesP
    """
    angles = []

    if lines is None:
        return 0.0

    for line in lines:
        # Unpack x1, y1, x2, y2 from the Hough result
        x1, y1, x2, y2 = line

        # Calculate angle in degrees
        angle_rad = math.atan2(float(y2 - y1), float(x2 - x1))
        angle_deg = math.degrees(angle_rad)

        # Only consider near-horizontal lines (within 45 degrees)
        if abs(angle_deg) < 45:
            angles.append(angle_deg)

    if not angles:
        return 0.0

    # NumPy's median handles sorting internally
    return float(np.median(angles))


def deskew_by_canny_hough_lines(input_img: np.ndarray, rotate_img: np.ndarray) -> np.ndarray:
    """
    Detects skew using Canny + Hough and rotates the target image.
    input_img: Image used for detection (usually grayscale/binary).
    rotate_img: The actual image to be rotated (can be the same or the original color image).
    """
    # 1. Canny Edge Detection (Logic from your CannyEffect)
    # Using 50, 150 thresholds as per your Go code
    edges = cv2.Canny(input_img, 50, 150)

    # 2. Probabilistic Hough Transform
    # rho=1, theta=pi/180, threshold=10
    lines = cv2.HoughLinesP(edges, 1, np.pi/180, 10)

    # 3. Find the Median Angle
    median_angle = find_median_angle(lines)

    # 4. Get dimensions and center
    rows, cols = rotate_img.shape[:2]
    center = (cols / 2, rows / 2)

    # 5. Get rotation matrix and Apply WarpAffine
    # Replicating GetRotationMatrix2D and WarpAffine calls
    M = cv2.getRotationMatrix2D(center, median_angle, 1.0)
    rotated = cv2.warpAffine(rotate_img, M, (cols, rows))

    return rotated

def add_weighted(src1: np.ndarray, src2: np.ndarray, alpha: float, beta: float, gamma: float) -> np.ndarray:
    """
    Calculates the weighted sum of two arrays.
    dst = src1*alpha + src2*beta + gamma
    """
    # Replicates gocv.AddWeighted
    return cv2.addWeighted(src1, alpha, src2, beta, gamma)
