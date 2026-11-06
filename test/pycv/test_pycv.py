import unittest
import cv2
import numpy as np

from pycv.effectchain import chain, word_denoise_chain, word_isolation_chain
from pycv.effects import (
    AdaptiveThresholdEffect,
    CannyEffect,
    DilateEffect,
    ErodeEffect,
    GaussianBlurEffect,
    GrayScaleEffect,
    InvertEffect,
    MorphEffect,
    ResizeEffect,
    ThresholdBinaryEffect,
    ThresholdOtsuEffect,
    ThinningEffect,
)
from pycv.helpers import (
    add_weighted,
    deskew,
    deskew_by_canny_hough_lines,
    draw_angle_line,
    draw_hough_lines,
    draw_line,
    extract_cell_bounds,
    find_median_angle,
    full_rect,
    get_horizontal_and_vertical_lines,
    get_rotation_matrix,
    line_angles,
    rand_color,
    rand_color_float64,
    random_rect,
    rb,
    sort_and_filter_rects,
    warp_affine,
)


class TestPyCV(unittest.TestCase):

    def setUp(self):
        self.img = np.zeros((100, 120, 3), dtype=np.uint8)
        cv2.rectangle(self.img, (20, 20), (80, 60), (255, 255, 255), 2)
        cv2.line(self.img, (10, 50), (110, 52), (255, 255, 255), 2)

    def test_all_effects_and_chains(self):
        m = MorphEffect(3, cv2.MORPH_OPEN)
        m.copy(self.img)
        assert m.cpy is not None and m.cpy.shape == self.img.shape
        assert m.apply(self.img).shape == self.img.shape

        assert DilateEffect(3, 3).apply(self.img).shape == self.img.shape
        assert ErodeEffect(3, 3).apply(self.img).shape == self.img.shape
        assert GaussianBlurEffect(3, 3).apply(self.img).shape == self.img.shape

        gray = GrayScaleEffect().apply(self.img)
        assert gray.shape == (100, 120)
        assert GrayScaleEffect().apply(gray).shape == (100, 120)

        inv = InvertEffect().apply(gray)
        assert int(inv[0, 0]) == 255

        assert AdaptiveThresholdEffect().apply(self.img).shape == (100, 120)
        assert ThresholdBinaryEffect(127).apply(self.img).shape == (100, 120)
        assert ThresholdOtsuEffect().apply(self.img).shape == (100, 120)
        assert CannyEffect(50, 150).apply(self.img).shape == (100, 120)

        resized_wh = ResizeEffect(width=60, height=50).apply(self.img)
        assert resized_wh.shape == (50, 60, 3)
        resized_scale = ResizeEffect(fx=0.5, fy=0.5).apply(self.img)
        assert resized_scale.shape == (50, 60, 3)

        thinned = ThinningEffect().apply(self.img)
        assert thinned.shape == (100, 120)

        out1 = chain(self.img.copy(), word_denoise_chain())
        out2 = chain(self.img.copy(), word_isolation_chain())
        assert out1.shape == (100, 120)
        assert out2.shape == (100, 120)

    def test_helpers(self):
        assert full_rect(self.img) == (0, 0, 120, 100)
        rx1, ry1, rx2, ry2 = random_rect(self.img, 200, 200)
        assert rx2 - rx1 == 120 and ry2 - ry1 == 100
        assert 0 <= rb() <= 255
        assert len(rand_color()) == 3
        assert len(rand_color_float64()) == 3

        gray = cv2.cvtColor(self.img, cv2.COLOR_BGR2GRAY)
        grid = get_horizontal_and_vertical_lines(gray, 5)
        assert grid.shape == (100, 120)

        contours, _ = cv2.findContours(grid, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)
        rects = extract_cell_bounds(contours)
        assert len(rects) >= 1
        sorted_r = sort_and_filter_rects([(0, 0, 10, 10), (5, 5, 50, 50)], min_area=200, max_area=5000)
        assert sorted_r == [(5, 5, 50, 50)]

        canvas = self.img.copy()
        draw_line(canvas, (0, 0), (50, 50), (0, 255, 0), 1)
        draw_angle_line(canvas, 0.5, (255, 0, 0), 1)

        lines = np.array([[[10, 50, 110, 52]], [[10, 60, 110, 62]]], dtype=np.int32)
        draw_hough_lines(canvas, lines, (0, 0, 255), 1)

        deg, rad = line_angles(lines)
        assert deg > 0 and rad > 0
        assert line_angles(None) == (0.0, 0.0)

        med = find_median_angle(lines)
        assert 0.0 < med < 5.0
        assert find_median_angle(None) == 0.0

        rot = get_rotation_matrix(self.img, 5.0, 1.0)
        warped = warp_affine(self.img, rot)
        assert warped.shape == self.img.shape

        assert deskew(self.img, rad).shape == self.img.shape
        assert deskew_by_canny_hough_lines(gray, self.img).shape == self.img.shape
        assert add_weighted(gray, gray, 0.5, 0.5, 0.0).shape == gray.shape


if __name__ == "__main__":
    unittest.main()
