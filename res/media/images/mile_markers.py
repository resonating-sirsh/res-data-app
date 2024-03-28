from re import A
from typing import Tuple
from pyvips import Image as PVImage
from pyvips.enums import Align, Direction
from math import ceil, floor
from operator import attrgetter
from itertools import accumulate
from enum import Enum

DPI_SETTING = 300
PIXEL_PER_MILLIMETER_SETTING = DPI_SETTING / 25.4
MILLIMETER_PER_PIXEL = 1 / PIXEL_PER_MILLIMETER_SETTING
MARKER_STRIP_WIDTH = 300
LABEL_HORIZ_MARGIN = 50
LABEL_VERT_MARGIN = 6
LABEL_MAX_WIDTH = MARKER_STRIP_WIDTH - (2 * LABEL_HORIZ_MARGIN)
LABEL_MAX_HEIGHT = 36
LABEL_FONT = "DejaVuSans"
BASE_MARK_LENGTH = 75
BASE_MARK_STROKE = 6
EXP_BREAKPOINT = 1000000


def new_marker_strip(height):
    return PVImage.black(MARKER_STRIP_WIDTH, height, bands=4)


class MarkAlign(Enum):
    LEFT = "left"
    RIGHT = "right"


class Mark:
    px: int
    add_label: bool = False
    length: int = BASE_MARK_LENGTH
    stroke: int = BASE_MARK_STROKE
    ink: Tuple[int] = (0, 0, 0, 255)

    def __init__(self, px):
        self.px = px
        self.add_label = self.__class__.add_label
        self.length = self.__class__.length
        self.stroke = self.__class__.stroke
        self.ink = self.__class__.ink

    @classmethod
    def mark_class_for_pixel(cls, px):
        for mc in cls.MARK_CLASSES:
            if mc.select(px):
                # return first matching mc
                return mc(px)
        # default to base mc
        return cls(px)

    @staticmethod
    def draw_label_below_mark(img, label_image, mark_bottom):
        label_top = mark_bottom + LABEL_VERT_MARGIN
        return img.draw_image(
            label_image,
            LABEL_HORIZ_MARGIN,
            label_top,
            mode="set",
        )

    @staticmethod
    def draw_label_above_mark(img, label_image, mark_top):
        """So we don't have to carry labels over between strip segments"""
        label_top = mark_top - (LABEL_MAX_HEIGHT + LABEL_VERT_MARGIN)
        return img.draw_image(
            label_image,
            LABEL_HORIZ_MARGIN,
            label_top,
            mode="set",
        )

    def draw_label(self, img, mark_top, mark_bottom):
        label_image = PVImage.text(
            self.label_text,
            font=LABEL_FONT,
            width=LABEL_MAX_WIDTH,
            height=LABEL_MAX_HEIGHT,
            align=Align.HIGH,
            rgba=True,
            dpi=DPI_SETTING,
            justify=False,
        )
        if label_image.height + mark_bottom + LABEL_VERT_MARGIN > img.height:
            return self.draw_label_above_mark(img, label_image, mark_top)
        else:
            return self.draw_label_below_mark(img, label_image, mark_bottom)

    def draw_mark_at_pixel(self, img, top, align=MarkAlign.LEFT):
        mark_left = img.width - self.length if align == MarkAlign.RIGHT else 0
        mark_top = self.px - top
        mark_bottom = mark_top + self.stroke

        if mark_top > img.height:  # let the next section handle mark + label
            return img, None

        # as long as any amount of mark is printed on this strip, handle the
        # label here. If there is room on strip, it will get printed below,
        # otherwise it will get printed above. This avoids bloating the work
        # that needs to spill over to the next strip, and avoids dropping labels
        # at the end of the roll.

        if self.add_label:
            img = self.draw_label(img, mark_top, mark_bottom)

        if mark_bottom > img.height and mark_top <= img.height:
            # mark doesn't entirely fit on strip, spill over partial stroke
            fitting_part = img.height - mark_top - 1
            img = img.draw_rect(
                self.ink, mark_left, mark_top, self.length, fitting_part, fill=True
            )
            # ensure spill over is returned. we know that mark will be drawn @ 0
            return img, lambda _i: _i.draw_rect(
                self.ink,
                mark_left,
                0,
                self.length,
                self.stroke - fitting_part,
                fill=True,
            )

        # mark wholly fits on the strip, no carryover needed
        img = img.draw_rect(
            self.ink, mark_left, mark_top, self.length, self.stroke, fill=True
        )
        return img, None

    @classmethod
    def draw_markers_for_range(cls, top, height, align=MarkAlign.LEFT, img=None):
        img = img if img is not None else new_marker_strip(height)
        first_mark_top = ceil(top / cls.SPACING) * cls.SPACING
        spill_over_next_segment = []
        for px in range(first_mark_top, top + height, cls.SPACING):
            mc = cls.mark_class_for_pixel(px)
            img, spill_over = mc.draw_mark_at_pixel(img, top, align=align)
            if spill_over is not None:
                spill_over_next_segment.append(spill_over)
        return img, spill_over_next_segment

    @classmethod
    def mark_images(cls, imgs, align=MarkAlign.LEFT, draw_on_image=False):
        heights = map(attrgetter("height"), imgs)
        with_cum_heights = zip(accumulate(heights, initial=0), imgs)
        cum_height = 0
        for img in imgs:
            top = cum_height
            cum_height += img.height
            if draw_on_image:
                yield cls.draw_markers_for_range(top, img.height, align=align, img=img)
            elif align == MarkAlign.LEFT:
                yield cls.draw_markers_for_range(top, img.height, align=align).join(
                    img, Direction.HORIZONTAL, expand=True
                )
            else:
                yield img.join(
                    cls.draw_markers_for_range(top, img.height, align=align),
                    Direction.HORIZONTAL,
                    expand=True,
                )


class PixelMark(Mark):
    SPACING = 100
    MARK_CLASSES = []

    def __init_subclass__(cls, base=False, **kwargs):
        if hasattr(cls, "select"):  # only register Marks with select()
            PixelMark.MARK_CLASSES.append(cls)

    @property
    def label_text(self):
        return f"{self.px:,.0f}px" if self.px < EXP_BREAKPOINT else f"{self.px:.4e}px"


class MegaPixelMark(PixelMark):
    length = 300
    stroke = 12
    add_label = True

    @staticmethod
    def select(px):
        return px % 1000000 == 0


class CentiKiloPixelMark(PixelMark):
    length = 225
    stroke = 9
    add_label = True

    @staticmethod
    def select(px):
        return px % 100000 == 0


class KiloPixelMark(PixelMark):
    length = 150
    add_label = True

    @staticmethod
    def select(px):
        return px % 1000 == 0


class ImperialMark(Mark):
    SPACING = 150  # every 0.5"
    MARK_CLASSES = []

    def __init_subclass__(cls, **kwargs):
        if hasattr(cls, "select"):  # only register Marks with select()
            ImperialMark.MARK_CLASSES.append(cls)

    @property
    def label_text(self):
        dist = self.px2dist(self.px)
        yards = f"{floor(dist / 36)}yd"
        feet = f"{floor(dist / 12) % 3}′"
        inches = f"{dist % 12:.0f}″"
        label_text = f"{yards}{feet}{inches}"
        return label_text

    @staticmethod
    def px2dist(px):
        return px / DPI_SETTING


class YardMark(ImperialMark):
    add_label = True
    length = 300
    stroke = 12
    ink = (255, 0, 0, 255)  # Red

    @classmethod
    def select(cls, px):
        return cls.px2dist(px) % 36 == 0


class YardMark1(YardMark):
    ink = (0, 255, 0, 255)  # Green

    @classmethod
    def select(cls, px):
        return cls.px2dist(px) % 36 == 12


class YardMark2(YardMark):
    ink = (0, 0, 255, 255)  # Blue

    @classmethod
    def select(cls, px):
        return cls.px2dist(px) % 36 == 24


class HalfFootMark(ImperialMark):
    add_label = True
    length = 225
    stroke = 9

    @classmethod
    def select(cls, px):
        return cls.px2dist(px) % 6 == 0


class TwoInchMark(ImperialMark):
    length = 150
    add_label = True

    @staticmethod
    def select(px):
        return px % 600 == 0


class InchMark(ImperialMark):
    length = 150

    @staticmethod
    def select(px):
        return px % 300 == 0


## This won't print cleanly given our resolution in DPI...
## We can either fudge it and approximate metric positions of the marks to the
## nearest pixel, i.e. 10mm = 118.11px, or we can make marks at regular
## inch/pixel intervals and have weird-looking metric measurements, e.g.
## 0.5″ = 150px = 12.7mm


class MetricMark(Mark):
    SPACING = PIXEL_PER_MILLIMETER_SETTING * 10
    MARK_CLASSES = []

    def __init_subclass__(cls, base=False, **kwargs):
        if hasattr(cls, "select"):  # only register Marks with select()
            MetricMark.MARK_CLASSES.append(cls)

    @property
    def label_text(self):
        dist = self.px2dist(self.px)
        meters = dist / 1000
        label_text = f"{meters:.3e}m"
        return label_text

    @staticmethod
    def px2dist(px):
        return px / PIXEL_PER_MILLIMETER_SETTING


class MeterMark(MetricMark):
    length = 300
    stroke = 12
    add_label = True

    @classmethod
    def select(cls, px):
        return cls.px2dist(px) % 1000 == 0


class QuarterMeterMark(MetricMark):
    length = 225
    stroke = 9
    ink = (255, 255, 0, 255)  # Yellow

    @classmethod
    def select(cls, px):
        return cls.px2dist(px) % 1000 == 250


class HalfMeterMark2(QuarterMeterMark):
    ink = (255, 0, 255, 255)  # Magenta

    @classmethod
    def select(cls, px):
        return cls.px2dist(px) % 1000 == 500


class QuarterMeterMark2(QuarterMeterMark):
    ink = (0, 255, 255, 255)  # Cyan

    @classmethod
    def select(cls, px):
        return cls.px2dist(px) % 1000 == 750


class DecimeterMark(MetricMark):
    length = 150
    stroke = 9
    add_label = True

    @classmethod
    def select(cls, px):
        return cls.px2dist(px) % 100 == 0


class CentimeterMark(MetricMark):
    @classmethod
    def select(cls, px):
        return cls.px2dist(px) % 10 == 0
