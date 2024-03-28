import torch.nn.functional as F
import torch
import math
import matplotlib.pyplot as plt
from shapely.affinity import translate
from shapely.geometry import Point, box
from shapely.ops import unary_union
from shapely.wkt import loads

DEFAULT_CELL_SIZE = 75

device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
canvas_dtype = torch.float16 if torch.cuda.is_available() else torch.float


def geom_to_tensor(geom, cell_size=DEFAULT_CELL_SIZE):
    x0, y0, x1, y1 = geom.bounds
    h = int(math.ceil((y1 - y0) / cell_size))
    w = int(math.ceil((x1 - x0) / cell_size))
    if w % 2 == 0:
        w += 1
    if h % 2 == 0:
        h += 1
    return fill_tensor(translate(geom, -x0, -y0), h, w)


def fill_tensor(geom, height, width, cell_size=DEFAULT_CELL_SIZE):
    t = torch.zeros((height, width), dtype=canvas_dtype, device=device)
    for i in range(height):
        cy0 = i * cell_size
        cy1 = cy0 + cell_size
        for j in range(width):
            cx0 = j * cell_size
            cx1 = cx0 + cell_size
            if geom.intersects(box(cx0, cy0, cx1, cy1)):
                t[i, j] = 1
    return t


def nest_df_to_tensor(nest_df, cell_size=DEFAULT_CELL_SIZE):
    height = nest_df.iloc[0]
    width = int(math.ceil(nest_df.iloc[0].output_bounds_width / cell_size))
    height = int(math.ceil(nest_df.iloc[0].total_nest_height / cell_size))
    return fill_tensor(
        unary_union([loads(w) for w in nest_df.nested_geometry.values]), height, width
    )


def conv(canvas, piece):
    return F.conv2d(
        canvas.unsqueeze(0).unsqueeze(0),
        piece.unsqueeze(0).unsqueeze(0),
        padding="same",
    ).squeeze()


class Piece:
    def __init__(self, id, geom, cell_size=DEFAULT_CELL_SIZE):
        self.id = id
        self.geom = geom
        self.bitmap = geom_to_tensor(geom, cell_size)
        self.area = self.bitmap.sum()
        self.height, self.width = self.bitmap.shape

    @staticmethod
    def from_wkt(id, wkt):
        return Piece(id, loads(wkt))


class Canvas:
    def __init__(
        self,
        max_width,
        nest_height=0,
        canvas=None,
        pieces=None,
        cell_size=DEFAULT_CELL_SIZE,
    ):
        self.cell_size = cell_size
        self.max_width = max_width
        self.width = int(max_width / cell_size)
        if canvas is None:
            self.canvas = torch.zeros(
                (10, self.width), dtype=canvas_dtype, device=device
            )
            self.canvas[0, :] = 1
            self.canvas[:, 0] = 1
            self.canvas[:, -1] = 1
        else:
            self.canvas = canvas
        self.nest_height = nest_height
        self.pieces = pieces or []

    @staticmethod
    def from_nest_df(nest_df):
        return Canvas(
            nest_df.iloc[0].output_bounds_width,
            canvas=nest_df_to_tensor(nest_df),
        )

    def add_piece(self, piece):
        if self.canvas.shape[0] < self.nest_height + piece.height:
            next_canvas = torch.cat(
                (
                    self.canvas,
                    torch.zeros(
                        (piece.height, self.width), dtype=canvas_dtype, device=device
                    ),
                ),
                dim=0,
            )
            next_canvas[:, -1] = 1
            next_canvas[:, 0] = 1
        else:
            next_canvas = self.canvas.clone()
        next = Canvas(
            self.max_width,
            nest_height=self.nest_height,
            canvas=next_canvas,
            cell_size=self.cell_size,
            pieces=list(self.pieces),
        )
        next.add_piece_inplace(piece)
        return next

    def try_add_piece_inplace(self, piece):
        # method which may or may not add the piece depending on the other
        # stuff in the canvas.  This one doesnt check that we can definitely
        # add the piece no matter what.
        feasible = conv(self.canvas, piece.bitmap)
        feasible[0 : (piece.height // 2), :] = 1
        feasible[
            self.canvas.shape[0] - (piece.height // 2) : self.canvas.shape[0], :
        ] = 1
        feasible[:, 0 : (piece.width // 2)] = 1
        feasible[
            :, self.canvas.shape[1] - (piece.width // 2) : self.canvas.shape[1]
        ] = 1
        if feasible.min() >= 1:
            return False
        i = feasible.argmin()
        y = (i // self.width) - piece.height // 2
        x = (i % self.width) - piece.width // 2
        self.canvas[
            y : (y + piece.height),
            x : (x + piece.width),
        ] += piece.bitmap
        self.pieces.append((piece, x, y))
        return True

    def add_piece_inplace(self, piece):
        assert self.canvas.shape[0] >= self.nest_height + piece.height
        y0 = max(0, self.nest_height - 300)
        y1 = min(self.canvas.shape[0], self.nest_height + piece.height)
        feasible = conv(self.canvas[y0:y1, :], piece.bitmap)
        feasible[0 : (piece.height // 2), :] = 1
        feasible += 0.02 * torch.arange(feasible.shape[1], device=device) / feasible.shape[1]
        feasible += 0.10 * torch.arange(feasible.shape[0], device=device).reshape(-1, 1) / feasible.shape[0]
        i = feasible.argmin()
        y = y0 + (i // self.width) - piece.height // 2
        x = (i % self.width) - piece.width // 2
        self.canvas[
            y : (y + piece.height),
            x : (x + piece.width),
        ] += piece.bitmap
        self.nest_height = max(self.nest_height, y + piece.height)
        self.pieces.append((piece, x, y))

    def plot(self, filename):
        nest_height = int(self.nest_height.cpu()) * self.cell_size
        plt.figure(figsize=(2, 2 * nest_height / self.max_width))
        plt.ylim(0, nest_height)
        plt.xlim(0, self.max_width)
        for piece, x, y in self.pieces:
            x0, y0, _, _ = piece.geom.bounds
            nested = translate(
                piece.geom,
                x * self.cell_size - x0,
                y * self.cell_size - y0,
            )
            plt.fill(*nested.boundary.xy, color=(0.6, 0.9, 0.6))
            plt.plot(*nested.boundary.xy, color=(0.3, 0.45, 0.3))
        plt.savefig(filename, bbox_inches="tight")

    def height_px(self):
        return self.nest_height * self.cell_size


if __name__ == "__main__":
    import res
    import random
    res.utils.logger.info(device)
    df = res.connectors.load("s3").read(
        "s3://res-data-production/flows/v1/make-nest-progressive-construct_rolls/primary/nest/construct-rolls-adhoc-1705426194/CTW70-1705442620/CTW70-1705442620_R46460-CTW70_6_self.feather"
    )
    max_width = df.output_bounds_width[0]
    pieces = [Piece.from_wkt(i, w) for i, w in enumerate(df.nestable_wkt)]
    random.shuffle(pieces)
    n = len(pieces)
    res.utils.logger.info("loaded")
    trials = 100
    dt = torch.zeros(trials)
    canvas = Canvas(max_width)
    cache = [canvas]
    for i, piece in enumerate(pieces):
        canvas = canvas.add_piece(piece)
        cache.append(canvas)
    best = canvas
    for trial in range(30000):
        if trial % 100 == 0:
            res.utils.logger.info(f"{trial}: {best.height_px():.1f} ({cache[n].height_px():.1f})")
        i, j = random.sample(range(n), k=2)
        pieces[i], pieces[j] = pieces[j], pieces[i]
        m = min(i, j)
        cache_new = list(cache)
        for k in range(m, n):
            cache_new[k + 1] = cache_new[k].add_piece(pieces[k])
        delta = cache[n].height_px() - cache_new[n].height_px()
        if delta > 0 or random.random() < 1 / (1 + math.exp(min(-delta * 0.002 * (trial / 3000.0), 100))):
            cache = cache_new
            if cache_new[n].nest_height < best.nest_height:
                best = cache_new[n]
        else:
            pieces[i], pieces[j] = pieces[j], pieces[i]

    plt.imsave("nest.png", best.canvas[0 : best.nest_height, :].flip(0).cpu())
    res.connectors.load("s3").upload(
        "nest.png", "s3://res-data-development/experimental/nest_rasterized/nest.png"
    )
    best.plot("nest_fig.png")
    res.connectors.load("s3").upload(
        "nest_fig.png",
        "s3://res-data-development/experimental/nest_rasterized/nest_fig.png",
    )
