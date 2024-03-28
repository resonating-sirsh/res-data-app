import res

import torch
import torch.nn as nn
import numpy as np
import pandas as pd
import os
import math
from shapely.affinity import translate, scale
from shapely.geometry import Point, LineString
from shapely.wkt import loads as shapely_loads
from .packing import Packing, pack_boxes_annealed_inner
from .packing_geometry import interpolate, choose_geom
from res.utils import logger

s3 = res.connectors.load("s3")

model_dict = {}

torch.set_num_threads(1)

LATEST_MODEL_PATH = "s3://res-data-development/experimental/nesting_nn/model_10l_xval_gpu_big/model_iter_18100.dat"

BATCH_SZ = 10

X_LOSS_VALUES = [-0.2, -0.1, -0.05, 0.0, 0.05, 0.1, 0.2]

device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")


def poly_to_vec(p, max_width, dim=128):
    theta = 2 * np.pi / dim
    q = translate(p, *-np.array(p.centroid.xy).flatten())
    q = scale(q, 1.0 / max_width, 1.0 / max_width, origin=(0, 0)).convex_hull
    return np.array(
        [
            np.linalg.norm(
                np.array(
                    q.boundary.intersection(
                        LineString(
                            [
                                Point(0, 0),
                                Point(np.sin(i * theta) * 100, np.cos(i * theta) * 100),
                            ]
                        )
                    ).xy
                ).flatten()
            )
            for i in range(dim)
        ]
    )


def frontier_to_vec(packing, dim=128):
    delta = packing.max_width / float(dim + 2)
    j = 0
    v = []
    frontier = packing.frontier
    for i in range(dim):
        x = delta * (i + 1)
        while x > frontier[j][2]:
            j += 1
        v.append(interpolate(frontier[j], x))
    v = np.array(v) / packing.max_width
    v = v - v.mean()
    v = np.clip(v, -1, 1)
    return v


def anneal_packing(boxes, packing, iters=5000):
    order = list(reversed(list(idx for idx, _, _ in packing.boxes())))
    anneal_order = pack_boxes_annealed_inner(
        0,
        boxes,
        packing.max_width,
        None,
        None,
        None,
        iters,
        initial_order=order,
        log_progress=False,
    )
    return anneal_order


def _box_vecs(boxes, max_width):
    return torch.from_numpy(
        np.stack([poly_to_vec(b.box, max_width) for b in boxes])
    ).float()


class Packer(nn.Module):
    def __init__(
        self,
        input_dim=128,
        hidden_dim=2048,
        int_dim=128,
        transform_layers=5,
        transform_dim=128,
        frontier_bias=False,
    ):
        super().__init__()
        self.frontier_embed = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.Tanh(),
            nn.Linear(hidden_dim, hidden_dim),
            nn.Tanh(),
            nn.Linear(
                hidden_dim,
                int_dim,
                bias=frontier_bias,
            ),
        )
        if transform_layers > 0:
            self.box_embed = nn.Sequential(
                nn.Linear(input_dim, hidden_dim),
                nn.Tanh(),
                nn.Linear(hidden_dim, transform_dim),
                nn.TransformerEncoder(
                    nn.TransformerEncoderLayer(d_model=transform_dim, nhead=8),
                    num_layers=transform_layers,
                ),
                nn.Linear(transform_dim, int_dim),
                nn.Tanh(),
            )
        else:
            self.box_embed = nn.Sequential(
                nn.Linear(input_dim, hidden_dim),
                nn.Tanh(),
                nn.Linear(hidden_dim, hidden_dim),
                nn.Tanh(),
                nn.Linear(hidden_dim, int_dim),
                nn.Tanh(),
            )
        self.box_frontier_angle = nn.Sequential(
            nn.Linear(2 * int_dim, hidden_dim),
            nn.Tanh(),
            nn.Linear(hidden_dim, len(X_LOSS_VALUES)),
        )
        self.logsoftmax = nn.LogSoftmax(dim=1)

    def forward_sample(
        self,
        boxes,
        box_embedding,
        max_width,
        nsamples,
        force=None,
    ):
        n = len(boxes)
        root = Packing(max_width=max_width)
        p = [root for _ in range(nsamples)]
        log_prob = torch.zeros(nsamples, dtype=torch.float, device=device)
        mask = torch.zeros((nsamples, n), dtype=torch.bool, device=device)

        for i in range(n):
            frontier_vec = torch.tensor(
                np.stack([frontier_to_vec(pi) for pi in p]), dtype=torch.float
            ).to(device)
            frontier_embedding = self.frontier_embed(frontier_vec)  # .squeeze()
            logits = torch.matmul(
                frontier_embedding, torch.transpose(box_embedding, 0, 1)
            ).masked_fill(mask, -np.inf)
            log_probs = self.logsoftmax(logits)
            chosen = torch.multinomial(log_probs.exp(), 1).squeeze()
            if force:
                chosen[-1] = force[i][0]
            x_loss_log_probs = self.logsoftmax(
                self.box_frontier_angle(
                    torch.concat([frontier_embedding, box_embedding[chosen, :]], 1)
                )
            )
            x_loss_val_idx = torch.multinomial(x_loss_log_probs.exp(), 1).squeeze()
            if force:
                x_loss_val_idx[-1] = X_LOSS_VALUES.index(force[i][1])
            for j in range(nsamples):
                p[j] = p[j].add(
                    boxes[int(chosen[j])],
                    int(chosen[j]),
                    x_loss_val=X_LOSS_VALUES[x_loss_val_idx[j]],
                )
            mask = mask.clone()
            mask[torch.arange(nsamples), chosen] = True
            log_prob += (
                log_probs[torch.arange(nsamples), chosen]
                + x_loss_log_probs[torch.arange(nsamples), x_loss_val_idx]
            )
        return [(log_prob[i], p[i]) for i in range(nsamples)]

    def get_loss(self, boxes, box_vecs, max_width, nsamples=100, force=None):
        box_embed = self.box_embed(box_vecs.to(device))
        force_order = (
            None if force is None else list(reversed(list(force.indices_and_x_loss())))
        )
        res = self.forward_sample(
            boxes, box_embed, max_width, nsamples=nsamples, force=force_order
        )
        best_packing = min(res, key=lambda t: t[1].height)[1]
        heights = [p.height for _, p in res]
        mean_height = np.mean(heights)
        sd_height = np.std(heights)
        loss = (
            sum(lp * (p.height - mean_height) / (sd_height + 100) for lp, p in res)
            / nsamples
        )
        return loss, mean_height, sd_height, best_packing

    def approx_best(self, boxes, box_vecs, max_width, nsamples=1000):
        box_embed = self.box_embed(box_vecs.to(device))
        with torch.no_grad():
            samples = self.forward_sample(boxes, box_embed, max_width, nsamples)
            # just to be sure that the nn didnt somehow omit a box.
            samples = [(l, p) for l, p in samples if len(p.all_box_ids()) == len(boxes)]
            return min(samples, key=lambda t: t[1].height)[1]

    def nest_boxes(self, boxes, max_width):
        box_vecs = _box_vecs(boxes, max_width)
        return self.approx_best(
            boxes,
            box_vecs,
            max_width,
        )

    def nest_boxes_legacy(self, geoms, max_width):
        return self.nest_boxes(
            [choose_geom(g) for g in geoms], max_width
        ).to_packingshapes()

    def active_search(self, boxes, max_width, iters=10):
        optimizer = torch.optim.AdamW(self.parameters(), lr=1e-4)
        self.train()
        box_vecs = _box_vecs(boxes, max_width)
        overall_best = None
        for i in range(iters):
            self.zero_grad()
            loss, _, _, best = self.get_loss(boxes, box_vecs, max_width, nsamples=50)
            loss.backward()
            res.utils.logger.info(f"active search iteration {i}: height {best.height}")
            optimizer.step()
            if overall_best is None or best.height < overall_best.height:
                overall_best = best
        return overall_best


def load_latest_model():
    return load_model(LATEST_MODEL_PATH)


def load_model(
    s3_state_dict_path="s3://res-data-development/experimental/nesting_nn/ecvic_transformer_state_dict.dat",
):
    global model_dict
    if model_dict is None:
        model_dict = {}
    if s3_state_dict_path not in model_dict:
        s3._download(s3_state_dict_path, target_path="/tmp", filename="state_dict.dat")
        loaded_data = torch.load(
            "/tmp/state_dict.dat",
            map_location=None if torch.cuda.is_available() else torch.device("cpu"),
        )
        if "state_dict" in loaded_data:
            model = Packer(**loaded_data.get("model_kwargs", {}))
            model.load_state_dict(loaded_data["state_dict"])
        else:
            model = Packer()
            model.load_state_dict(loaded_data)
        model_dict[s3_state_dict_path] = model.to(device)
    return model_dict[s3_state_dict_path]


def load_training_data(
    cache_path="s3://res-data-development/experimental/nesting_nn/self_2023_07_14_1000.pickle",
    piece_type="self",
    date_start="2023-01-01",
    date_end="2023-07-14",
    max_datapoints=1000,
):
    if s3.exists(cache_path):
        logger.info(f"Loading data from {cache_path}")
        return s3.read(cache_path)
    from pyairtable import Table
    from tqdm import tqdm

    logger.info(f"Generating training data from {date_start} to {date_end}")
    nest_records = Table(
        res.utils.secrets_client.get_secret("AIRTABLE_API_KEY"),
        "apprcULXTWu33KFsh",
        "tbl7n4mKIXpjMnJ3i",
    ).all(
        fields=["Argo Jobkey", "Dataframe Path"],
        formula=f"""
        AND(
            {{Created At}} >= '{date_start}',
            {{Created At}} <= '{date_end}',
            FIND('_{piece_type}', {{Argo Jobkey}}),
            {{Rolls}}!='',
            {{Nested Pieces}} > 20,
            {{Nested Pieces}} < 85
        )
        """,
    )
    tqdm.pandas()
    res.utils.logger.info(f"Found {len(nest_records)} nests for train / test")
    data_df = pd.DataFrame.from_records([r["fields"] for r in nest_records])
    if data_df.shape[0] > max_datapoints:
        data_df = data_df.sample(max_datapoints, replace=False).reset_index(drop=True)
    res.utils.logger.info("Loading nests")
    data_df["nest_df"] = data_df["Dataframe Path"].progress_apply(lambda p: s3.read(p))
    data_df = data_df[
        data_df.nest_df.apply(lambda d: "nestable_wkt" in d.columns)
    ].reset_index(drop=True)
    data_df["height"] = data_df["nest_df"].apply(lambda d: d.max_nested_y.max())
    data_df["max_width"] = data_df["nest_df"].apply(lambda d: d.output_bounds_width[0])
    res.utils.logger.info("Loading geometry")
    data_df["boxes"] = data_df["nest_df"].progress_apply(
        lambda d: d.nestable_wkt.apply(shapely_loads).apply(choose_geom).values
    )
    res.utils.logger.info("Embedding geometry")
    data_df["box_vecs"] = data_df.progress_apply(
        lambda r: _box_vecs(r.boxes, r.max_width), axis=1
    )
    data_df = data_df.drop(columns=["nest_df"])
    s3.write(cache_path, data_df)
    return data_df


def train_model(
    model_dir="s3://res-data-development/experimental/nesting_nn/model_new2",
    first_iter=None,
    max_iter=2500,
    **model_kwargs,
):
    data = (
        load_training_data()
    )  # cache_path="s3://res-data-development/experimental/nesting_nn/self_test_july.pickle")
    train = data.iloc[10:]
    test = data.iloc[:10]
    res.utils.logger.info(f"Train: {train.shape[0]} Test: {test.shape[0]}")
    model = Packer(**model_kwargs).to(device)
    optimizer = torch.optim.AdamW(model.parameters(), lr=1e-4)
    scheduler = torch.optim.lr_scheduler.StepLR(optimizer, step_size=100, gamma=0.99)
    if first_iter is not None:
        res.utils.logger.info(f"Loading model from iter {first_iter}")
        s3._download(
            f"{model_dir}/model_iter_{first_iter}.dat",
            target_path="/tmp",
            filename="state_dict.dat",
        )
        loaded_data = torch.load("/tmp/state_dict.dat")
        model.load_state_dict(loaded_data["state_dict"])
        optimizer.load_state_dict(loaded_data["optimizer"])
        scheduler.load_state_dict(loaded_data["scheduler"])
    tmp_file_name = f"model_{os.getpid()}.dat"
    best_solutions = {}
    for iter in range(first_iter if first_iter is not None else 0, max_iter):
        if iter % 10 == 0:
            res.utils.logger.info(iter)
        model.train()
        model.zero_grad()
        batch = train.sample(BATCH_SZ)
        batch_loss = None
        for i, r in batch.iterrows():
            loss, _, _, best_packing = model.get_loss(
                r.boxes,
                r.box_vecs,
                r.max_width,
                nsamples=100,
                force=best_solutions.get(i),
            )
            best_solutions[i] = best_packing
            batch_loss = loss if batch_loss is None else batch_loss + loss
        (batch_loss / BATCH_SZ).backward()
        optimizer.step()
        scheduler.step()
        if iter % 100 == 0 and iter > 0:
            model.eval()
            test_heights = test.apply(
                lambda r: model.approx_best(r.boxes, r.box_vecs, r.max_width).height,
                axis=1,
            )
            losses = (test_heights - test.height) / test.height
            res.utils.logger.info(
                f"iter: {iter}, {losses.mean()} -- {losses.values} [{len(best_solutions)}]"
            )
            res.utils.logger.info(f"Checkpointing state {iter} to {tmp_file_name}")
            torch.save(
                {
                    "iter": iter,
                    "state_dict": model.state_dict(),
                    "optimizer": optimizer.state_dict(),
                    "scheduler": scheduler.state_dict(),
                    "model_kwargs": model_kwargs,
                },
                tmp_file_name,
            )
            s3.upload(tmp_file_name, f"{model_dir}/model_iter_{iter}.dat")
            if iter % 1000 == 0:
                # throw away the best solutions in case they are now low probability.
                # could do something smarter possibly.
                best_solutions = {}
    return model


if __name__ == "__main__":
    train_model(
        model_dir="s3://res-data-development/experimental/nesting_nn/model_10l_xval_gpu_big",
        transform_layers=10,
        max_iter=40000,
        first_iter=None,
        int_dim=128,
    )
