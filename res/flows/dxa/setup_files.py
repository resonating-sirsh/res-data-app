import res

import pandas as pd
from res.flows.FlowContext import FlowContext
from res.media.images.providers.dxf import DxfFile
from tqdm import tqdm
from datetime import timedelta
from tenacity import retry, wait_fixed, stop_after_attempt


class net_dxf_location_cache:
    """
    for now put the files in dynamo so we can quickly look them up for body,version,size


        c = net_dxf_location_cache()

        c.put('TT-0000', 3, '2ZZSM', {'a':'b', 'c':'d'})
        c.get('TT-0000', 3, '2ZZSM')

    """

    def __init__(self):
        self._db = res.connectors.load("dynamo")["production"]["body_net_dxfs"]

    def load(
        self,
        body_filter=None,
        body_version_filter=None,
        watermark_days_since_added=None,
    ):
        def try_version(x):
            try:
                return int(float(x.split("-")[2].replace("V", "")))
            except:
                return None

        df = self._db.read()
        df = df[df["key"].map(lambda x: len(x.split("-")) == 4)]
        df["body_code"] = df["key"].map(lambda x: x[:7])
        df["body_version"] = df["key"].map(try_version)
        df = df[df["body_version"].notnull()]
        df["body_version"] = df["body_version"].map(int)
        df["size_code"] = df["key"].map(lambda x: x.split("-")[-1])
        if body_filter:
            if isinstance(body_filter, str):
                body_filter = [body_filter]
            df = df[df["body_code"].isin(body_filter)]
        if body_version_filter:
            df = df[df["body_version"] == body_version_filter]
        if watermark_days_since_added:
            df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True)
            df = df[
                df["updated_at"]
                > res.utils.dates.utc_days_ago(watermark_days_since_added)
            ]
        return df.reset_index().drop("index", 1)

    def validate(self, chk, recheck_invalid=False, recheck_all=False):

        validator = piece_validator_factory(True)

        def get_piece_names(x):
            names = []
            try:
                for k, v in x.items():
                    _, nf = DxfFile.make_setup_file_svg(v, return_shapes=True)
                    names += list(nf["key"])
                return names
            except Exception as ex:
                return None

        def duplicates(x):
            if isinstance(x, list):
                return len(list(x)) - len(list(set(x)))

        def num_bad(x):
            if isinstance(x, list):
                return len(x)

        def bad_piece_names(x):
            names = []
            try:
                for k, v in x.items():
                    _, nf = DxfFile.make_setup_file_svg(v, return_shapes=True)
                    names += list(nf["key"])
                return names
            except:
                return None

        # assume not for the input data
        if "is_valid" not in chk.columns:
            chk["is_valid"] = False
        # the slow track is to retry everything i.e. not trusting older tests
        if recheck_all:
            todo = chk
        # this faster track is to only theck things that are not yet validated
        elif recheck_invalid:
            todo = chk[chk["is_valid"] != True].reset_index()
        # quickest: if we ever looked at it, ignore
        else:
            todo = chk[chk["is_valid"].notnull()].reset_index()
        # chk = chk[:1]  # for testing
        res.utils.logger.info(f"Validating {len(todo)} records. Please wait....")
        todo["names"] = todo["files"].map(get_piece_names)
        todo["invalid_piece_names"] = todo["names"].map(validator)
        todo["num_duplicates"] = todo["names"].map(duplicates)
        todo["num_bad"] = todo["invalid_piece_names"].map(num_bad)
        todo["kosher"] = False
        todo.loc[
            (todo["num_bad"] == 0) & (todo["num_duplicates"] == 0), "kosher"
        ] = True
        for record in todo.to_dict("records"):
            try:
                self.set_validity(
                    record["body_code"],
                    record.get("body_version", record.get("version")),
                    record["size_code"],
                    record["kosher"],
                )
            except Exception as ex:
                res.utils.logger.debug(f"Validation failed {ex}")

        return todo

    def _make_key(self, body_code, body_version, size):
        try:
            if not isinstance(body_version, int):
                body_version = int(float(str(body_version)))
        except:
            pass
        v = str(body_version).lower().replace("v", "")
        key = f"{body_code}-V{v}-{size}".upper()
        # res.utils.logger.debug(f"body key is {key}")
        return key

    def get(self, body_code, body_version, size):
        data = self._db.get(self._make_key(body_code, body_version, size))
        if data is not None and len(data):
            return dict(data.iloc[0])["files"]

    def get_full_record(self, body_code, body_version, size):
        data = self._db.get(self._make_key(body_code, body_version, size))
        if data is not None and len(data):
            return dict(data.iloc[0])

    def put_rank_pair(
        self, body_code, body_version, size, body_asset, body_asset_combo=None
    ):

        for c in [body_code, body_version, size]:
            if pd.isnull(c):
                res.utils.log.warn(
                    f"Part of the BVS key is null {body_code} {body_version} {size} - skipping"
                )
                return

        files_map = {"self": "body_asset"}
        if body_asset_combo:
            files_map["combo"] = body_asset_combo

        return self._db.put(
            {
                "key": self._make_key(body_code, body_version, size),
                "files": files_map,
                "updated_at": res.utils.dates.utc_now_iso_string(),
            }
        )

    def put(self, body_code, body_version, size, files_map):
        return self._db.put(
            {
                "key": self._make_key(body_code, body_version, size),
                "files": files_map,
                "updated_at": res.utils.dates.utc_now_iso_string(),
            }
        )

    def set_validity(self, body_code, body_version, size, is_valid):
        key = self._make_key(body_code, body_version, size)
        record = self._db.get(key)

        # its weird that this one be none?
        if record is not None:
            record = dict(record.iloc[0])
            record["is_valid"] = is_valid
            record["validated_at"] = res.utils.dates.utc_now_iso_string()
            return self._db.put(record)
        else:
            raise Exception(f"Failed to set valid status on key {key}")
        return None

    def validate_cached_bodies(self, todo):
        res.utils.logger.info("validating....")
        validator = piece_validator_factory(True)

        def get_piece_names(x):
            names = []
            try:
                for k, v in x.items():
                    _, nf = DxfFile.make_setup_file_svg(v, return_shapes=True)
                    names += list(nf["key"])
                return names
            except Exception as ex:
                return None

        def duplicates(x):
            if isinstance(x, list):
                return len(list(x)) - len(list(set(x)))

        def num_bad(x):
            if isinstance(x, list):
                return len(x)

        todo = todo.reset_index().drop("index", 1)
        todo["size_code"] = todo["key"].map(lambda x: x.split("-")[-1].strip())
        todo["body_version"] = (
            todo["key"].map(lambda x: x.split("-")[2].replace("V", "")).map(int)
        )
        todo["names"] = todo["files"].map(get_piece_names)
        todo["invalid_piece_names"] = todo["names"].map(validator)
        todo["num_duplicates"] = todo["names"].map(duplicates)
        todo["num_bad"] = todo["invalid_piece_names"].map(num_bad)
        todo["kosher"] = False
        todo.loc[
            (todo["num_bad"] == 0) & (todo["num_duplicates"] == 0), "kosher"
        ] = True
        for record in todo.to_dict("records"):
            try:
                self.set_validity(
                    record["body_code"],
                    record["body_version"],
                    record["size_code"],
                    record["kosher"],
                )
            except Exception as ex:
                res.utils.logger(f"Failed on {record} - {ex}")
        return todo


def make_setup_files_from_net_files(data, show_progress=False):
    """
    save setup files to s3://meta-one-assets-prod/bodies/setup_files
    logic for making setup file is contained in the DXF logic
    we need to do some fuzzy things like remove extra boundaries on shapes
    """

    s3 = res.connectors.load("s3")

    problems = {}
    iterator = tqdm(data["s3_path"]) if show_progress else data["s3_path"]
    for f in iterator:

        try:
            g = f.replace("net_files", "setup_files")
            g = g.replace(".dxf", ".svg")
            svg, data = DxfFile.make_setup_file_svg(f, return_shapes=True)
            res.utils.logger.info(f"saving {g}")
            s3.write(g, svg)
            g = g.replace(".svg", ".feather")
            res.utils.logger.info(f"saving {g}")
            s3.write(g, data)
        except Exception as ex:
            problems[f] = repr(ex)

    return problems


def s3_file_name(f):
    """
    s3://meta-one-assets-prod/bodies/net_files/[body_code]/v[body_version]/[filename].dxf
    """
    name = f.lower().replace("-", "_")

    body_code = "-".join(f.split("-")[:2]).lower().replace("-", "_")
    version = f.split("-")[2].lower()

    path = f"s3://meta-one-assets-prod/bodies/net_files/{body_code}/{version}/{name}"

    return path


def get_net_files(
    brands=[],
    bodies=None,
    fid="41568396914",
    fetch_new=True,
    verbose=False,
    save_to=None,
    copy_always=False,
    pull_modified_dates=True,
    folders_modified_after=None,
):
    """
    get net files used to make setup files

    is there a way to only check folders that have changed in Box?

    copy_always: currently not using time stamps so we can force writing a newer/any file that what we have in s3
    """

    s3 = res.connectors.load("s3")

    box = res.connectors.load("box")
    c = box._get_client()
    folder = c.folder(folder_id=fid).get()

    def append_bodies(setup, data, brand):

        for body in setup.get_items():
            modified_at = body.get().modified_at
            modified_at = res.utils.dates.parse(modified_at)
            if modified_at < folders_modified_after:
                continue
            # looking for NET files
            if not "-NET" in body.name.upper():
                continue
            if verbose:
                res.utils.logger.info(f"Adding body {body.name}")
            body_part = "-".join(body.name.split("-")[:2])
            if not bodies or body_part in bodies:
                if body.object_type == "folder":
                    for find_dxf in body.get_items():
                        if ".dxf" in find_dxf.name.lower():
                            if verbose:
                                res.utils.logger.info(f"Adding dxf {find_dxf.name}")
                            d = {
                                "brand": brand.name,
                                "brand_folder_id": brand.id,
                                "setup_folder_id": setup.id,
                                "setup_folder_name": setup.name,
                                "body": body.name,
                                "dxf_file_name": find_dxf.name,
                                "dxf_file_id": find_dxf.id,
                                "modified_at": find_dxf.get().modified_at
                                if pull_modified_dates
                                else None,
                            }
                            data.append(d)

    data = []

    def could_contain_markers(f):
        f = f.name.lower().replace(" ", "")
        if "aimarkers" in f or "markers" in f or "dxf" in f:
            return True
        return False

    for brand in folder.get_items(limit=10):
        if not brands or brand.name in brands:
            # filter for brand here
            if brand.object_type == "folder":
                res.utils.logger.info(f"Processing brand {brand.name }")
                for f in brand.get_items():
                    if f.object_type == "folder" and could_contain_markers(f):
                        modified_at = f.get().modified_at
                        modified_at = res.utils.dates.parse(modified_at)
                        if (
                            folders_modified_after is not None
                            and modified_at < folders_modified_after
                        ):
                            res.utils.logger.info(
                                f"Skipping folder {f.name} which has not been modified since {folders_modified_after}"
                            )
                            continue
                        res.utils.logger.info(
                            f"Processing folder {f.name} which has been modified at {modified_at}"
                        )
                        for setup in f.get_items():
                            if "setup" in setup.name.lower().replace(" ", ""):
                                append_bodies(setup, data, brand)
                            # the other format is just adding direct e.g. for core core convention
                        append_bodies(f, data, brand)

    data = pd.DataFrame(data)
    data["s3_path"] = data["dxf_file_name"].map(s3_file_name)

    if fetch_new:
        res.utils.logger.debug(f"Sending to s3 {len(data)} records if new")
        if save_to:
            res.utils.logger.info(f"saving feather file: {save_to}")
            data.to_feather(save_to)

        for record in tqdm(data.to_dict("records")):
            if not s3.exists(record["s3_path"]) or copy_always:
                try:
                    box.copy_to_s3(record["dxf_file_id"], record["s3_path"])
                except:
                    res.utils.logger.warn(f"Failed on {record}")
    return data


@retry(wait=wait_fixed(60), stop=stop_after_attempt(1))
def prepare_listing(df):

    """
    we can get a dataframe of files and process them so that we know about sizes etc
    """

    def has_combos(x):
        for a in x:
            if "_c" in a:
                return True
        return False

    def try_get_version(s):
        try:
            return s.split("-")[2].lower().replace("v", "")
        except:
            return None

    from res.connectors.airtable import AirtableConnector

    res.utils.logger.info("loading body info")
    bodies = AirtableConnector.get_airtable_table_for_schema_by_name("meta.bodies")
    df = df.sort_values("modified_at")
    df = df.drop_duplicates(subset=["s3_path"], keep="last")
    bvs = df.groupby("body").agg({"s3_path": list}).reset_index()
    bvs["body_code"] = bvs["body"].map(lambda x: x[:7])
    bvs = pd.merge(
        bvs,
        bodies[["key", "available_sizes"]],
        left_on="body_code",
        right_on="key",
        how="left",
    )

    res.utils.logger.info(
        f"without available sizes: {len(bvs[bvs['available_sizes'].isnull()])}"
    )
    bvs = bvs[bvs["available_sizes"].notnull()]

    bvs["available_sizes"] = bvs["available_sizes"].map(
        lambda x: [a.strip() for a in x.split(",")]
    )
    # bvs = bvs  # [['body_code', 's3_path', 'available_sizes']]
    bvs["body_version"] = bvs["body"].map(try_get_version)
    bvs["num_files"] = bvs["s3_path"].map(len)
    bvs["num_sizes"] = bvs["available_sizes"].map(
        lambda x: len(x) if isinstance(x, list) else None
    )
    bvs["has_combos"] = bvs["s3_path"].map(has_combos)

    return bvs


def classify_files(
    body, filenames, sizes, body_version=None, load_names=True, size_resolver=None
):
    """

    for any record that has a body code, a set of files and sizes for the body, apply the classifier
    it is a way to overcome the inconsistency in naming net files so we can parse out components for size and rank with some confidence

    pass in version if know or we infer

    # in the first two cases we have the combo or self in the name
    # in the next case we have a hybrid where the combo may be in the name but the self is omitted
    # we also have other places the _c can be placed in the file
    #  's3://meta-one-assets-prod/bodies/net_files/jr_2000/v18/jr_2000_v18_net_37_15x15.dxf',
    #  's3://meta-one-assets-prod/bodies/net_files/jr_2000/v18/jr_2000_v18_net_37_c_15x15.dxf',
    #  's3://meta-one-assets-prod/bodies/net_files/rr_3004/v1/rr_3004_v1_net_s_10x10_c.dxf'
    """

    size_resolver = {} or size_resolver

    def names_from_setups(f):
        names = []
        with res.connectors.load("s3").file_object(f) as f:
            for l in [l.decode() for l in f.readlines()]:
                if body in l and len(l.split(" ")) == 1:
                    names.append(l.rstrip("\n").rstrip("\r"))
        return names

    def get_combo_component(l):
        """
        this is to check the set not the individual to see if there is a combo qualifier in the set
        """
        for item in l:
            filename = item.split("/")[-1].split(".")[0]
            filename_components = filename.split("_")
            # print(filename_components)
            if "c" in filename_components:
                return filename_components.index("c"), len(filename_components)
        return None, None

    has_combos, num_components_combos = get_combo_component(filenames)

    def match_sizes(comps, sizes):
        nsizes = [s.lower().replace("-", "_").replace(" ", "_") for s in sizes]
        mapping = dict(zip(nsizes, sizes))
        # this is where we add size aliases i.e. things that people name things in files that are not proper size chart values
        added = {}
        for k, v in mapping.items():
            if "kids_" in k:
                size_alias = k.replace("kids_", "")
                added[size_alias] = v
            if "size_" in k:
                size_alias = k.replace("size_", "")
                added[size_alias] = v
            if "one_size_fits_all" in k:
                added[size_alias] = "os"
            if "L" in k:
                added[size_alias] = "lg"
            if "M" in k:
                added[size_alias] = "md"
            if "S" in k:
                added[size_alias] = "sm"
        mapping.update(added)
        nsizes = list(mapping.keys())
        # print(mapping)

        # lambda function or alias -> this creates a many to one function onto the Size chart based on aliases its just a dict
        # check special cases if petites because there is a pre context
        if "p" in comps:
            comps = [f"p_{c}" for c in comps]
        if "pt" in comps:
            comps = [f"pt_{c}" for c in comps]

        # print(nsizes, comps)
        sizes = set(nsizes) & set(comps)

        if len(sizes) == 1:
            return mapping[list(sizes)[0]]
        else:
            sizes = [s for s in sizes if s not in ["s", "c"]]
            if len(sizes):
                return mapping[sizes[0]]

    def iterator():
        for item in filenames:
            filename = item.split("/")[-1].split(".")[0]
            comps = filename.split("/")[-1].split("_")
            rank = "self"
            if has_combos:
                # little dangerous? the silent "self" i.e. combo specified and self not e.g. AS-3001-V1-NET
                rank = (
                    "self" if len(comps) < num_components_combos else comps[has_combos]
                )
                if rank == "c":
                    rank = "combo"
                elif rank == "s":
                    rank = "self"
                elif rank == "f":
                    rank = "fuse"
                else:
                    rank = "self"

            # if its one_size_fits_all then the comps thing does not work but there is only one size anyway
            s = (
                match_sizes(comps, sizes)
                if "one_size_fits_all" not in filename
                else sizes[0]
            )
            yield {
                "body_code": body,
                "body_version": body_version or item.split("/")[-2].replace("v", ""),
                "normed_size": s,
                "size_code": size_resolver.get(s),
                "type": rank,
                "uri": item,
                "valid_body_file": body.lower().replace("-", "_") in filename,
                "file_key": filename.split(".")[0],
                "piece_names": names_from_setups(item) if load_names else None,
            }

    classified = list(iterator())
    # assert len(filenames) == len(classified), f"we could not determine the structure of all files {filenames}"

    return classified


def classify_files_from_row(
    row,
    as_df=False,
    load_names=True,
    size_resolver=None,
    root=f"s3://meta-one-assets-prod/bodies/net_files_normed",
):
    """
    row: body_code, s3_path, available_sizes
    rewrites to a new location at root

    """

    try:

        def make_path(row):
            body_code = row["body_code"].lower().replace("-", "_")
            return f"{root}/{body_code}/{row['body_version']}/{row['normed_size']}/{row['file_key']}-{row['type']}.dxf".lower()

        files = classify_files(
            row["body_code"],
            row["s3_path"],
            row["available_sizes"],
            load_names=load_names,
            size_resolver=size_resolver,
        )
        data = pd.DataFrame()
        if len(files):
            data = pd.DataFrame(files)
            data["normed_path"] = data.apply(make_path, axis=1)
        return data.to_dict("records") if as_df == False else data
    except:
        print(dict(row))
        raise


def piece_validator_factory(use_airtable=False):
    from res.flows.meta.pieces import PieceName, get_piece_components_cache

    kc = get_piece_components_cache(just_use_airtable=use_airtable)
    if not use_airtable:
        kc = kc.read()

    def invalid_piece_names(s):
        if isinstance(s, list):
            return [p for p in s if len(PieceName(p).validate(known_components=kc)) > 0]
        return s

    return invalid_piece_names


# classify_files_from_row(row)
def classify_files_from_records(data, load_names=True, validate_piece_names=False):
    """
    records could be loaded from what we get on response of updating box or from a listing on s3
    data can be the result ot _prepare_listing(file-list), file-list as returned from the scraper

    it can be any data set with a body, sizes and files on s3 for the net dxfs
    """
    # override
    load_names = load_names or validate_piece_names

    def size_resolver():
        sizes = res.connectors.load("airtable")["appjmzNPXOuynj6xP"][
            "tblvexT7dliEamnaK"
        ]
        sizes = sizes.to_dataframe(
            fields=["_accountingsku", "Size Normalized", "Size Chart"]
        )
        size_chart_lu = dict(sizes[["Size Chart", "_accountingsku"]].values)
        return size_chart_lu

    def extract_piece_name(s):
        if load_names == False:
            return []
        l = []
        for a in s:
            l += a["piece_names"]
        return list(set(l))

    res.utils.logger.info("Loading the piece validation factory and size resolver")
    v = piece_validator_factory() if validate_piece_names else lambda a: None
    sr = size_resolver()

    res.utils.logger.info("Extracting file classes")
    data["classified"] = data.apply(
        lambda row: classify_files_from_row(
            row, load_names=load_names, size_resolver=sr
        ),
        axis=1,
    )
    res.utils.logger.info("Cleaning up some attributes...")
    data["num_classified"] = data["classified"].map(len)
    data["normed_files"] = data["classified"].map(
        lambda p: [x["normed_path"] for x in p]
    )
    data["distinct_piece_names"] = data["classified"].map(extract_piece_name)
    res.utils.logger.info("Running validation")
    data["invalid_piece_names"] = data["distinct_piece_names"].map(v)
    res.utils.logger.info("Done")
    return data


def import_changes(changes):
    cc = net_dxf_location_cache()
    df = prepare_listing(changes)
    res.utils.logger.info(f"for {len(df)} bodies... Classifying....")
    df = classify_files_from_records(df, load_names=True, validate_piece_names=True)

    def extract_formatted(chk):
        files = {}
        for a in chk:
            if a["size_code"] is None:
                # we had a problem mapping the size for one reason or another
                return {}
            if a["size_code"] not in files:
                files[a["size_code"]] = {}
            files[a["size_code"]][a["type"] or "self"] = a["uri"]
        return files

    try:
        res.utils.logger.info(
            "Formatting and caching net dxf locations for the body version for each size..."
        )
        df["classified_formatted"] = df["classified"].map(extract_formatted)

        for record in df.to_dict("records"):
            try:
                bc = record["body_code"]
                bv = record["body_version"]
                for k, v in record["classified_formatted"].items():
                    cc.put(bc, bv, k, v)
            except Exception as ex:
                res.utils.logger.warn(f"failed to update the cache {ex}")
    except Exception as ex:
        res.utils.logger.warn(f"failed to update the cache outer loop {ex}")

    res.utils.logger.info("Done")

    # save new to hasura in this job - these are basically body updates but we dont want to update 3d owned bodies - could simple check first

    return df


def sync_changes(event={}, context={}, window=1):
    """
    high level workflow to pull down changes and sync
    the code gets new files and copies them to s3
    we also cache the locations in the net dxf body cache -> key is made of BODY-VERSION-ACCOUNTING_SIZE
    """

    date_context = res.utils.dates.utc_now() + timedelta(days=-1 * window)
    res.utils.logger.info(
        f"Looking for changes in the last {window} days. Will copy anything new to s3"
    )
    changes = get_net_files(folders_modified_after=date_context, copy_always=True)
    changes["modified_at"] = pd.to_datetime(changes["modified_at"], utc=True)
    changes = changes[changes["modified_at"] > date_context]
    res.utils.logger.info(f"Found {len(changes)} - aggregating for the body")

    res.connectors.load("s3").write(
        "s3://res-data-platform/cache/box_transfers_net_files.csv", changes
    )

    df = import_changes(changes)

    res.connectors.load("s3").write(
        "s3://res-data-platform/cache/box_transfers_net_files_imported_changes.csv",
        changes,
    )

    # reload recently added and re-validate
    cc = net_dxf_location_cache()
    stuff = cc.load(watermark_days_since_added=window)
    cc.validate(stuff, recheck_invalid=True)

    return changes


################################
##
###############################


def handler(event, context):
    with FlowContext(event, context) as fc:
        data = get_net_files()
        make_setup_files_from_net_files(data)

    return {}
