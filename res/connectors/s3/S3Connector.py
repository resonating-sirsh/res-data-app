import pandas as pd
from pathlib import Path
import urllib
import boto3
import yaml
import json
import os
import pickle
import numpy as np
from io import BytesIO
import s3fs
import tempfile
from datetime import datetime
from pandas.api.types import is_datetime64_any_dtype as is_datetime
from dateutil.parser import parse as date_parse
import warnings


try:
    import res
    from res.utils.dates import coerce_to_full_datetime
    import fastavro

    # this is so that the imaging requirements are optional
    # numeric tests should fail when this does not load
    from skimage import io
    from PIL import Image

    Image.MAX_IMAGE_PIXELS = None
except:
    pass

warnings.simplefilter(action="ignore", category=ResourceWarning)

S3_ENTITY_ROOT = "s3://res-data-platform/entities"


class S3Connector(object):
    def __init__(self, format="line_json"):
        self.format = format
        self._s3fs = s3fs.S3FileSystem(version_aware=True)

    def get_client(self, **kwargs):
        return boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
        )

    def try_load_meta_schema(self, schema):
        schema = schema.replace(".", "-")
        return self.read(f"s3://res-data-platform/schema/{schema}.yaml")

    def exists(self, url):
        url = urllib.parse.urlparse(url)
        assert (
            url.scheme == "s3"
        ), "The url must be of the form s3://BUCKET/path/to/file/ext"
        bucket = url.netloc
        prefix = url.path.lstrip("/")
        client = self.get_client()
        result = client.list_objects(Bucket=bucket, Prefix=prefix)
        return "Contents" in result

    def as_stream(self, path):
        with self.file_object(path) as f:
            return BytesIO(f.read())

    def file_object(self, path, mode="rb", version_id=None):
        """
        by default we open in read mode as its safer
        """
        P = Path(path)
        url = urllib.parse.urlparse(path)
        assert (
            url.scheme == "s3"
        ), "The url must be of the form s3://BUCKET/path/to/file/ext"
        bucket = url.netloc
        prefix = url.path.lstrip("/")

        return self._s3fs.open(f"{bucket}/{prefix}", mode, version_id=version_id)

    def object_dir(self, path):
        from pathlib import Path

        p = path
        if "." in Path(p).name:
            return p.replace(Path(p).name, "")
        return p

    def save(self, path, data, **kwargs):
        """
        in alias for write
        """
        self.write(path=path, data=data, **kwargs)

    def write_get_info(self, path, data, **kwargs):
        """
        write a file and also retrieve the version info after saving
        """
        self.write(path, data, **kwargs)
        version = self.get_versions(path)[0]
        return {
            "s3_version_id": version["id"],
            "s3_file_modified_at": version["last_modified"].isoformat(),
        }

    def write(self, path, data, **kwargs):
        P = Path(path)
        url = urllib.parse.urlparse(path)
        assert (
            url.scheme == "s3"
        ), "The url must be of the form s3://BUCKET/path/to/file/ext"
        bucket = url.netloc
        prefix = url.path.lstrip("/")
        c = self.get_client()

        # TODO handle dataframe or special types
        if P.suffix in [".parquet"]:
            with self._s3fs.open(f"{bucket}/{prefix}", "wb") as f:
                return data.to_parquet(f, **kwargs)
        if P.suffix in [".feather"]:
            with self._s3fs.open(f"{bucket}/{prefix}", "wb") as f:
                return data.to_feather(f, **kwargs)
        if P.suffix in [".csv"]:
            with self._s3fs.open(f"{bucket}/{prefix}", "wb") as f:
                return data.to_csv(f, **kwargs)
        if P.suffix in [".pickle"]:
            with self._s3fs.open(f"{bucket}/{prefix}", "wb") as f:
                return pickle.dump(data, f, **kwargs)
        if P.suffix in [".jpg", ".jpeg", ".tiff", "tif", ".png"]:
            format = P.suffix[1:]  # this will not always work so we can test cases
            _data = BytesIO()
            if not isinstance(data, Image.Image):
                data = Image.fromarray(data)
            # todo if dpi in kwargs use here
            options = {"format": format, **kwargs}
            if "dpi" in kwargs:
                options["dpi"] = kwargs.get("dpi")
                # the caller might just specify an integer e.g. 300 but the x,y is required here i.e. (300,300)
                if isinstance(options["dpi"], int):
                    options["dpi"] = (options["dpi"], options["dpi"])

            data.save(_data, **options)
            data = _data.getvalue()
            return c.put_object(Bucket=bucket, Key=prefix, Body=data)

        if P.suffix in [".pdf"]:
            return c.put_object(
                Bucket=bucket, Key=prefix, Body=data, ContentType="application/pdf"
            )

        # the following could be a bad abstraction
        # python dicts are special. it may be we should accept them and map the data to proper vals
        if isinstance(data, dict):
            if P.suffix in [".yml", ".yaml"]:
                data = yaml.safe_dump(data)
            if P.suffix in [".json"]:
                data = json.dumps(data)

        return c.put_object(Bucket=bucket, Key=prefix, Body=data)

    def write_image_bytes(self, uri, bytes):
        """
        Save importing a few things bytes to file
        """
        b = BytesIO(bytes)
        return self.write(uri, Image.open(b))

    def open_temp_file(self, file):
        bucket, path = self.split_bucket_and_blob_from_path(file)
        # open an s3 file
        with self._s3fs.open(f"{bucket}/{path}", "rb") as s3f:
            # write to a temporary local file
            with tempfile.NamedTemporaryFile(
                suffix=f".{file.split('.')[-1]}", prefix="file", mode="wb"
            ) as f:
                f.write(s3f.read())
                f.flush()
                # return the context manager
                return f

    def process_using_tempfile(self, file, fn):
        bucket, path = self.split_bucket_and_blob_from_path(file)
        # open an s3 file
        with self._s3fs.open(f"{bucket}/{path}", "rb") as s3f:
            # write to a temporary local file
            with tempfile.NamedTemporaryFile(
                suffix=f".{file.split('.')[-1]}", prefix="file", mode="wb"
            ) as f:
                f.write(s3f.read())
                f.flush()
                # return the context manager
                return fn(f.name)

    def write_html_as_pdf_to_s3(
        self,
        html,
        out_uri,
        make_signed_url=False,
    ):
        import tempfile
        from weasyprint import HTML as WHTML

        s3 = res.connectors.load("s3")

        W = WHTML(string=html)

        with tempfile.NamedTemporaryFile(suffix=".pdf", prefix="f", mode="wb") as f:
            W.write_pdf(f.name)
            res.utils.logger.debug(f"Writing the html converted to pdf to s3 {out_uri}")
            s3.upload(f.name, out_uri)
            if make_signed_url:
                out_uri = s3.generate_presigned_url(out_uri)
        W = None
        return out_uri

    def upload(self, source, target):
        bucket, path = self.split_bucket_and_blob_from_path(target)
        client = self.get_client()
        client.upload_file(source, bucket, path)

    def upload_folder_to_archive(
        self, folder, target_folder, archive_name=None, plan=False
    ):
        import tarfile
        from tqdm import tqdm
        from glob import glob

        archive_name = archive_name or folder.rstrip("/").split("/")[-1]

        tar_file_name = f"/tmp/{archive_name}.tar.gz"
        print(f"packing {folder} into {tar_file_name}")
        with tarfile.open(tar_file_name, "w:gz") as tar:
            for name in tqdm(glob(f"{folder}/**/*.*", recursive=True)):
                p = Path(name)
                class_item = p.relative_to(folder)
                tar.add(name, arcname=f"{archive_name}/{class_item}")

        upload_uri = f"{target_folder}/{Path(tar_file_name).name}"
        print(f"uploading to {upload_uri}")

        if not plan:
            self.upload(tar_file_name, upload_uri)

    def unpack_folder_from_archive(self, uri, destination_folder):
        import tarfile

        name = uri.split("/")[-1]
        temp = f"/tmp/{name}"
        print(f"Downloading {uri} to /tmp")
        self._download(uri, target_path="/tmp")
        with tarfile.open(temp, "r") as tar:
            tar.extractall(path=destination_folder)
        print(f"Extracted to {destination_folder}")
        return f"{destination_folder}"

    def make_one_production_path(self, one_number, filename):
        return f"s3://{res.RES_DATA_BUCKET}/make-one-assets/{one_number}/{filename}"

    def upload_make_one_production_file(self, one_number, source, filename):
        self.upload(
            source,
            self.make_one_production_path(one_number, filename),
        )

    def copy_to_make_one_production(self, one_number, s3_path):
        filename = s3_path.split("/")[-1]
        self.copy(s3_path, self.make_one_production_path(one_number, filename))

    def _open_image(self, file_or_buffer, **kwargs):
        return np.array(Image.open(file_or_buffer), dtype=np.uint8)

    def get_files_sizes(self, paths, check_exists=False):
        c = boto3.resource("s3")
        for f in paths:
            if not check_exists or self.exists(f):
                bucket, key = self.split_bucket_and_blob_from_path(f)
                yield {"filename": f, "size": c.Object(bucket, key).content_length}

    def get_file_info(self, file_url):
        bucket, key = self.split_bucket_and_blob_from_path(file_url)
        return self.get_client().head_object(Bucket=bucket, Key=key)

    def split_bucket_and_blob_from_path(self, path):
        P = Path(path)
        url = urllib.parse.urlparse(path)
        assert (
            url.scheme == "s3"
        ), "The url must be of the form s3://BUCKET/path/to/file/ext"
        bucket = url.netloc
        prefix = url.path.lstrip("/")

        return bucket, prefix

    def generate_presigned_url(self, url, expiry=3600, for_upload=False):
        """
        usually for get objects or specify put_object
        https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html#generating-a-presigned-url-to-upload-a-file
        """
        bucket_name, object_key = self.split_bucket_and_blob_from_path(url)

        try:
            if for_upload:
                return self.get_client().generate_presigned_url(
                    "put_object",
                    Params={
                        "Bucket": bucket_name,
                        "Key": object_key,
                    },
                    ExpiresIn=expiry,
                    HttpMethod="PUT",
                )

            return self.get_client().generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket_name, "Key": object_key},
                ExpiresIn=expiry,
            )

        except Exception as ex:
            raise ex

    def get_versions(self, filename):
        from operator import attrgetter

        bucket, object_key = self.split_bucket_and_blob_from_path(filename)

        bucket = boto3.resource("s3").Bucket(bucket)

        # Versions must be sorted by last_modified date because delete markers are
        # at the end of the list even when they are interspersed in time.
        versions = sorted(
            bucket.object_versions.filter(Prefix=object_key),
            key=attrgetter("last_modified"),
            reverse=True,
        )

        versions = [
            {
                "filename": f"s3://{v.bucket_name}/{v.object_key}",
                "last_modified": v.last_modified,
                "id": v.id,
                "obj": v,
            }
            for v in versions
        ]

        return versions

    def rollback_object(self, filename, version_id=None, plan=False):
        """
        Rolls back an object to an earlier version by deleting all versions that
        occurred after the specified rollback version.


        - if no version is supplied we rollback to last
        - if a version is supplied we remove all versions after that

        -plan can be used to just show the versions

        if we return versions we get get the version object from `collection[i]['obj]` <- the obj is the actual reference to s3
        and we can read it if we need to
            v.get()['Body'].read()

        you can test this by update of some dummy text file

            s3 = res.connectors.load('s3')
            s3.upload( '/Users/USER/Downloads/file.txt', 's3://meta-one-assets-prod/styles/meta-marker/test/file.txt')

            versions = rollback_object(s3c,
                                's3://meta-one-assets-prod/styles/meta-marker/test/file.txt',
                                None, #<-no version
                                plan=True) #<-plan to see the versions or dont place to bump a version

        """

        bucket, object_key = self.split_bucket_and_blob_from_path(filename)

        bucket = boto3.resource("s3").Bucket(bucket)

        versions = self.get_versions(filename)

        if version_id is None and len(versions) > 2:
            version_id = versions[1]["id"]
            res.utils.logger.debug(f"we would roll back to prev version {version_id}")

        if plan:
            return versions

        if version_id in [ver["id"] for ver in versions]:
            res.utils.logger.debug(f"Rolling back to version {version_id}")
            for version in versions:
                V = version["obj"]
                if version["id"] != version_id:
                    V.delete()
                    res.utils.logger.debug(f"Deleted version {V.version_id}")
                else:
                    break

            res.utils.logger.info(
                f"Active version is now {bucket.Object(object_key).version_id}"
            )
        else:
            raise KeyError(
                f"{version_id} was not found in the list of versions for "
                f"{object_key}."
            )

    def copy(self, source, target):
        """
        Copy from one location to another
        """
        bucket_a, path_a = self.split_bucket_and_blob_from_path(source)
        bucket_b, path_b = self.split_bucket_and_blob_from_path(target)

        with self._s3fs.open(f"{bucket_a}/{path_a}", "rb") as s:
            with self._s3fs.open(f"{bucket_b}/{path_b}", "wb") as t:
                t.write(s.read())

    def unzip(
        self,
        archive_path,
        target_path,
        plan=False,
        exclude_match=[".DS_Store"],
        verbose=False,
    ):
        """
        example

        s3.unzip('s3://res-data-platform/samples/zip_files/simple.zip',
                 "s3://res-data-platform/samples/test_unzip/folder1/simple",
                 plan=False)

        be careful with intent on extracted folder - if the archive contains a top level folder with the right name you can ignore /simple/ in the path
        """

        from zipfile import ZipFile
        import io

        # "s3://meta-one-assets-prod/color_on_shape/cc_6001/v10/somelu/zipped_pieces.zip"

        exclude_match = exclude_match or []

        def valid(name):
            for m in exclude_match:
                if m in name:
                    return False
            return True

        def _make_path(path):
            return f"{target_path}/{path}"

        fs = s3fs.S3FileSystem()
        input_zip = ZipFile(io.BytesIO(fs.cat(archive_path)))
        extracted = []
        for name in input_zip.namelist():
            if not valid(name) or input_zip.getinfo(name).is_dir():
                continue

            data = io.BytesIO(input_zip.read(name))
            _target = _make_path(name)
            if verbose:
                res.utils.logger.info(f"unpack to {_target}")
            extracted.append(_target)
            if not plan:
                with self.file_object(_target, "wb") as f:
                    f.write(data.read())

        return extracted

    def rename(self, source, target):
        self._s3fs.rename(source, target)

    def read_text(self, path, **kwargs):
        P = Path(path)
        url = urllib.parse.urlparse(path)
        assert (
            url.scheme == "s3"
        ), "The url must be of the form s3://BUCKET/path/to/file/ext"
        bucket = url.netloc
        prefix = url.path.lstrip("/")

        with self._s3fs.open(f"{bucket}/{prefix}", "r") as f:
            return f.read()

    def read_text_lines(self, path, **kwargs):
        P = Path(path)
        url = urllib.parse.urlparse(path)
        assert (
            url.scheme == "s3"
        ), "The url must be of the form s3://BUCKET/path/to/file/ext"
        bucket = url.netloc
        prefix = url.path.lstrip("/")

        with self._s3fs.open(f"{bucket}/{prefix}", "r") as f:
            for line in f.readlines():
                yield line

    def read_logs_in_folder(self, uri, match_string=None):
        def _reader():
            for f in self.ls(uri):
                lines = list(self.read_text_lines(f))
                if match_string:
                    if match_string not in str(lines):
                        continue
                for l in lines:
                    yield l

        return list(_reader())

    def summarize_logs_in_folder(self, uri):
        """
        experimental convenience
        """
        from res.learn.agents import InterpreterAgent

        agent = InterpreterAgent()
        data = self.read_logs_in_folder(uri)
        return agent.ask(
            f"Please summarize the logs - pull out any identifiers and exceptions that are observed and also any mention of contract variables: {data}, You can ignore any json attributes preamble that seem like configuration settings"
        )

    def read_image_properties(self, uri):
        """
        the file object needs the file to be read properly not as we do from numpy arrays
        """
        from PIL import Image

        with self.file_object(uri) as f:
            im = Image.open(f)
            return im.info

    def read_image(self, uri, version_id=None):
        """
        read as pil image
        """

        if version_id is None:
            return Image.fromarray(self.read(uri))

        bucket_name, object_key = self.split_bucket_and_blob_from_path(uri)
        response = self.get_client().get_object(
            Bucket=bucket_name,
            Key=object_key,
            VersionId=version_id,
        )
        image_data = response["Body"].read()

        return Image.open(BytesIO(image_data))

    def get_version_id(self, path, version_id=None, before=None, after=None, at=None):
        if np.count_nonzero([version_id, before, after, at]) != 1:
            raise ValueError("Must choose one of version_id, before, after, at.")
        # res.utils.logger.debug(f"{version_id} {before} {after} {path}")

        versions = self.get_versions(path)
        if at:
            for version in versions:
                if version["last_modified"] == at:
                    return version["id"]

            res.utils.logger.debug(f"Version at {at} {path} doesn't exist!")
            raise FileNotFoundError(f"No version of {path} found at {at}")
        if before:
            for version in versions:
                if version["last_modified"] < before:
                    return version["id"]

            res.utils.logger.debug(f"Version before {before} {path} doesn't exist!")
            raise FileNotFoundError(f"No version of {path} found before {before}")
        elif after:
            # reversed so you get the first one after the passed in date
            for version in reversed(versions):
                if version["last_modified"] > after:
                    return version["id"]

            res.utils.logger.debug(f"Version after {after} {path} doesn't exist!")
            raise FileNotFoundError(f"No version of {path} found after {after}")
        elif isinstance(version_id, int):
            if version_id > 0 or version_id <= -len(versions):
                raise ValueError(
                    f"version_id ({version_id}) is out of range (0 to -{len(versions)-1})"
                )
            return versions[-version_id]["id"]
        else:
            if version_id not in [v["id"] for v in versions]:
                res.utils.logger.debug(f"Version {version_id} {path} doesn't exist!")
                raise FileNotFoundError(f"{path} v{version_id}")
            return version_id

    def read_version(self, path, version_id=None, before=None, after=None, at=None):
        url = urllib.parse.urlparse(path)
        assert (
            url.scheme == "s3"
        ), "The url must be of the form s3://BUCKET/path/to/file/ext"
        bucket = url.netloc
        prefix = url.path.lstrip("/")

        c = self.get_client()
        try:
            version_id = self.get_version_id(
                path, version_id=version_id, before=before, after=after, at=at
            )
            return c.get_object(Bucket=bucket, Key=prefix, VersionId=version_id)["Body"]
        except Exception as ex:
            raise ex

    def iterate_image_versions(self, uri):
        """
        there is a not fully tested implementation of read version for images. For example the uri i used in the png code path which does not respect the streaming body result
        it would be better to use the image reader from the streaming body instead of what we do but i dont want to change it just yet
        if we do change it probably best to isolate to the versioned id case to isolate impact unless sure

        """
        for f in self.get_versions(uri):
            im = self.read_version(f["filename"], version_id=f["id"]).read()
            im = Image.open(BytesIO(im))

            yield f, im

    def read(self, path, version_id=None, before=None, after=None, at=None, **kwargs):
        """
        NB: version id not supported on all code paths here!!
        """
        P = Path(path)
        url = urllib.parse.urlparse(path)
        assert (
            url.scheme == "s3"
        ), "The url must be of the form s3://BUCKET/path/to/file/ext"
        bucket = url.netloc
        prefix = url.path.lstrip("/")

        def get_streaming_body():
            c = self.get_client()
            try:
                if version_id or before or after or at:
                    response = self.read_version(
                        path, version_id=version_id, before=before, after=after, at=at
                    )
                else:
                    response = c.get_object(Bucket=bucket, Key=prefix)["Body"]
                return response
            except Exception as ex:
                raise ex

        if P.suffix in [".yml", ".yaml"]:
            return yaml.safe_load(get_streaming_body())
        if P.suffix in [".json"]:
            return json.load(get_streaming_body())
        if P.suffix in [".svg"]:
            return get_streaming_body().read().decode()
        if P.suffix in [".csv"]:
            return pd.read_csv(path, **kwargs)
        if P.suffix in [".txt", ".log"]:
            return get_streaming_body().read()

        if P.suffix in [".parquet"]:
            with self._s3fs.open(f"{bucket}/{prefix}", "rb") as f:
                return pd.read_parquet(f, **kwargs)
        if P.suffix in [".avro"]:
            encoding = kwargs.get("encoding", "rb")
            with self._s3fs.open(f"{bucket}/{prefix}", encoding) as f:
                if "ignore_root" in kwargs:
                    root = kwargs.get("ignore_root")
                    return pd.DataFrame.from_records(
                        [r[root] for r in fastavro.reader(f)]
                    )
                return pd.DataFrame.from_records([r for r in fastavro.reader(f)])
        if P.suffix in [".feather"]:
            with self._s3fs.open(f"{bucket}/{prefix}", "rb") as f:
                return pd.read_feather(f, **kwargs)
        # TODO: all these need to be handled
        if P.suffix in [".png", ".jpg", ".jpeg", ".tiff", ".tif"]:
            with self._s3fs.open(f"{bucket}/{prefix}", "rb") as s3f:
                with tempfile.NamedTemporaryFile(
                    suffix=".png", prefix="f", mode="wb"
                ) as f:
                    f.write(s3f.read())
                    f.flush()
                    return io.imread(f.name)
        if P.suffix in [".dxf"]:
            from res.media.images.providers.dxf import DxfFile

            s3_path = f"{bucket}/{prefix}"
            if version_id or before or after or at:
                version_id = self.get_version_id(
                    path, version_id=version_id, before=before, after=after, at=at
                )

            version_args = {"version_id": version_id} if version_id is not None else {}
            res.utils.logger.debug(version_args)
            with self._s3fs.open(s3_path, "r", **version_args) as f:
                d = self.object_dir("s3://" + s3_path).rstrip("/")
                return DxfFile(f, home_dir=d, **kwargs)
        if P.suffix in [".pickle"]:
            with self._s3fs.open(f"{bucket}/{prefix}", "rb") as f:
                return pickle.load(f)

        raise Exception(f"TODO: handle case for file type {path}")

    def read_file_bytes(self, uri: str) -> BytesIO:
        with self._s3fs.open(uri, "rb") as t:
            bts = t.read()
            return BytesIO(bts)

    def mounted_file(self, file, mode=None):
        """
        WIP - experiment with file modes
        default mode is read - change to rb if needed
        returns a file object locally if needed

        caller should use file.name to read

        with s3.mounted_file('path') as f:
            read(f.name)
        """
        bucket, prefix = self.split_bucket_and_blob_from_path(file)

        with self._s3fs.open(f"{bucket}/{prefix}", "r") as s3f:
            # experimenting with just downloading it for now as its just k8s pods anyway
            self._download(file, target_path="/tmp", filename=Path(file).name)
            # f = tempfile.NamedTemporaryFile(
            #     suffix=Path(file).suffix, prefix=Path(file).stem, mode="w"
            # )
            # f.write(s3f.read())
            # f.flush()
            # return f
            return f"/tmp/{Path(file).name}"

    def _download(self, path, target_path=None, filename=None):
        """
        We generally do not download to disk - this is a convenience method to fetch paths for local inspection
        Files are always downloaded to the user's home /Downloads
        """
        P = Path(path)
        if target_path is not None and not os.path.exists(target_path):
            Path(target_path).mkdir(parents=True)
        home = str(Path.home())
        filename = filename or P.name
        to = (
            f"{home}/Downloads/{filename}"
            if target_path is None
            else f"{target_path}/{filename}"
        )

        with open(to, "wb") as f:
            self._download_to_file(path, f)
            return to

    def _download_to_file(self, path, dst_file):
        url = urllib.parse.urlparse(path)
        assert (
            url.scheme == "s3"
        ), "The url must be of the form s3://BUCKET/path/to/file/ext"
        bucket = url.netloc
        prefix = url.path.lstrip("/")
        self.get_client().download_fileobj(bucket, prefix, dst_file)

    def ls(self, url, suffixes=None, modified_after=None, modified_before=None):
        for info in self.ls_info(
            url=url,
            suffixes=suffixes,
            modified_after=modified_after,
            modified_before=modified_before,
        ):
            yield info["path"]

    def get_files_with_versions(self, uri):
        """
        not tested for pagination
        """
        bucket, prefix = self.split_bucket_and_blob_from_path(uri)
        # paginator = client.get_paginator("list_objects")
        V = self.get_client().list_object_versions(Bucket=bucket, Prefix=prefix)
        files = []
        for v in V["Versions"]:
            files.append(
                {
                    "version_id": v["VersionId"],
                    "uri": f"s3://{bucket}/{v[f'Key']}",
                    "last_modified": v["LastModified"].isoformat(),
                }
            )

        return files

    def ls_info(self, url, suffixes=None, modified_after=None, modified_before=None):
        """
        Use the paginator to list any number of object names in the bucket
        A path prefix can be used to limit the search
        One or more suffixes e.g. .ext, .txt, .parquet, .csv can be used to filter
        A generator is returned providing only the file names fully qualified s3://BUCKET/file.ext
        Also the url passed in should be an s3://url or url expression with wild cards (not yet implemented)
        In future could optionally return metadata such as time stamps
        """
        url = urllib.parse.urlparse(url)
        assert (
            url.scheme == "s3"
        ), "The url must be of the form s3://BUCKET/path/to/file/ext"
        bucket = url.netloc
        prefix = url.path.lstrip("/")

        client = self.get_client()

        paginator = client.get_paginator("list_objects")
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

        def _match_suffixes(s):
            return Path(s).suffix in suffixes

        for page in page_iterator:
            if "Contents" in page:
                for obj in page["Contents"]:
                    f = obj["Key"]
                    if not suffixes or _match_suffixes(f):
                        mod_date = obj["LastModified"]

                        if isinstance(mod_date, str):
                            mod_date = date_parse(mod_date)

                        mod_date = mod_date.replace(tzinfo=None)
                        # conditions
                        if modified_before and mod_date > coerce_to_full_datetime(
                            modified_before
                        ):
                            continue
                        if modified_after and mod_date < coerce_to_full_datetime(
                            modified_after
                        ):
                            continue

                        yield {
                            "path": f"s3://{bucket}/{f}",
                            "size": obj["Size"],
                            "last_modified": mod_date,
                        }

    def delete_files(self, files):
        """
        delete a collection of objects
         s3://path1
         s3://path2
        """
        for f in files:
            self._delete_object(f)

    def _delete_object(self, path):
        """
        delete an objective given by s3:/path/to/object
        """
        boto3.resource("s3").Object(
            *self.split_bucket_and_blob_from_path(path)
        ).delete()

    def get_partition_path_for_entity(self, entity):
        return f"{S3_ENTITY_ROOT}/{entity}"

    def write_date_partition(
        self,
        df,
        date_partition_key,
        entity,
        group_partition_key=None,
        dedup_key=None,
        root=S3_ENTITY_ROOT,
        replace=False,
        objects_as_strings=True,
    ):
        """
        Utility to save data to partitions by some group key and date
        The dates will be converted to UTC for storage if possible e.g. if time zoon information os included or we assume utc

        """
        new_date_col = "__resDATE_KEY"
        df[new_date_col] = df[date_partition_key]
        if not is_datetime(df[new_date_col]):
            df[new_date_col] = pd.to_datetime(
                df[new_date_col], errors="coerce", utc=True
            )

        df[new_date_col] = df[new_date_col].dt.date

        timestamp = datetime.utcnow()

        ## parquet cannot save complex types so we convert them to strings if the option is set
        # if objects_as_strings:
        #     for k, v in df.dtypes.items():
        #         if v == object:
        #             df[k] = df[k].astype(str)

        # a unique key to determine duplicates - by default the full row
        # this would not work on complex types but these are removed for parquet
        if dedup_key is None:
            dedup_key = list(df.columns)
        if not isinstance(dedup_key, list):
            dedup_key = [dedup_key]

        ######################################################

        def partition_df(data, group, date_key, results=None):
            results = results or []

            path = f"{root}/{entity}/{group}/{date_key}.parquet"
            data["__partition_write_ts"] = timestamp

            if not replace:
                # if replace - just trust that we have all the data and dont check existing which is much slower
                if self.exists(path):
                    existing_data = self.read(path)

                    # existing_types = dict(existing_data.dtypes)
                    # new_types = dict(data.dtypes)

                    # for k, v in existing_types.items():
                    #     if new_types[k] != v:
                    #         print(f"converting type of {k} to match existing")
                    #         data[k] = data[k].astype(v)

                    data = (
                        pd.concat([existing_data, data])
                        .sort_values("__partition_write_ts")
                        .drop_duplicates(subset=dedup_key, keep="last")
                    )

            try:
                data.to_parquet(path)
            except Exception as ex:
                raise Exception(
                    f"Failed to save partition data for path {path} - datatypes are {data.dtypes}"
                ) from ex

            results.append(
                {
                    "key": group,
                    "partition_key": date_key,
                    "entity": entity,
                    "path": path,
                    "rowsum": len(data),
                }
            )

        ##########################################################

        results = []
        if group_partition_key:
            for group, group_data in df.groupby(group_partition_key):
                for date_key, data in group_data.groupby(new_date_col):
                    partition_df(data, group, date_key, results)

        else:
            for date_key, data in df.groupby(new_date_col):
                partition_df(data, "ALL_GROUPS", date_key, results)

        return pd.DataFrame(results)

    def get_resmagic_file(self, fid, download=False):
        #'61687a016ec8180008f91a09'
        from bson.objectid import ObjectId

        mongo = res.connectors.load("mongo")
        files = mongo["resmagic"]["files"]
        r = files.find_one({"_id": ObjectId(fid)})

        r = r["s3"]

        file = r["key"].split("/")[-1]
        file = f"s3://{r['bucket']}/{r['key']}"

        if download:
            self._download(file)
        return file

    def put_resmagic_file(self, s3_uri):
        s3_bucket, s3_key = self.split_bucket_and_blob_from_path(s3_uri)
        file_info = self.get_file_info(s3_uri)
        return res.connectors.load("graphql").query(
            """
            mutation createS3File($input: CreateS3FileInput!) {
                createS3File(input: $input) {
                    file {
                        id
                    }
                }
            }
            """,
            {
                "input": {
                    "name": s3_uri.split("/")[-1],
                    "type": file_info["ContentType"],
                    "size": file_info["ContentLength"],
                    "bucket": s3_bucket,
                    "key": s3_key,
                }
            },
        )["data"]["createS3File"]["file"]["id"]


# TODO: - ingegation tests on IO and ls of files - for example dates used in ls watermarks
