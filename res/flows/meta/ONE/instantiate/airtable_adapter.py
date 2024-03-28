"""
save to s3 from airtable so we can parse and upload stuff
"""

import requests
import traceback
import re
import res
import json

from res.learn.agents.builder.utils import ask, describe_visual_image

from res.flows.meta.ONE.instantiate.model import ROOT


def clean_string(s):
    def remove_dots_keep_last(input_string):
        parts = input_string.split(".")
        if len(parts) > 0:
            prefix = f"_".join(parts[:-1])
            return f"{prefix}.{parts[-1]}"
        return input_string

    s = re.sub(r"[^a-zA-Z0-9.]+", "-", s).lower()
    s = remove_dots_keep_last(s)
    return s


def fetch_to_s3(airtable_attachment, root):
    s3 = res.connectors.load("s3")
    response = requests.get(airtable_attachment["url"])
    filename = clean_string(airtable_attachment["filename"])

    uri = f"{root}/{filename}"
    with s3.file_object(uri, "wb") as f:
        res.utils.logger.debug(uri)
        f.write(response.content)
    return uri


class AirtableTechPackIngester:
    def __init__(
        self,
        airtable_record_id,
        version=0,
        key_col="BODY CODE",
        tech_pack_col="Sew By Images",
        feedback_images=["Annotated Photos From SEW"],
        dxf_file="2D Input File",
        fabric_column="Fabric for First ONE",
        airtable_source="appa7Sw0ML47cA8D1/tblunjhEa4EmdOUnX",
        build=True,
    ):
        s3 = res.connectors.load("s3")
        airtable = res.connectors.load("airtable")
        self._airtable_record = airtable[airtable_source.split("/")[0]][
            airtable_source.split("/")[1]
        ].get_record(airtable_record_id)["fields"]

        res.utils.logger.debug(self._airtable_record.keys())

        key = self._airtable_record[key_col]
        my_root = f"{ROOT}/{key}/v{version}/input"
        res.utils.logger.debug(f"{my_root=}")

        if build:
            for attachment in self._airtable_record[tech_pack_col]:
                fetch_to_s3(attachment, root=f"{my_root}/techpack")

            for attachment in self._airtable_record[dxf_file]:
                fetch_to_s3(attachment, root=f"{my_root}/dxf")

            for col in feedback_images:
                for attachment in self._airtable_record[col]:
                    fetch_to_s3(attachment, root=f"{my_root}/feedback-images")

        self._content = list(s3.ls(my_root))

        self._info = {"body_code": key, "files_input_list": list(self._content)}
        self._output_dir = f"{ROOT}/{key}/v{version}/output"
        self._input_dir = my_root


"""

2024-03-09 12:58:56 [debug    ] s3://res-data-platform/meta-one-input/bodies/EP-6002/v0/input/techpack/8547bc6c-8055-4116-8a89-fc184d7ec66f.jpg
2024-03-09 12:58:56 [debug    ] s3://res-data-platform/meta-one-input/bodies/EP-6002/v0/input/techpack/techpack-ep-6000.pdf
2024-03-09 12:58:57 [debug    ] s3://res-data-platform/meta-one-input/bodies/EP-6002/v0/input/techpack/2024-cutters-must-midi-dress-no-pocket.pdf
2024-03-09 12:58:57 [debug    ] s3://res-data-platform/meta-one-input/bodies/EP-6002/v0/input/dxf/ls-midi-dress-3-4-2024.dxf
2024-03-09 12:58:58 [debug    ] s3://res-data-platform/meta-one-input/bodies/EP-6002/v0/input/dxf/ls-midi-dress-3-4-2024.rul
2024-03-09 12:58:58 [debug    ] s3://res-data-platform/meta-one-input/bodies/EP-6002/v0/input/feedback-images/whatsapp-image-2024-02-07-at-3.35.44-pm.jpg
2024-03-09 12:58:58 [debug    ] s3://res-data-platform/meta-one-input/bodies/EP-6002/v0/input/feedback-images/whatsapp-image-2024-02-07-at-3.35.44-pm-1-.jpg
2024-03-09 12:58:59 [debug    ] s3://res-data-platform/meta-one-input/bodies/EP-6002/v0/input/feedback-images/whatsapp-image-2024-02-07-at-3.35.44-pm-2-.jpg

"""
