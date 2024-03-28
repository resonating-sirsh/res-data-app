import res
from res.flows import FlowContext
import os

RES_ENV = os.environ.get("RES_ENV")


def handler(event, context={}):
    with FlowContext(event, context):
        # only do this on dev for now
        if RES_ENV == "production":
            return {}

        # slack = res.connectors.load("slack")
        shortcut = res.connectors.load("shortcut")
        airtable = res.connectors.load("airtable")
        project_table = airtable["app70YvSyxW4d0vJ3"]["tblSyArc4vdTnIDz9"]
        adata = project_table.to_dataframe(
            fields=["Name", "% Current", "Shortcut milestone"]
        )

        res.utils.logger.info("fetch and slack status")

        data = shortcut.get_one_platform_reframe_status(slack_it=True)

        d = shortcut.get_milestones()[["app_url", "name"]]
        d = dict(
            data.join(d.set_index("name"))[["app_url", "Perecentage Complete"]]
            .set_index("app_url")
            .reset_index()
            .values
        )
        airtable = res.connectors.load("airtable")
        project_table = airtable["app70YvSyxW4d0vJ3"]["tblSyArc4vdTnIDz9"]
        adata = project_table.to_dataframe(
            fields=["Name", "% Current", "Shortcut milestone"]
        ).drop("__timestamp__", 1)
        adata = adata[adata["Shortcut milestone"].notnull()]
        adata["% Current"] = adata["Shortcut milestone"].map(lambda x: d.get(x))

        res.utils.logger.info("updating airtable")

        for r in adata.to_dict("records"):
            project_table.update_record(r)

        res.utils.logger.info("done")

    return {}
