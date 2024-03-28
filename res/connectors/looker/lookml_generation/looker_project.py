"""
experimental opinionated way to manage a namespace - fill with views and models given strong types

https://github.com/looker-open-source/sdk-codegen/tree/main/python


On looker we can review and sync changes
Here when we add files PR is openend
We should delete from here and try to change in one places or manage conflicts from looker
Merge to master from git


"""

import res
import lookml
import pandas as pd
import os
from res.utils.secrets import secrets_client
import lookml
from looker_sdk import models
import looker_sdk

LOOKER_REPO = "resonance/iamcurious-looker"
# if build automation - this should be mapped to the users branch in looker
USER_GTI_BRANCH = os.environ.get("MY_LOOKER_GIT_BRANCH")
GIT_TOKEN = os.environ.get("GIT_ACCESS_TOKEN")
LOOKER_BASE_URL = os.environ.get("LOOKERSDK_BASE_URL")
LOOKER_PROJECT = "iamcurious"
SNOWFLAKE_CONNECT = "snow_cnx"


class ResLookerView:
    def __init__(self, name, **kwargs):
        pass

    @staticmethod
    def from_type(t):
        pass

    @staticmethod
    def from_pandas(p):
        pass

    @staticmethod
    def from_existing_table(name, **kwargs):
        pass


class ResLookerProject:
    def __init__(self, namespace, model, **kwargs):

        assert namespace in [
            "res.Sell",
            "res.Meta",
            "res.Production",
        ], "temp safety for conventions - want to use namespace as top level folder"

        assert (
            USER_GTI_BRANCH
        ), "You must set up your looker git branch; Set MY_LOOKER_GIT_BRANCH to the personal branch looker set for you e.g. dev-saoirse-amarteifio-jm7s"

        secrets_client.get_secret("LOOKERSDK_CLIENT_ID")
        secrets_client.get_secret("LOOKERSDK_CLIENT_SECRET")

        self._namespace = namespace

        self._project = lookml.Project(
            repo=LOOKER_REPO,
            branch=USER_GTI_BRANCH,
            access_token=GIT_TOKEN,
            looker_project_name=LOOKER_PROJECT,
        )

        # Initialize client
        self._sdk = looker_sdk.init40()

        self._views = []

    @property
    def project(self):
        return self._project

    @property
    def connections(self):
        return pd.DataFrame([dict(d) for d in self._sdk.all_connections()]).set_index(
            "name"
        )

    @property
    def folders(self):
        return pd.DataFrame([dict(d) for d in self._sdk.all_folders()]).set_index(
            "name"
        )

    def create_queue_dashboard(self, qtype, name="namespace.app.queue"):
        """
        TODO when searching for dashboards and creating models on dev views, we should set the dev workspace for this code
        """
        # broken abstraction leaving here for now
        import dataclasses
        from dataclasses import dataclass

        folder_id = str(326)
        # name and folder come from the space
        name = "flow_api_test"
        app_name = "queues"

        # lowering here which is not something we always need to do
        view_name = qtype.res_type_name().split(".")[-1].lower()

        fields = dataclasses.fields(dataclass(qtype))
        # retrieve the field names from the qtype inn the looker format - qualfied by the view name as required
        # we would want some way to choose fields properly and omit things - here we are omitting tags from analytics but we could include it in some form
        fields = [
            f"{view_name}.{lookml.core.lookCase(f.name)}"
            for f in fields
            if f.name.lower() not in ["tags"]
        ]

        exists = self._sdk.search_dashboards(title=name, folder_id=folder_id)
        if exists:
            d = exists[0]
            res.utils.logger.debug("existing dashboard matched")
        else:
            res.utils.logger.info("creating new dashboard")
            d = self._sdk.create_dashboard(
                body=models.WriteDashboard(title=name, folder_id=folder_id)
            )

        res.utils.logger.debug(
            f"creating query for view {view_name} with fields {fields} in app {app_name}"
        )
        q = self._sdk.create_query(
            {
                "model": app_name,
                "view": view_name,
                "fields": fields,
                "vis_config": {"type": "looker_grid"},
            }
        )

        de = self._sdk.search_dashboard_elements(dashboard_id=d.id, title=name)

        if len(de):
            de = de[0]
        else:
            # retrieve to upsert dashboard element -> The query seems smart enough to be hashed as a unique thing - no need to name it
            de = self._sdk.create_dashboard_element(
                body=models.WriteDashboardElement(
                    dashboard_id=d.id, title=name, query_id=q.id, type="vis"
                )
            )

        return d

    def create_model_file(self, qtypes, name=None):
        proj = self.project

        if not isinstance(qtypes, list):
            qtypes = [qtypes]

        namespace = "res.Meta"  # hard coding for now
        app_name_or_model = "queues"

        filename = f"{namespace}/{app_name_or_model}.model.lkml"
        res.utils.logger.debug(f"Upserting model {filename}")
        try:
            model = proj.file(filename)
            proj.delete(model)
            # some sort of reset for idempotency
        except:
            pass

        model = proj.new_file(filename)

        (
            model
            + """ 

        connection: "trino_dev" 

        """
        )

        (
            model
            + """ 

        include:  "/**/*.view.lkml" 

        """
        )

        for qt in qtypes:
            # get the view name from the type
            v = qt.res_type_name().split(".")[-1].lower()
            (
                model
                + f"""explore: {v}{{
                    label: "{v}"
                    persist_for: "5 seconds"
            }}"""
            )

        proj.put(model)

        return model

    def create_view_file(self, qtype):

        name = qtype.res_type_name().split(".")[-1].lower()
        namespace = "res.Meta"  # testing hardcode
        view_file_path = f"{namespace}/views/{name}.view.lkml"
        view = qtype.looker_view()

        res.utils.logger.debug(f"Upserting view {view_file_path}")

        # delete the file if it exists as a lazy idempotency method - we will refine this
        try:
            f = self.project.file(view_file_path)
            self.project.delete(f)
        except Exception as ex:
            print(ex)
            pass

        v = self.project.new_file(view_file_path)
        vo = lookml.View(name)
        vo + view
        v + vo
        self.project.put(v)

        return v

    def update_queue(self, qtype):
        """ """
        res.utils.logger.info(f"Registering queue in looker")
        _ = self.create_view_file(qtype)
        _ = self.create_model_file(qtype)
        _ = self.create_queue_dashboard(qtype)
        res.utils.logger.info("queue registered")


# class ResLookerApiSettings(looker_sdk.api_settings.ApiSettings):
#     def __init__(self, *args, **kw_args):
#         self.my_var = kw_args.pop("my_var")
#         super().__init__(*args, **kw_args)

#     def read_config(self) -> looker_sdk.api_settings.SettingsConfig:
#         config = super().read_config()

#         config["client_id"] = os.getenv("LOOKERSDK_CLIENT_ID")
#         config["client_secret"] = os.getenv("LOOKERSDK_CLIENT_SECRET")
#         return config


# def get_looker_sdk():
#     return looker_sdk.init40(config_settings=ResLookerApiSettings())
