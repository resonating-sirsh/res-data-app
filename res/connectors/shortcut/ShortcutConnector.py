from res.utils import secrets_client
import os
import requests
import pandas as pd
from res.utils import secrets_client, dataframes
import res

SHORTCUT_HOST = os.environ.get("SHORTCUT_HOST", "https://api.app.shortcut.com")
SHORTCUT_API_TOKEN = secrets_client.get_secret("SHORTCUT_API_TOKEN")

STORY_FIELDS = [
    "id",
    "name",
    "estimate",
    "owner_ids",
    "description",
    "started",
    "completed",
    "blocked",
    "group_id",
    "epic_id",
    "iteration_id",
    "updated_at",
    "created_at",
    "completed_at",
    "app_url",
]
EPIC_MILESTONE_FIELDS = [
    "id",
    "name",
    "app_url",
    "started_at",
    "completed_at",
    "labels",
    "state",
    "group_name",
    "planned_start_date",
    "stats",
    "state_milestone",
    "name_milestone",
    "completed_at_override_milestone",
    "app_url_milestone",
]
ONE_groups = ["Meta-One", "Autobots", "res-premises"]


class ShortcutConnector:
    def __init__(self, **kwargs):
        pass

    def get_groups(self, names):

        url = "https://api.app.shortcut.com/api/v3/groups"

        params = {}
        response = requests.get(
            url, headers={"Shortcut-Token": SHORTCUT_API_TOKEN}, params=params
        ).json()
        df = pd.DataFrame(response)

        df = df[df["name"].isin(names)].reset_index() if names else df
        return df.set_index("id")

    def get_milestones(self):
        url = "https://api.app.shortcut.com/api/v3/milestones"
        params = {}
        response = requests.get(
            url, headers={"Shortcut-Token": SHORTCUT_API_TOKEN}, params=params
        ).json()
        df = pd.DataFrame(response)
        return df

    def get_iterations(self):
        url = "https://api.app.shortcut.com/api/v3/iterations"
        params = {}
        response = requests.get(
            url, headers={"Shortcut-Token": SHORTCUT_API_TOKEN}, params=params
        ).json()
        df = pd.DataFrame(response)
        return df

    def get_members(self):
        url = "https://api.app.shortcut.com/api/v3/members"
        params = {}
        response = requests.get(
            url, headers={"Shortcut-Token": SHORTCUT_API_TOKEN}, params=params
        ).json()
        df = pd.DataFrame(response)

        for n in ["name", "mention_name", "email_address"]:
            df[n] = df["profile"].map(lambda a: a.get(n))

        return df.set_index("id")

    def get_epics(self, groups):
        group_ids = self.get_groups(names=groups) if ONE_groups else None
        group_ids = list(group_ids.index)

        url = "https://api.app.shortcut.com/api/v3/epics"

        params = {}
        response = requests.get(
            url, headers={"Shortcut-Token": SHORTCUT_API_TOKEN}, params=params
        ).json()

        df = pd.DataFrame(response)
        df = df[df["group_id"].isin(group_ids)].reset_index() if group_ids else df
        m = self.get_milestones()
        df = pd.merge(
            df,
            m,
            how="left",
            left_on="milestone_id",
            right_on="id",
            suffixes=["", "_milestone"],
        )

        groups = dict(self.get_groups(None)[["name"]].reset_index().values)
        df["group_name"] = df["group_id"].map(lambda g: groups.get(g))
        return df

    def get_labelled_stories(self, label):

        results = []
        url = "https://api.app.shortcut.com/api/v3/search/stories"
        params = {"query": f"""label:"{label}" """, "page_size": 25}
        while url:
            response = requests.get(
                url, headers={"Shortcut-Token": SHORTCUT_API_TOKEN}, params=params
            ).json()
            data = response.get("data", [])
            next_token = response.get("next")
            url = SHORTCUT_HOST + next_token if next_token else None
            results += data
            params = {}
        df = pd.DataFrame(results)

        return df

    def get_milestone_stories(self, milestone_names, dates=None):
        if not isinstance(milestone_names, list):
            milestone_names = [milestone_names]
        results = []
        for q in milestone_names:
            url = "https://api.app.shortcut.com/api/v3/search/stories"
            params = {"query": f"""milestone:"{q}" """, "page_size": 25}
            while url:
                print(url, params)
                response = requests.get(
                    url, headers={"Shortcut-Token": SHORTCUT_API_TOKEN}, params=params
                ).json()
                data = response.get("data", [])
                next_token = response.get("next")
                url = SHORTCUT_HOST + next_token if next_token else None
                results += data
                params = {}

        stories = pd.DataFrame(results)
        return stories

    def get_one_platform_story_details(self, groups=ONE_groups):

        member_emails = dict(self.get_members()["email_address"].reset_index().values)

        epics = self.get_epics(groups=groups)[EPIC_MILESTONE_FIELDS]
        milestones = list(epics["name_milestone"].dropna().unique())

        spokes = self.get_labelled_stories(label="spoke")
        spokes["res_story_type"] = "spoke"
        res.utils.logger.info(f"spoke stories: {len(spokes)}")
        group_stories = self.get_milestone_stories(milestone_names=list(milestones))
        group_stories["res_story_type"] = "reFrame"
        res.utils.logger.info(f"reframe stories: {len(group_stories)}")
        data = pd.concat([group_stories, spokes])[STORY_FIELDS + ["res_story_type"]]

        # data = owner_emails(data,members)

        data = pd.merge(
            data,
            epics,
            left_on="epic_id",
            right_on="id",
            how="left",
            suffixes=["", "_epic"],
        )
        data["iteration_id"] = data["iteration_id"].map(
            lambda s: str(int(s)) if pd.notnull(s) else None
        )
        data["owner_emails"] = data["owner_ids"].map(
            lambda o: [member_emails.get(i) for i in (o or [])]
        )

        iterations = self.get_iterations()[["id", "app_url", "name"]]
        iterations["id"] = iterations["id"].map(str)

        data = pd.merge(
            data,
            iterations,
            left_on="iteration_id",
            right_on="id",
            how="left",
            suffixes=["", "_iteration"],
        )

        data["has_estimate"] = data["estimate"].map(lambda x: pd.notnull(x)).astype(int)

        return data.drop(["group_id", "epic_id", "owner_ids"], 1)

    def get_one_platform_reframe_status(self, slack_it=True):
        def send_reframe_status(status):
            p = {
                "slack_channels": ["one-platform"],
                "message": "reFrames status",
                "attachments": [
                    {
                        "title": "ONE Platform reframes",
                        "title_link": "https://airtable.com/app70YvSyxW4d0vJ3/tblSyArc4vdTnIDz9/viwSmK2xmtpT2hbtS?blocks=hide",
                        "stats_table": status,
                    }
                ],
            }

            slack = res.connectors.load("slack")

            return slack(p)

        d = self.get_one_platform_story_details()
        chk = d[
            [
                "iteration_id",
                "name_iteration",
                "res_story_type",
                "estimate",
                "has_estimate",
                "completed",
                "name_milestone",
            ]
        ].rename(columns={"name_milestone": "reFrame"})
        chk["estimate_"] = chk["estimate"].fillna(3)
        chk["completed_points"] = 0
        chk.loc[chk["completed"], "completed_points"] = chk["estimate_"]
        stats = chk.groupby("reFrame").agg(
            {
                "res_story_type": "count",
                "completed_points": sum,
                "has_estimate": sum,
                "estimate_": sum,
                "estimate": sum,
            }
        )
        stats["pct_complete"] = stats["completed_points"] / stats["estimate_"]
        stats["pct_has_estimate"] = stats["has_estimate"] / stats["res_story_type"]

        stats = dataframes.rename_and_whitelist(
            stats,
            columns={
                "res_story_type": "Story Count",
                "completed_points": "Points Completed",
                "estimate": "Estiamted Points",
                "pct_complete": "Perecentage Complete",
                "pct_has_estimate": "Percentrage estimated stories",
            },
        )

        if slack_it:
            send_reframe_status(stats)

        return stats
