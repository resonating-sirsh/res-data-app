"""
wrapper to do some templating to send slack messages

Formatting see: https://api.slack.com/reference/surfaces/formatting

python sdk https://github.com/slackapi/python-slack-sdk, https://api.slack.com/start/building/bolt-python#install


file uploader: https://www.youtube.com/watch?v=XQO98akt2jI <- or others
"""

import boto3
import res
import json
import pandas as pd
import slack_sdk
import os
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import time
from tenacity import retry, wait_fixed, stop_after_attempt
import re
from dateparser import parse
from datetime import datetime
from res.utils.secrets import secrets
import typing
from typing import Union, List
from res.connectors.s3 import clean_filename, fetch_to_s3
from pydantic import BaseModel
import typing


# thread id / t
SLACK_FILES_ROOT = (
    f"s3://res-data-platform/slack/files"  # channel_id/ts/file_name_hash.ext
)


class SlackFiles(BaseModel):
    name: str
    title: str
    url_private_download: str
    # size may want to filter by size

    @property
    def clean_name(self):
        # experimenting with attachments
        return clean_filename(self.name)


class SlackThread(BaseModel):
    ts: str
    text: str
    files: typing.List[SlackFiles] = []
    user: str

    def copy_files(self, channel_id):
        s3 = res.connectors.load("s3")
        files = [
            f"{SLACK_FILES_ROOT}/{channel_id}/{self.ts.replace('.','_')}/{f.clean_name}"
            for f in self.files
        ]
        for i, u in enumerate(self.files):
            f = files[i]
            if not s3.exists(f):
                fetch_to_s3(
                    u.url_private_download,
                    f,
                    token=os.environ.get("SLACK_ASK_ONE_BOT_TOKEN"),
                )
        return files

    def format(self, channel_id):
        files = self.copy_files(channel_id=channel_id)
        data = {
            "timestamp": parse(self.ts).isoformat(),
            "user": f"<@{self.user}>",
            "files": files,
            "text": self.text,
        }
        return data


class SlackMessage(BaseModel):
    channel_id: str
    ts: str
    text: str
    reply_count: int = 0
    reply_users_count: int = 0
    user: str
    files: typing.List[SlackFiles] = []
    replies: typing.List[SlackThread] = []

    def copy_files(self):
        s3 = res.connectors.load("s3")
        files = [
            f"{SLACK_FILES_ROOT}/{self.channel_id}/{self.ts.replace('.','_')}/{f.clean_name}"
            for f in self.files
        ]
        for i, u in enumerate(self.files):
            f = files[i]
            if not s3.exists(f):
                fetch_to_s3(
                    u.url_private_download,
                    f,
                    token=os.environ.get("SLACK_ASK_ONE_BOT_TOKEN"),
                )
        return files

    def format(self, user_mapping_function=None, filter_join_messages=True):
        """

        Just return the conversation with formatted user, timestamp and references to s3 files
        """
        files = self.copy_files()
        data = [
            {
                "timestamp": parse(self.ts).isoformat(),
                "text": self.text,
                "user": f"<@{self.user}>",
                "files": files,
            }
        ]
        data += [t.format(channel_id=self.channel_id) for t in self.replies]

        if user_mapping_function:
            for item in data:
                # this is to replace user ids with names
                item["text"] = user_mapping_function(item["text"])
                item["user"] = user_mapping_function(item["user"])

        return data


def get_s3():
    return res.connectors.load("s3")


def get_example_payload():
    return {
        "slack_channels": ["meta-one-status-updates"],
        "message": "*Hey there* - `some stuff` and ```some code```",
        "attachments": [
            {
                "title": "Evolution through time of issues in late orders",
                "title_link": "share_url",
                "image_url": get_s3().generate_presigned_url(
                    "s3://res-data-platform/samples/images/body_pieces/cc-6001/CC-6001-V8-BELT_SM_000.png"
                ),
            }
        ],
    }


class SlackConnector:
    def __init__(self, **options):
        self._client = boto3.client(
            "lambda",
            # aws_access_kopey_id=os.environ.get("aws_key"),
            # aws_secret_access_key=os.environ.get("aws_secret_key"),
            region_name="us-east-1",
        )

        for k in [
            "SLACK_ASK_ONE_APP_TOKEN",
            "SLACK_ASK_ONE_SIGNING_SECRET",
            "SLACK_ASK_ONE_BOT_TOKEN",
        ]:
            if k not in os.environ:
                secrets.get_secret(k)

        token = os.environ.get("SLACK_ASK_ONE_BOT_TOKEN")

        self._webclient = WebClient(token=token)
        self._channels = None
        self._users = None

    def try_typing(self, channel):
        if channel:
            pass
            # TODO quickly tried a bunch of things on the api but not sure yet how to do this

    def post_message(
        self,
        message: str,
        channel: str,
        thread_ts: str = None,
        use_markdown=False,
    ):
        self._webclient.chat_postMessage(
            channel=channel, thread_ts=thread_ts, text=message, mrkdwn=use_markdown
        )

    def post_file(
        self,
        data: bytes,
        filename: str,
        channel: str,
        thread_ts: str = None,
        initial_comment: str = None,
    ):
        """
        post raw data into a file to a channel and a thread if known
        """
        # do the slack thing
        self._webclient.files_upload(
            channels=[channel],
            file=data,
            filename=filename,
            thread_ts=thread_ts,
            initial_comment=initial_comment,
        )

    def post_files(
        self,
        files: list,
        channel: str,
        thread_ts=None,
        initial_comment=None,
    ):
        """
        files can look like this:
        [
            {
                "file": base64.b64decode(b64img.split(",")[1]),
                "filename": "buttonhole.png",
            }
        ],

        """
        # To upload multiple files at a time
        self._webclient.files_upload_v2(
            file_uploads=files,
            channel=channel,
            initial_comment=initial_comment,
            thread_ts=thread_ts,
        )

    def post_s3_files(
        self, files: Union[str, List[str]], channel: str, thread_ts: str = None
    ):
        """
        post a file to  a channel and thread if known
        assuming s3 files for now
        reads bytes and posts data based on the s3 file name

        **Args**
           files: for now a file or list of files. in future can support binary data also
           channel: the slack channel - could support broadcast
           thread_ts: this is a handle on a thread for replying in thread
        """
        if isinstance(files, str):
            files = [files]
        s3 = res.connectors.load("s3")
        for file in files:
            res.utils.logger.debug(f"Reading {file}")
            filename = file.split("/")[-1]
            data = s3.read_file_bytes(file)
            self.post_file(data, filename, channel, thread_ts)

    def __call__(self, payload):
        return self._send_payload(payload)

    def get_user_name_by_id(self, key):
        pass

    def get_channel_name_by_id(self, key):
        pass

    def get_user_id_by_name(self, key):
        pass

    def get_channel_id_by_name(self, key):
        pass

    def load_users(self):
        users = []
        cursor = None
        while True:
            a = self._webclient.users_list(cursor=cursor)
            users += a.data["members"]
            cursor = a.data["response_metadata"].get("next_cursor")
            if not cursor:
                break
        users = pd.DataFrame(users)
        users = users[["id", "name"]]
        return users

    def _get_user_map(self):
        if self._users is None:
            self._users = self.load_users()
        return dict(self._users[["id", "name"]].values)

    def _get_channels_map(self):
        if self._channels is None:
            self._channels = self.load_channels()
        return dict(self._channels[["id", "name"]].values)

    def replace_users_in_string(self, s):
        """
        from cached

        users = slack.load_users()
        users = dict(users.values)

        """

        # lazy load - introduce caching later
        U = self._get_user_map()

        def replace_tokens(match):
            user_id = match.group(1)
            return U.get(user_id, user_id)

        return re.sub(r"<@(U[0-9A-Z]+)>", replace_tokens, s)

    def load_channels(self):
        channels = []
        cursor = None
        while True:
            a = self._webclient.conversations_list(cursor=cursor)
            channels += a.data["channels"]
            cursor = a.data["response_metadata"].get("next_cursor")
            if not cursor:
                break
        channels = pd.DataFrame(channels)

        channels["description"] = channels["purpose"].map(lambda x: x.get("value"))
        channels = channels[["id", "name", "description", "num_members"]]
        return channels

    def message_channel(self, message, channel, use_markdown=False):
        self(
            {
                "slack_channels": [channel],
                # using markdown can be convenient for same basic formatting e.g. tables and code
                "message": message if use_markdown == False else f"```{message}```",
            }
        )

    # def getMessages(token, channelId):
    #     print("Getting Messages")
    #     # this function get all the messages from the slack team-search channel
    #     # it will only get all the messages from the team-search channel
    #     slack_url = "https://slack.com/api/conversations.history?token=" + token + "&channel=" + channelId
    #     messages = requests.get(slack_url).json()
    #     return messages

    # def getChannels(token):
    #     '''
    #     function returns an object containing a object containing all the
    #     channels in a given workspace
    #     '''
    #     channelsURL = "https://slack.com/api/conversations.list?token=%s" % token
    #     channelList = requests.get(channelsURL).json()["channels"] # an array of channels
    #     channels = {}
    #     # putting the channels and their ids into a dictonary
    #     for channel in channelList:
    #         channels[channel["name"]] = channel["id"]
    #     return {"channels": channels}

    # def getUsers(token):
    #     # this function get a list of users in workplace including bots
    #     users = []
    #     channelsURL = "https://slack.com/api/users.list?token=%s&pretty=1" % token
    #     members = requests.get(channelsURL).json()["members"]
    #     return members

    def _send_payload(self, payload):
        def treat(a):
            """
            special custom overrides
            """
            if "stats_table" in a:
                s = a.pop("stats_table")
                # replace the dataframe table with an image of it on s3 (presigned)

                a["image_url"] = SlackConnector._stats_to_s3(s)
            return a

        if "attachments" in payload:
            payload["attachments"] = [treat(a) for a in payload["attachments"]]

        payload = json.dumps(payload)

        res.utils.logger.debug("Sending Slack notifications...")
        self._client.invoke(
            FunctionName="notify_slack_channels",
            InvocationType="Event",
            Payload=payload,
        )

    def index_channel(self, name, index_type=None, since_date=None):
        """
        starting to create abstraction for indexing slack channels using llama_index
        We should run this as a sort of job when we learn how these things works - and efficiently maintain the index
        """
        from llama_index import GPTSimpleVectorIndex, SlackReader

        s = SlackReader()

        # name resolve for channel - can hard code  map for now
        name = "C04J4T719LG"
        s.earliest_date_timestamp = since_date
        slack_documents = s.load_data(channel_ids=[name])
        index_type = index_type or GPTSimpleVectorIndex
        index = index_type.from_documents(slack_documents)
        return index

    def get_processed_channel_messages(self, channel_id, since_date=None):
        a = self.read_channel_messages(
            channel_id, replace_user_ids=True, oldest=since_date
        )
        a["channel_name"] = a["channel_id"].map(
            lambda x: self._get_channels_map().get(channel_id)
        )
        return a[["thread", "id", "channel_name", "timestamp", "username"]]

    def process_all_messages(self, channel_id):
        """ """
        batch = self.process_messages(channel_id)
        yield batch
        while batch:
            oldest = batch[-1].ts
            batch = self.process_messages(channel_id, latest=oldest, inclusive=False)
            yield batch

    def process_messages(
        self,
        channel_id: str,
        oldest: str = None,
        latest: str = None,
        batch_size=10,
        inclusive=True,
    ):
        """
        slack processes messages in descending time but we can choose our windows
        """
        if isinstance(oldest, str):
            oldest = parse(oldest)

        if isinstance(latest, str):
            latest = parse(latest)

        oldest = datetime.timestamp(oldest) if oldest else None
        latest = datetime.timestamp(latest) if latest else None

        result = self._webclient.conversations_history(
            channel=channel_id,
            inclusive=inclusive,
            latest=latest,
            oldest=oldest,
            limit=batch_size,
        )

        return [
            self.process_message(m, channel_id=channel_id) for m in result["messages"]
        ]

    def process_message(self, m: dict, channel_id: str) -> typing.List[SlackMessage]:
        """
        cleaner message processor with pydantic will deprecate the others later
        """

        def message_replies(ms, max_replies=100):
            """
            todo we are not paging yet
            """
            if (ms.get("reply_count") or 0) > 0:
                return list(
                    self._webclient.conversations_replies(
                        channel=channel_id,
                        ts=ms["ts"],
                        limit=min(max_replies, ms["reply_count"]),
                    )["messages"]
                )
            return []

        # -> the thread is not inclusive, we add after the first message
        m["replies"] = message_replies(m)[1:]

        return SlackMessage(**m, channel_id=channel_id)

    def read_channel_messages(
        self,
        channel_id,
        latest=None,
        oldest=None,
        replace_user_ids=True,
        sleep_time=30,
    ):
        """
        read all messages in channel e.g. from channel_id = "C037R71B4JX"
        this uses a slack bot AskOne which must be added to the channel in this case
        """

        if isinstance(oldest, str):
            oldest = parse(oldest)

        if isinstance(latest, str):
            latest = parse(latest)

        oldest = datetime.timestamp(oldest) if oldest else None
        latest = datetime.timestamp(latest) if latest else None

        res.utils.logger.debug(f"Reading {channel_id} - since {oldest} to {latest}")

        # we can join if we have to
        try:
            self._webclient.conversations_join(channel=channel_id)
        except:
            pass

        @retry(wait=wait_fixed(10), stop=stop_after_attempt(3))
        def parse_replies(message):
            replies = []
            if message.get("reply_count"):
                for r in self._webclient.conversations_replies(
                    channel=channel_id, ts=message["ts"], limit=100, oldest=oldest
                ):
                    for m in r["messages"]:
                        if "user" in m:
                            replies.append((m["user"], m["text"]))
            return replies

        all_messages = []
        reply_map = {}
        try:
            res.utils.logger.info(f"reading slack threads - oldest {oldest}")
            result = self._webclient.conversations_history(
                channel=channel_id,
                inclusive=True,
                latest=latest,
                oldest=oldest,
                limit=100,
            )

            for message in result["messages"]:
                ###
                replies = parse_replies(message)
                if replies:
                    reply_map[message["ts"]] = replies
                ###
            all_messages += result["messages"]
            ts_list = [item["ts"] for item in all_messages]
            last_ts = ts_list[:-1]

            while result["has_more"]:
                res.utils.logger.info(
                    f"reading another chunk of slack threads - sleeping for {sleep_time} seconds and doing further chunks of 100"
                )
                time.sleep(sleep_time)
                result = self._webclient.conversations_history(
                    channel=channel_id,
                    cursor=result["response_metadata"]["next_cursor"],
                    latest=last_ts,
                    oldest=oldest,
                    limit=100,
                )
                res.utils.logger.info("looking at replies for the messages")
                for message in result["messages"]:
                    ####
                    replies = parse_replies(message)
                    if replies:
                        reply_map[message["ts"]] = replies
                    ####
                all_messages += result["messages"]

            all_messages = pd.DataFrame(all_messages)
            all_messages["timestamp"] = pd.to_datetime(all_messages["ts"], unit="s")
            all_messages["replies"] = all_messages["ts"].map(lambda x: reply_map.get(x))
            all_messages["channel_id"] = channel_id

            all_messages["id"] = all_messages.apply(
                lambda row: res.utils.uuid_str_from_dict(
                    {"ts": str(row["ts"]), "channel": row["channel_id"]}
                ),
                axis=1,
            )

            def process_reply_text(s):
                def f(t):
                    return f"<@{t[0]}> said:> {t[1]}"

                try:
                    return "\n".join([f(t) for t in s])
                except:
                    return None

            all_messages["thread"] = all_messages["replies"].map(process_reply_text)
            all_messages["thread"] = all_messages["thread"].fillna(all_messages["text"])

            if replace_user_ids:
                all_messages["text"] = all_messages["text"].map(
                    self.replace_users_in_string
                )
                all_messages["thread"] = all_messages["thread"].map(
                    self.replace_users_in_string
                )

                all_messages["username"] = all_messages["user"].map(
                    lambda x: self._get_user_map().get(x)
                )

            return all_messages

        except SlackApiError as e:
            res.utils.logger.warn(
                f"Error processing channel {channel_id}: {e.response['error']}"
            )
            all_messages = pd.DataFrame(all_messages)
            all_messages["replies"] = all_messages["ts"].map(lambda x: reply_map.get(x))
            return all_messages

    def get_indexed_channel_table(self, channel_id):
        import lancedb

        LANCE_ROOT = "s3://res-data-platform/lancedb/sirsh/tables"
        db = lancedb.connect(LANCE_ROOT)
        table = db.open_table(f"{channel_id}".lower())

        return table

    def channel_vector_search(self, channel_id, text, top_n=3, min_distance=0.3):
        """
        this is a wip - we will probably factor out some fo the lance and open ai stuff but using slack as a way to test search
        example channel - are-we-flowing C045J9GM5K7
        """
        import openai

        def embed_func(c):
            rs = openai.Embedding.create(input=c, engine="text-embedding-ada-002")
            return [record["embedding"] for record in rs["data"]]

        table = self.get_indexed_channel_table(channel_id)
        emb = embed_func(text)[0]

        results = table.search(emb).limit(top_n).to_df()

        return results[results["score"] < min_distance]

    def index_slack_channel(
        self, channel_id, columns_to_index=["ts", "user", "text", "timestamp"]
    ):
        """
        this is a WIP method
        - we need smarter ways to deltas and tweak the embeddings etc
        - we will probably factor out some fo the lance and open ai stuff but using slack as a way to test search

        prune eg.. or use lance somehow
        for f in list(s3.ls('s3://res-data-platform/lancedb/sirsh/tables')):
            if 'c045j9gm5k7' in f:
                s3._delete_object(f)
        """
        import lancedb
        from tqdm import tqdm
        import openai

        from openai import OpenAI

        client = OpenAI()

        def embed_func(c):
            rs = client.embeddings.create(input=c, model="text-embedding-ada-002")

            return [record.embedding for record in rs.data]

        LANCE_ROOT = "s3://res-data-platform/lancedb/sirsh/tables"
        res.utils.logger.info(f"Reading messages from channel {channel_id}...")
        messages = self.read_channel_messages(channel_id)[columns_to_index]
        res.utils.logger.info(f"Read {len(messages)} messages...")
        # https://github.com/lancedb/lancedb/blob/main/notebooks/youtube_transcript_search.ipynb
        db = lancedb.connect(LANCE_ROOT)
        res.utils.logger.info("Building embeddings")
        embeddings = []
        for text in tqdm(messages["text"]):
            embeddings.append(embed_func(text))

        messages["vector"] = embeddings
        messages["vector"] = messages["vector"].map(lambda x: x[0])
        messages = messages.dropna()

        res.utils.logger.info(
            f"Creating table {LANCE_ROOT}/{channel_id.lower()} with {len(messages)} items with non null vectors"
        )
        # todo check it exists and add
        table = db.create_table(f"{channel_id}".lower(), messages)
        return table

    @staticmethod
    def _stats_to_s3(data):
        import seaborn as sns
        from matplotlib import pyplot as plt
        import io
        import boto3

        plt.figure(figsize=(20, 10))
        sns.set(font_scale=2)
        sns.set_style("whitegrid")
        _ = sns.heatmap(data, annot=True, cmap="Blues", cbar=False, fmt=".3g")
        buf = io.BytesIO()
        plt.savefig(buf, format="png", bbox_inches="tight")

        buf.seek(0)
        bucket = boto3.resource("s3").Bucket("res-data-platform")
        key = f"temp-images/{res.utils.res_hash()}.png"
        path = f"s3://res-data-platform/{key}"
        res.utils.logger.debug(f"file to {path}")
        bucket.put_object(Body=buf, ContentType="image/png", Key=key)

        return res.connectors.load("s3").generate_presigned_url(path, expiry=36000)

    # connector workflows or message templates
    def queue_notification(
        self, user, status, queue_name, queue_url, preview_image=None, **kwargs
    ):
        pass

    def post_stats_table_notification(self, data, **kwargs):
        pass

    def queue_notification_aggregate_daily(
        self, user, status, queue_name, queue_url, preview_image=None, **kwargs
    ):
        pass


def ingest_slack_channel(channel, since_date=None, slack=None):
    from res.observability.io import VectorDataStore
    from res.observability.entity import SlackMessageVectorStoreEntry

    slack = slack or res.connectors.load("slack")
    lu = dict(slack.load_channels()[["name", "id"]].values)
    lu["make_one_team"] = "C04J4T719LG"
    lu["design-to-make"] = "C05V1NS615M"
    lu["sew-development-dr"] = "C0694AA6PEE"
    data = SlackMessageVectorStoreEntry.collection_batch_from_reader(
        channel_id=lu[channel], since_date=since_date
    )

    res.utils.logger.info(f"Creating slack vector store {channel}")
    store = VectorDataStore(
        SlackMessageVectorStoreEntry.create_model(channel, namespace="slack"),
        create_if_not_found=True,
    )
    store.add(data)

    return store


def ingest_slack_channels(date=None, relative_back=2):
    if date == "*":
        date = None
    elif date is None:
        date = str(res.utils.dates.relative_to_now(relative_back))
        res.utils.logger.info(f"Using the default date {date}")

    from res.observability.io import list_stores
    import pandas as pd
    import traceback
    import time

    slack = res.connectors.load("slack")

    df = pd.DataFrame(list_stores())
    df = df[df["namespace"] == "slack"]
    slack_channels = df["name"].unique()

    for channel in slack_channels[::-1]:
        res.utils.logger.info(channel)
        try:
            res.utils.logger.metric_node_state_transition_incr(
                "SlackObservability", channel, "ENTERED"
            )
            st = ingest_slack_channel(channel, since_date=date, slack=slack)
            res.utils.logger.metric_node_state_transition_incr(
                "SlackObservability", channel, "EXITED"
            )
        except:
            res.utils.logger.metric_node_state_transition_incr(
                "SlackObservability", channel, "FAILED"
            )
            res.utils.logger.warn(
                f"Error ingesting channel {channel}:> {traceback.format_exc()}"
            )
        # the long sleep
        res.utils.logger.info("Sleeping...")
        time.sleep(30)


def explain_slack_message(message):
    """
    experimental method to extract relationships from slack messages using LLM
    this could be used to save rels in a graph database
    """
    from res.learn.agents.builder.utils import ask

    P = f"""Please summarize the discussion in the data and determine what users and other entities are involved. Use the response model below
        In all cases entities and related entities can be determined from the entity store
       
    **Response
    ```
    class MessageReferences(BaseModel):
        summary: str
        users_in_conversation: List[str]
        #the body code has the format AB-1234 and you will find
        bodies_referenced: List[str]
        #numeric production request numbers
        ONEs_referenced: List[str]
        #four part codes with style codes and sizes; these must have the format BODY MATERIAL COLOR SIZE and be given in the entity store as a sku e.g. related to a ONE
        SKUs_referenced: List[str]
        #Three part codes that must be of the form BODY MATERIAL COLOR 
        style_codes_referenced: List[str]
        order_numbers_referenced: List[str]
        #links to s3 files 
        files_referenced: List[str]
        #any non s3 links e.g. to airtable, coda, looker
        hyperlinks: List[str]
        timestamp: str

    ```

    **Data**
    ```json
    {json.dumps(message)}
    ```

    """
    d = json.loads(ask(P, as_json=True))

    #     def cb(b):
    #         return f"-".join(b.split('-')[:2])
    #     d['bodies_referenced'] = [cb(b) for b in d['bodies_referenced'] ]

    return d
