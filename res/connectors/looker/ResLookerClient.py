import looker_sdk
import urllib, time, os
from looker_sdk import models
from res.utils import logger, secrets_client
from tenacity import retry, wait_fixed, stop_after_attempt

# https://github.com/llooker/pylookml


class ResLookerClient:
    """
    Client object for looker sdk
    """

    def __init__(
        self,
        process_name=os.getenv("RES_APP_NAME", "unlabeled"),
        environment="development",
    ):
        self.environment = environment
        self._process_name = process_name
        # Set looker client and secret env vars
        os.environ["LOOKERSDK_BASE_URL"] = "https://resonance.looker.com:19999"
        secrets_client.get_secret("LOOKERSDK_CLIENT_ID")
        secrets_client.get_secret("LOOKERSDK_CLIENT_SECRET")
        # Initialize client
        self.sdk = looker_sdk.init40()

    def get_dashboard_object(self, dashboard_id):
        try:
            dashboard = next(iter(self.sdk.search_dashboards(id=dashboard_id)), None)
            if not dashboard:
                msg = f'dashboard "{dashboard_id}" not found'
                logger.critical(msg)
                raise ValueError(msg)
            assert isinstance(dashboard, looker_sdk.sdk.api40.models.Dashboard)
            return dashboard
        except Exception as e:
            logger.critical(str(e))
            raise

    def sync_views_for_model(self, model_id, views, view_provider):
        """
        The view provider should provide connection info and schema metadata
        Views are added to a model if they do not exist or alterneed if they do
        """
        pass

    # Generates a PDF given a dashboard object
    @retry(wait=wait_fixed(20), stop=stop_after_attempt(3))
    def generate_pdf_from_dashboard(
        self, dashboard_id, style="tiled", width=545, height=842, filters=None
    ):
        """
        Generate a pdf from a dashboard by dashboard id (hard coded widths for now)
        The filters can be a url string or dictionary of filters
        The response is the binary representation of the pdf which can be saved to file/s3
        """

        dashboard = self.get_dashboard_object(dashboard_id)
        logger.info(f"Generating dashboard: {dashboard.id}, {dashboard.title}")
        assert dashboard.id
        task = self.sdk.create_dashboard_render_task(
            dashboard.id,
            "pdf",
            models.CreateDashboardRenderTask(
                dashboard_style=style,
                dashboard_filters=urllib.parse.urlencode(filters) if filters else None,
            ),
            width,
            height,
        )
        if not (task and task.id):
            msg = f'Could not create a render task for "{dashboard.title}"'
            logger.critical(msg)
            raise ValueError(msg)

        # poll the render task until it completes
        elapsed = 0.0
        delay = 0.5  # wait .5 seconds
        while True:
            logger.info("Polling for dashboard to be rendered...")
            poll = self.sdk.render_task(task.id)
            if poll.status == "failure":
                msg = f'Render failed for "{dashboard.title}"'
                logger.critical(msg)
                raise ValueError(msg)
            elif poll.status == "success":
                break

            time.sleep(delay)
            elapsed += delay
        result = self.sdk.render_task_results(task.id)
        logger.info(f"Render task completed in {elapsed} seconds")
        return result

    def update_queue(self, qtype, **kwargs):
        """
        For the Flow API, register a queue type
        - add views
        - update model (Flow API test folder)
        - add a dashboard for the real-time tables in pinot
        """
        from res.connectors.looker.lookml_generation import ResLookerProject

        p = ResLookerProject("res.Meta", "meta-one")

        p.update_queue(qtype)
