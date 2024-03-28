from http import HTTPStatus
from uuid import UUID
from res.utils import logger
from fastapi import APIRouter
from fastapi_utils.cbv import cbv
from res.flows.dxa import bertha
from fastapi.responses import StreamingResponse
from fastapi import HTTPException
from requests.models import Response
import io


def get_bertha_routes() -> APIRouter:
    router = APIRouter()

    @cbv(router)
    class _Router:
        @router.get(
            "/bertha/status",
            name="status",
        )
        def get_bertha_status(self, from_str=None, hours=24) -> dict:
            """
            Get Bertha's job status chart

            You can pass in from_str as a UTC timestamp and hours as an integer to
            get a chart from a specific time
            """
            logger.info(f"get_bertha_status")

            if hours and type(hours) == str:
                hours = int(hours)

            result = bertha.get_base64_status_chart(from_str=from_str, hours=hours)

            return (
                StreamingResponse(result, media_type="image/png")
                if result
                else "no jobs found"
            )

        @router.post(
            "/bertha/create_swatch",
            name="create swatch",
        )
        def create_swatch(
            self,
            materials: str = "COMCT, WCTNS",
            colors: str = "NATUNM",
        ) -> bool:
            """
            Create a swatch for the given materials and colors for 1-15 yards

            `materials` and `colors` can be a comma or space separated list of values.

            For each material and color, a swatch will be created for each yarage from 1-15
            using the CC-9066-V1 body. A style, apply color request and apply dynamic color
            job will be initiated.
            """

            def split_list(s):
                for delimiter in [",", " "]:
                    s = s.replace(delimiter, ",")
                return [x.strip() for x in s.split(",") if x.strip()]

            all_yards = [f"0Y{i:03}" for i in range(1, 16)]
            bertha.add_swatch_style(
                "CC-9066-V1",
                material_codes=split_list(materials),
                color_codes=split_list(colors),
                size_codes=all_yards,
            )

            return True

        @router.post(
            "/bertha/redo_apply_color_requests_new_flow",
            name="redo apply color requests new flow",
        )
        def redo_apply_color_requests_new_flow(
            self,
            skus: list[str] = [],
            test=True,
        ) -> dict:
            """
            ***This is for platform maintenance only***

            ***remember test is True by default***

            Generate images for default color requests (all over print) using the new
            apply_dynamic_color flow (i.e. not using VStitcher/Bertha (ironically?)).

            You can pass in a list of requests which can be any of these formats:

            style sku e.g.: `["TT-3072 CTNBA YELLXY"]`

            sku+size e.g.: `["TT-3072 CTNBA YELLXY 3ZZMD"]`

            sku+apply_color_request_index e.g.: `["TT-3072 CTNBA YELLXY v1"]`
            """
            logger.info(f"redo_apply_color_requests_new_flow {skus}")

            test = str(test).lower() in ["true", "1", "t", "y", "yes"]

            result = bertha.post_apply_dynamic_color(skus, test=test)
            if test:
                return result

            # check if they are all dictionaries with a success HTTP status code
            failed = [
                (k, r.reason if isinstance(r, Response) else r)
                for k, r in result.items()
                if not isinstance(r, Response) or r.status_code != HTTPStatus.OK
            ]
            if failed:
                raise HTTPException(
                    status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
                    detail=f"These requests failed: {failed}",
                )

            return (
                {k: v.reason for k, v in result.items()} if result else "no jobs redone"
            )

        @router.post(
            "/bertha/check_for_buttons",
            name="check for buttons",
        )
        def check_for_buttons(
            self,
            sku_glob: str = "CC-3001 XXXXX MARBLY",
            body_version: int = None,
            piece: str = None,
            first: int = 100,
            slack_channel: str = None,  # U03Q8PPFARG @John, C06DVEE12JV #inspector_gpt4vet
        ) -> dict:
            """
            Check a style/piece for buttons using GPT4V.

            The sku_glob is a string that can be any of these formats (the material doesn't matter so it can be anything)):

            ```
                CC-3001 XXXXX MARBLY        # look at the minimum size pieces
                CC-3001 XXXXX MARBLY 1ZZXS  # look at the 1ZZXS size pieces
                CC-3001 XXXXX *             # look at all colors
                CC-3001 XXXXX MARBLY *      # look at all the sizes for a color
            ```

            if the `body_verion` is not specified, the latest will be used. For CC-3001, v9 had some buttonholes

            `piece` can be used to filter the results to a specific pieces e.g. `HD` to match part of the piece name

            `slack_channel` can be used to post pieces it suspects has buttons to a slack channel (`C06DVEE12JV` for
            `#inspector_gpt4vet`)

            it can take a while to run
            """

            from res.flows.dxa.inspectors.buttongate import check_for_buttons

            return check_for_buttons(
                sku_glob,
                body_version=body_version,
                piece=piece,
                slack_channel=slack_channel,
                top_pieces=first,
                max_pieces=0,
            )

        @router.post(
            "/bertha/inspect_grading",
            name="inspect grading",
        )
        def inspect_grading(
            self,
            body_code: str = "CC-2065",
            body_version: int = 9,
            slack_channel: str = None,  # U03Q8PPFARG @John, C06DVEE12JV #inspector_gpt4vet
        ) -> dict:
            """
            Check a body for potential grading issues using GPT4V.

            `slack_channel` can be used to post suspect pieces to a slack channel (`C06DVEE12JV` for
            `#inspector_gpt4vet`)

            it can take a while to run!
            """

            from res.flows.dxa.inspectors.grading import inspect_grading

            return inspect_grading(
                body_code=body_code,
                body_version=body_version,
                slack_channel=slack_channel,
            )

        @router.post(
            "/bertha/inspect_simulation",
            name="inspect simulation",
        )
        def inspect_simulation(
            self,
            body_code: str = "TH-1002",
            body_version: int = 7,
            color_code: str = None,
            slack_channel: str = None,  # U03Q8PPFARG @John, C06DVEE12JV #inspector_gpt4vet
        ) -> dict:
            """
            Check a body for potential simulation issues using GPT4V.

            `slack_channel` can be used to post suspect pieces to a slack channel (`C06DVEE12JV` for
            `#inspector_gpt4vet`)

            it can take a while to run!
            """

            from res.flows.dxa.inspectors.simulation import inspect_simulation

            return inspect_simulation(
                body_code=body_code,
                body_version=body_version,
                color_code=color_code,
                slack_channel=slack_channel,
            )

        @router.post(
            "/bertha/inspect_tileability",
            name="inspect tileablility",
        )
        def inspect_tileability(
            self,
            s3_path: str = None,
            batch: int = None,
            return_result_as_image: bool = False,
            debug: bool = False,
        ):
            """
            Check artworks to see if they tile well

            `s3_path` is the path to the image in S3, this will check a single image.

            `batch` is number of artworks to check, this will check multiple images
            that do not already have the `tileable` attribute in the `metadata` field
            and save the results to the database.

            `return_result_as_image` will return the result as an image if True. This
            only applies to the single image check.

            At the time of writing:

            `s3://meta-one-assets-prod/artworks/jr/c39de00637cef5d47dca2cd5343c9e3a_300.png`
            doesn't tile well

            `s3://meta-one-assets-prod/artworks/rm/83fe1e6780d2a4000c44642bd3b917d5_300.png`
            tiles well

            """

            from res.flows.dxa.inspectors.tiling import inspect_tileability

            result = inspect_tileability(
                s3_path=s3_path,
                batch=batch,
                return_result_as_image=return_result_as_image,
                debug=debug,
            )

            if s3_path and return_result_as_image:
                bytes = io.BytesIO()
                result.save(bytes, format="PNG")
                result.close()
                bytes.seek(0)

                return StreamingResponse(bytes, media_type="image/png")

            return result

    return router


router = get_bertha_routes()
