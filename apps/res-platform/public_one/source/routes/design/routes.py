from res.utils import logger
from fastapi import APIRouter, Path, status, Query
from fastapi_utils.cbv import cbv
import typing
from pydantic import BaseModel, Field
from . import get_current_token, Security, determine_brand_context, ping_slack_sirsh
from res.flows.meta.ONE.body_node import BodyMetaOneNode
from res.flows.meta.ONE.meta_one import MetaOne, BodyMetaOne
from res.flows.meta.ONE.style_node import MetaOneNode
from fastapi import HTTPException
from schemas.pydantic.meta import BodyMetaOneStatus, BodyMetaOneResponse
import res
import res.flows.dxa.design as design_handlers
import traceback
from res.connectors.redis import EndPointKeyCache


class BrandStyle(BaseModel):
    name: str = None
    types: typing.List[str] = None


def catch_known_exceptions(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except design_handlers.DesignException as e:
            logger.warn(f"DesignException: {e}")
            raise HTTPException(
                status_code=400,
                detail={
                    "error_code": e.error_code,
                    "error_message": e.message,
                    "error_detail": e.detail,
                },
            )
        except Exception as e:
            logger.error(f"Unknown error: {e}")
            ping_slack_sirsh(f"Public ONE {traceback.format_exc()} ")
            raise HTTPException(
                status_code=500,
                detail={
                    "error_code": "UNKNOWN_ERROR",
                    "error_message": str(e),
                },
            )

    return wrapper


get_artworks = catch_known_exceptions(design_handlers.get_artworks)
create_directional = catch_known_exceptions(design_handlers.create_directional_via_c1)


def get_design_routes() -> APIRouter:
    router = APIRouter()

    @cbv(router)
    class _Router:
        @router.get(
            "/design/bodies",
            name="list all bodies for the brand",
            status_code=status.HTTP_200_OK,
        )
        def search_bodies(
            self,
            token: str = Security(get_current_token),
        ) -> typing.List[dict]:
            """
            ***

            List your brand bodies. Bodies are the base garments on which you place color and choose materials and trims to complete your styles

            ***

            """

            bc = determine_brand_context(token)
            if bc:
                return BodyMetaOneNode.list_brand_bodies(
                    brand_code=bc,
                    sign_link_fields=["front_image_uri"],
                )
            else:
                return HTTPException(
                    status_code=401,
                    detail="Not Authorized to look at assets outside of brand",
                )

        @router.get(
            f"/design/body",
            response_model=BodyMetaOneResponse,
            name="get the body by code and version",
            status_code=200,
        )
        def get_body_by_code(
            self,
            bodyCode: str = Query(..., example="TT-1234", description="The body code"),
            bodyVersion: typing.Optional[int] = None,
            token: str = Security(get_current_token),
        ) -> dict:
            """
            ***
            - Determines the details of the body.
            - Some bodies may not be ready yet and a contract pending status of `BODY_ONE_READY` will be shown indicating that we are awaiting that contract to pass.
            - The body version is optional and by default the active body version will be fetched.
            ***

            """
            # todo infer body version

            if bodyCode[:2] != determine_brand_context(token):
                return HTTPException(
                    status_code=401,
                    detail="Not Authorized to look at assets outside of brand",
                )

            m1 = BodyMetaOne.for_sample_size(
                body_code=bodyCode, body_version=bodyVersion
            )
            try:
                return m1.status_at_node()
            except:
                ping_slack_sirsh(f"Public ONE /design/body - {traceback.format_exc()} ")
                res.utils.logger.warn(
                    f"Could not load the full status but we have {m1}"
                )
                # this is a different schema - to do clean this up - if the body is not ready
                m1["error_status"] = "This body is not fully on-boarded yet"
                m1["contracts_failed"] = ["BODY_ONE_READY"]
                return m1

        @router.get(
            f"/design/bodies/status",
            response_model=BodyMetaOneStatus,
            status_code=200,
            name="get the development status for the body",
        )
        def get_body_status(
            self,
            bodyCode: str = Query(..., example="TT-1234", description="The body code"),
            bodyVersion: typing.Optional[int] = None,
            token: str = Security(get_current_token),
        ) -> dict:
            """
            ***
            - Determines the development status of the body.
            - Some bodies may not be ready yet and a contract pending status of `BODY_ONE_READY` will be shown indicating that we are awaiting that contract to pass.
            - The body version is optional and by default the active body version will be fetched.
            ***
            """

            if bodyCode[:2] != determine_brand_context(token):
                return HTTPException(
                    status_code=401,
                    detail="Not Authorized to look at assets outside of brand",
                )

            return BodyMetaOneNode.get_body_status(bodyCode, bodyVersion)

        @router.get(
            f"/design/bodies/points_of_measure",
            name="get the points of measurement for the body (by version)",
            status_code=200,
        )
        def get_body_points_of_measure(
            self,
            bodyCode: str = Query(..., example="TT-1234", description="The body code"),
            bodyVersion: typing.Optional[int] = None,
            token: str = Security(get_current_token),
        ) -> dict:
            """
            ***
            - Provides the points of measures for a body.  These are the critical measurements (in units: cm) along key points in the body such as the length across the chest etc.
            - The measurements are provided for each size that the body is made in.
            - "Petite" versions of each of the size-specific measurements may be given in some cases and prefixed with `P_`
            ***
            """
            from res.flows.meta.ONE.meta_one import BodyMetaOne
            from res.flows.meta.bodies import get_body_asset_status

            if bodyCode[:2] != determine_brand_context(token):
                return HTTPException(
                    status_code=401,
                    detail="Not Authorized to look at bodies outside of brand",
                )

            try:
                if not bodyVersion:
                    status = get_body_asset_status(body_code=bodyCode)
                    bodyVersion = status["body_version"]
                m = BodyMetaOne.for_sample_size(
                    body_code=bodyCode, body_version=bodyVersion
                )
                poms = m.get_points_of_measure()
                return {
                    "body_code": bodyCode,
                    "body_version": bodyVersion,
                    "points_of_measure": poms,
                }
            except Exception as e:
                ping_slack_sirsh(
                    f"Public ONE /design/bodies/points_of_measure - {traceback.format_exc()} "
                )
                raise HTTPException(
                    status_code=500,
                    detail=str(e),
                )

        #######
        ##          STYLES
        #######

        @router.get(
            "/design/styles",
            name="list all styles for the brand",
            status_code=200,
        )
        def search_styles(
            self,
            brandCode: str = Query(..., example="TT", description="Your brand code"),
            bodyCode: str = Query(
                ..., example="TT-1234", description="Filter by body code"
            ),
            token: str = Security(get_current_token),
        ) -> typing.List[dict]:
            """
            ***
            - List brand styles
            - Optionally filter by the body to show only styles based on that body
            ***


            """

            brandCode = brandCode or determine_brand_context(token)
            return MetaOneNode.list_brand_styles(
                brand_code=brandCode,
                body_code=bodyCode,
            )

        @router.get(
            "/design/style",
            name="get style by style code",
            status_code=200,
        )
        def get_style(
            self,
            styleCode: str,
            token: str = Security(get_current_token),
        ) -> dict:
            """
            ***
            - Fetch a meta one style size detail by four part style BODY MATERIAL COLOR SIZE_CODE using this example format `TT-3072 CTNBA NIKIDU 2ZZSM` which is the body, material, color and size of the SKU
            - We pre-sign the s3 URL and this will time out in 30 minutes
            - If the style code entered is the BMC code `TT-3072 CTNBA NIKIDU` only, then the sample size will be returned
            ***

            """

            if styleCode[:2] != determine_brand_context(token):
                return HTTPException(
                    status_code=401,
                    detail="Not Authorized to look at assets outside of brand",
                )

            res.utils.logger.info(f"Sku {styleCode}")
            try:
                s3 = res.connectors.load("s3")
                if len(styleCode.split(" ")) == 3:
                    a = MetaOne.for_sample_size(styleCode)
                else:
                    a = MetaOne.safe_load(styleCode)
                pcs = a._data[
                    [
                        "key",
                        "uri",
                        "piece_set_size",
                        "piece_ordinal",
                        "normed_size",
                        "body_piece_type",
                        "style_sizes_size_code",
                        "garment_piece_material_code",
                    ]
                ].rename(columns={"style_sizes_size_code": "size_code"})
                pcs["meta_key"] = pcs["key"].map(lambda x: f"-".join(x.split("-")[-2:]))
                # add a presigned url with the default timeout for the caller to add things to airtable or whatever
                pcs["presigned_uri"] = pcs["uri"].map(s3.generate_presigned_url)
                return {
                    "styleCode": styleCode,
                    "stylePieces": pcs.to_dict("records"),
                }
            except:
                ping_slack_sirsh(
                    f"Public ONE /design/style - {traceback.format_exc()} "
                )

                res.utils.logger.warn(traceback.format_exc())
                return {}

        @router.get(
            f"/design/style/status/",
            status_code=200,
            name="get the development status of the style",
        )
        def get_full_style_status(
            self,
            styleCode: str = Query(
                ..., example="TT-3072 CTNBA NIKIDU", description="The BMC style code"
            ),
            bodyVersion: typing.Optional[int] = None,
            # brand_code: str = None,
            # includeFullStatus: bool = True,
            # includeCost: bool = False,
            token: str = Security(get_current_token),
        ) -> dict:
            """
            ***
            Get the development status of a style  If the body version is not supplied, the active version is used.
            ***
            """
            if styleCode[:2] != determine_brand_context(token):
                return HTTPException(
                    status_code=401,
                    detail="Not Authorized to look at assets outside of brand",
                )

            try:
                return MetaOneNode.get_full_style_status(
                    style_code=styleCode,
                    body_version=bodyVersion,
                    brand_code=determine_brand_context(token),
                    include_full_status=True,
                    include_cost=False,
                )
            except Exception as ex:
                ping_slack_sirsh(
                    f"Public ONE /design/style/status/- {traceback.format_exc()} "
                )
                raise HTTPException(
                    500,
                    f"Could not handle the request for {styleCode} - {ex}",
                )

        @router.get(f"/design/artworks")
        def get_artworks(
            self,
            brandCode: str = None,
            name: typing.Optional[str] = None,
            colorCode: typing.Optional[str] = None,
            signUris: typing.Optional[bool] = True,
            limit: typing.Optional[int] = 10,
            offset: typing.Optional[int] = 0,
            maxDpi: typing.Optional[int] = None,
            token: str = Security(get_current_token),
        ) -> typing.List[dict]:
            """
            ***
            Get an artwork. Artworks are used to apply color to bodies when creating styles. Add artworks to your library to allow for styles to created in various colors.
            ***

            """
            bc = determine_brand_context(token)
            brandCode = brandCode or bc
            if brandCode[:2] != bc:
                return HTTPException(
                    "Not Authorized to look at assets outside of brand TT"
                )

            logger.info(f"get_artworks   {brandCode} {name} {colorCode}")
            result = get_artworks(
                name=name,
                brand=brandCode,
                color_code=colorCode,
                id=None,
                limit=limit,
                offset=offset,
                sign_uris=signUris,
                max_dpi=maxDpi,
            )
            if result:
                return result
            else:
                raise HTTPException(404, "Artwork not found")

        @router.post(f"/style/directional", name="Create Directional Style")
        def add_directional(
            self,
            bodyVersion: int,
            imageUri: str,
            imageName: str,
            brandCode: str = Query(..., example="TT", description="The brand code"),
            bodyCode: str = Query(
                ..., example="TT-3072", description="The body code for the new style"
            ),
            materialCode: str = Query(
                ...,
                example="COMCT",
                description="The material/fabric for the new style",
            ),
            colorCode: str = Query(
                ..., example="APISTY", description="The color code for the new style"
            ),
        ):
            """
            ***
            Create a style using a repeat artwork (no custom placement). These styles are sometimes referred to as _directional_
            ***

            """
            return create_directional(
                bodyCode,
                materialCode,
                colorCode,
                bodyVersion,
                brandCode,
                imageUri,
                imageName,
                c1_action="upsert",
            )

        @router.get(f"/style/costs", status_code=200)
        def get_meta_one_costs_table(
            self,
            sku: str = Query(
                ...,
                example="TT-3072 CTNBA NIKIDU 2ZZSM",
                description="The BMC+Size SKU as sold on e-commerce",
            ),
            token: str = Security(get_current_token),
        ) -> typing.List[dict]:
            """
            ***
            Fetch costs table for a sku using in BMC format - this is the three part SKU plus the e-commerce size code
            ***

            """
            from res.flows.meta.ONE.queries import get_bms_style_costs_for_sku

            if sku[:2] != determine_brand_context(token):
                return HTTPException(
                    "Not Authorized to look at assets outside of brand"
                )

            # a record set is returned - the 0 index gives the dict - costs are a list of dicts... ...
            try:
                cache = EndPointKeyCache("/meta-one/costs-table", ttl_minutes=60)
                cached = cache[sku]
                if cached:
                    res.utils.logger.warn(
                        f"using cached value for item /meta-one/costs-table/{sku}"
                    )
                    return cached
                result = get_bms_style_costs_for_sku(sku)
                cache[sku] = result
                return result
            except:
                raise HTTPException(
                    404,
                    "Failed to get the costs table - please check the format of the sku you entered is correct",
                )

    return router


router = get_design_routes()
