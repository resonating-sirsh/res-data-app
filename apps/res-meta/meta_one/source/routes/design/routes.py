import traceback
import uuid
from typing import List, Optional
from fastapi import APIRouter, HTTPException
from fastapi_utils.cbv import cbv
import schemas.pydantic.design as design_schemas
import traceback

import res
import res.flows.dxa.design as design_handlers
from res.utils import logger
from res.utils.error_codes import ErrorCodes
import res.flows.dxa.apply_dynamic_color as apply_dynamic_color

SECTION = "design"


def catch_known_exceptions(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except design_handlers.DesignException as e:
            logger.warn(f"DesignException: {traceback.format_exc()}")
            raise HTTPException(
                status_code=400,
                detail={
                    "error_code": e.error_code,
                    "error_message": e.message,
                    "error_detail": e.detail,
                },
            )
        except Exception as e:
            logger.error(f"Unknown error: {traceback.format_exc()}")
            raise HTTPException(
                status_code=500,
                detail={
                    "error_code": ErrorCodes.UNKNOWN_ERROR,
                    "error_message": str(e),
                },
            )

    return wrapper


# using catch_known_exceptions as a decorator below will mess up fastapi
# because it'll think args/kwargs are the parameters
# this way is ugly, but neater than doing the above try/except in each api
add_artwork = catch_known_exceptions(design_handlers.add_artwork)
update_artwork = catch_known_exceptions(design_handlers.update_an_artwork)
delete_artwork = catch_known_exceptions(design_handlers.delete_artwork)
get_design = catch_known_exceptions(design_handlers.get_design)
add_design = catch_known_exceptions(design_handlers.add_design)
update_design = catch_known_exceptions(design_handlers.update_design)
delete_design = catch_known_exceptions(design_handlers.delete_design)
add_new_color = catch_known_exceptions(design_handlers.add_new_color)
update_color = catch_known_exceptions(design_handlers.update_color)
delete_color = catch_known_exceptions(design_handlers.delete_color)
get_artworks = catch_known_exceptions(design_handlers.get_artworks)
get_body_3d_files = catch_known_exceptions(design_handlers.get_body_3d_files)
get_catalog = catch_known_exceptions(design_handlers.get_catalog)
get_colors = catch_known_exceptions(design_handlers.get_colors)
get_fonts = catch_known_exceptions(design_handlers.get_fonts)
add_font = catch_known_exceptions(design_handlers.add_font)
update_font = catch_known_exceptions(design_handlers.update_font)
delete_font = catch_known_exceptions(design_handlers.delete_font)
create_directional = catch_known_exceptions(
    design_handlers.create_directional_via_postgres
)


def get_design_routes() -> APIRouter:
    api_router = APIRouter()

    @cbv(api_router)
    class _Router:
        """ARTWORK/COLOR/3D"""

        @api_router.get(
            f"/{SECTION}/artworks", response_model=List[design_schemas.Artwork]
        )
        def get_artworks(
            self,
            id: Optional[str] = None,
            search: Optional[str] = None,
            brand: Optional[str] = None,
            name: Optional[str] = None,
            color_code: Optional[str] = None,
            sign_uris: bool = False,
            limit: Optional[int] = 10,
            offset: Optional[int] = 0,
            max_dpi: Optional[int] = None,
            include_deleted: Optional[bool] = False,
        ) -> List[design_schemas.Artwork]:
            """Get an artwork/search for artworks

            `{"brand": "JL", "dpi_36": true, "dpi_72": true }`

            """
            logger.info(f"get_artworks {id} {search} {brand} {name} {color_code}")
            result = get_artworks(
                search=search,
                name=name,
                brand=brand,
                color_code=color_code,
                id=id,
                limit=limit,
                offset=offset,
                sign_uris=sign_uris,
                max_dpi=max_dpi,
                include_deleted=include_deleted,
            )
            if result:
                return result
            else:
                raise HTTPException(404, "Artwork not found")

        @api_router.post(
            f"/{SECTION}/artworks", response_model=design_schemas.CreateArtworkResponse
        )
        def add_new_artwork(
            self, artwork: design_schemas.CreateArtwork
        ) -> design_schemas.CreateArtworkResponse:
            """
            Add a new artwork to the database. Will analyze and reject if necessary, converting
            to various dpis fires off a job and will take time

            ---

            Examples with good artwork:

            `{"s3_uri": "s3://meta-one-assets-prod/artworks/jl/c8111d5efd39b86ecadfe9c1b1d6c13c_300.png", "name": "JL API Test 1", "brand": "JL"}`

            `{"s3_uri": "s3://meta-one-assets-prod/artworks/jl/7a9e5c8a2cf5014fea0d7550fc473d64_300.png", "name": "JL API Test 2", "brand": "JL"}`

            Example with bad artwork:

            `{"s3_uri": "s3://resmagic/uploads/Channel_digital_image_CMYK_color.jpg", "name": "cmyk", "brand": "JL"}`
            """
            result = add_artwork(artwork)

            if not "errors" in result:
                return result
            else:
                not_found = ErrorCodes.IMAGE_NOT_FOUND in result["errors"]
                raise HTTPException(404 if not_found else 400, result)

        @api_router.put(f"/{SECTION}/artworks", response_model=bool)
        def update_artwork(
            self,
            id: uuid.UUID,
            deleted: bool = False,
            name: Optional[str] = None,
            brand: Optional[str] = None,
            description: Optional[str] = None,
        ):
            """
            Update an artwork in the database.

            `{"id": "f1d9cf22-d883-1006-bd63-127e3092f73d", "name": "New Name", "deleted": false}`

            """
            updated = update_artwork(
                id,
                deleted,
                name,
                brand,
                description,
            )
            if updated:
                return updated
            else:
                raise HTTPException(404, "Artwork not found")

        @api_router.delete(f"/{SECTION}/artworks", response_model=bool)
        def delete_artwork(self, id: uuid.UUID):
            """

            Soft delete an artwork from the database.

            `{"id": "f1d9cf22-d883-1006-bd63-127e3092f73d"}`

            """
            # Could have options to do soft delete
            # and/or delete the file from s3 too
            # and/or disconnect from any colors that use it
            # there may be a constraint if the artwork is placed somewhere

            deleted = delete_artwork(id)
            if deleted:
                return deleted
            else:
                raise HTTPException(404, "Artwork not found")

        @api_router.get(
            f"/{SECTION}/3d-body-files", response_model=List[design_schemas.Body3DFiles]
        )
        def get_body_3d_files(
            self,
            id: str = None,
            body_code: str = None,
            body_version: str = None,
            use_latest_version: bool = False,
            sign_uris: bool = False,
            limit: Optional[int] = 10,
            offset: Optional[int] = 0,
        ) -> List[design_schemas.Body3DFiles]:
            """
            Get the 3D files for a body (.glb and point cloud) with an id, or search for bodies that
            have 3D files (potentially filtered by body_code and body_version)
            """
            logger.info(
                f"get_body_3d_files id={id} body_code={body_code} body_version={body_version}"
            )
            result = get_body_3d_files(
                id,
                body_code,
                body_version,
                use_latest_version,
                sign_uris,
                limit,
                offset,
            )
            if result:
                return result
            else:
                raise HTTPException(404, "3D body files not found")

        @api_router.get(f"/{SECTION}/colors", response_model=List[design_schemas.Color])
        def get_colors(
            self,
            id: Optional[str] = None,
            brand: Optional[str] = None,
            name: Optional[str] = None,
            sign_uris: Optional[bool] = False,
            limit: Optional[int] = 10,
            offset: Optional[int] = 0,
            max_dpi: Optional[int] = None,
            include_deleted: Optional[bool] = False,
        ) -> List[design_schemas.Color]:
            """
            Get a Color (collection of artworks)

            (This data only comes from hasura, not create one. For the future we would like to
            do a migration and create a color here any time a color is created in create one)

            `{"brand": "JL", "dpi_36": true}`
            """
            logger.info(f"get_colors name={name} brand={brand} id={id}")
            result = get_colors(
                id=id,
                brand=brand,
                name=name,
                limit=limit,
                offset=offset,
                sign_uris=sign_uris,
                max_dpi=max_dpi,
                include_deleted=include_deleted,
            )
            if result:
                return result
            else:
                raise HTTPException(404, "Color not found")

        @api_router.post(f"/{SECTION}/colors", response_model=uuid.UUID | None)
        def add_new_color(self, color: design_schemas.CreateColor) -> uuid.UUID | None:
            """
            Add a Color (collection of artworks)

            `{"code": "APIJLT", "brand": "JL", "name": "Test POST Color", "artwork_ids": ["f1d9cf22-d883-1006-bd63-127e3092f73d"]}`

            `{"code": "APIJL2", "brand": "JL", "name": "Test POST Color 2", "artwork_ids": ["a40a8161-9879-2312-2788-31b57d2d05c2"]}`

            """
            logger.info(f"add_new_color {color}")
            return add_new_color(color)

        @api_router.put(f"/{SECTION}/colors")
        def update_color(self, color: design_schemas.UpdateColor) -> dict:
            """
            Update a Color (only non-null fields will be updated)

            ```
            {
                "code": "APIJLT",
                "brand": "JL",
                "name": "Test PUT Color",
                "artwork_ids": [
                    "f1d9cf22-d883-1006-bd63-127e3092f73d",
                    "a40a8161-9879-2312-2788-31b57d2d05c2"
                ]
            }
            ```

            """
            logger.info(f"update_color {color}")
            return update_color(color)

        @api_router.delete(f"/{SECTION}/colors")
        def delete_color(self, id: uuid.UUID, orphan_colors: bool = False) -> dict:
            """

            Soft delete a Color, orphaning will delete the color-artwork relationship but not the artwork

            `{"id": "4a72fe88-f354-41a7-23a5-0e29c2d5c885"}`

            """
            logger.info(f"delete_color {str(id)}")
            return delete_color(str(id), orphan_colors)

        # @api_router.get(f"/{SECTION}/style_short_list")
        # def get_style_short_list(
        #     self,
        #     sku_search: str = None,
        # ):
        #     """
        #     Get a concise list of styles that match the search criteria
        #     """
        #     try:
        #         return get_style_list(sku_search)
        #     except Exception as e:
        #         logger.warn(traceback.format_exc())
        #         raise HTTPException(500)

        @api_router.get(f"/{SECTION}/catalog")
        def get_catalog(
            self,
            brand_search: str = None,
            body_search: str = None,
            color_search: str = None,
            limit: int = 10,
            offset: int = 0,
        ):
            """
            Get a concise list of designs that match the search criteria

            The returned design_id can be used to retrieve the full design
            """
            return get_catalog(brand_search, body_search, color_search, limit, offset)

        @api_router.get(f"/{SECTION}")
        def get_design(
            self,
            design_id: str = None,
            body_id: str = None,
            body_code: str = None,
            body_version: int = None,
            color_code: str = None,
            max_dpi: int = None,
        ):
            """
            Get the 3d model files, artworks and their placements, geometries and more for a design

            There are a few ways to call this:

            1. passing in a `body_code` and `body_version` will return a blank design for the style editor to work on

            2. passing in a `design_id` will search for a design with that id

            3. passing in a `body_code`, `body_version` and `color_code` will search for a design with that body and color

            The returned design will contain s3 signed urls for the model files and artworks. For artworks you can specify
            the `max_dpi` to limit the size of the returned images. 72dpi is recommended for the style editor.

            ---

            Blank design example:

            ```{body_code: "TT-3072", body_version: 1}```

            Design search example:

            ```{design_id: "b3126f6b-3e36-6e4e-546b-46495186ae90"}```

            Body and color search example:

            ```{body_code: "TT-3072", body_version: 1, color_code: "JLINTP"}```

            """
            return get_design(
                design_id, body_id, body_code, body_version, color_code, max_dpi
            )

        @api_router.post(f"/{SECTION}")
        def add_new_design(
            self,
            design: design_schemas.UserDesign,
            sizeless_sku: str,
            body_version: int,
            brand: str = None,
            submit_job: bool = False,
            editor_type: str = None,
            bypass_create_one: bool = False,
        ):
            # fmt: off
            f"""
            Save a design with artworks and their placements, geometries and more.

            Currently, a design should have a pre-existing style in create.one. The `sizeless_sku` should look something like `TT-3072 MATRL COLOR1` this is used to match to a style in AirTable/create one. The link to the legacy style is necessary to enable the rest of the flow such as ordering & production. 

            `submit_job` will fire off a job to process the design and create images ready to print

            `editor_type` is used to specify the editor that created the design, e.g. `{apply_dynamic_color.STYLE_EDITOR_TYPE_3D}` or `{apply_dynamic_color.STYLE_EDITOR_TYPE_2D}` 

            `bypass_create_one` is used for testing, it skips checking/updating create.one and airtable
            """
            # fmt: on
            # logger.info(
            #     f"add_new_placements {design} {brand} {color_code} {body_id} {body_code} {body_version}"
            # )
            return add_design(
                design,
                sizeless_sku,
                body_version,
                brand,
                submit_job,
                editor_type,
                bypass_create_one,
            )

        @api_router.put(f"/{SECTION}")
        def update_design(
            self,
            design: design_schemas.UserDesign,
            design_id: str,
            sizeless_sku: str,
            submit_job: bool = False,
            editor_type: str = None,
            bypass_create_one: bool = False,
        ):
            return update_design(
                design,
                design_id,
                sizeless_sku,
                submit_job,
                editor_type,
                bypass_create_one,
            )

        @api_router.delete(f"/{SECTION}")
        def delete_design(self, design_id: str):
            """
            soft delete a design
            """
            return delete_design(design_id)

        @api_router.get(f"/{SECTION}/fonts")
        def get_fonts(
            self,
            id: str = None,
            search: str = None,
            brand: str = None,
            name: str = None,
            sign_uris: bool = False,
            limit: int = 10,
            offset: int = 0,
            include_deleted: bool = False,
        ):
            """
            Get a list of fonts for the brand
            """
            return get_fonts(
                id=id,
                search=search,
                brand=brand,
                name=name,
                sign_uris=sign_uris,
                limit=limit,
                offset=offset,
                include_deleted=include_deleted,
            )

        @api_router.post(f"/{SECTION}/fonts")
        def add_font(self, font: design_schemas.Font):
            """
            Add a new font to the database

            e.g.

            `https://fontsfree.net//wp-content/fonts/basic/sans-serif/FontsFree-Net-ALSDirect2.ttf`

            `https://fontsfree.net/wp-content/uploads/2024/02/ComicMono-Bold.ttf`

            or one that's uploaded to our own s3:
            `s3://resmagic/uploads/DOS437.ttf`

            You just need these filled in:

            ```
            {
                "name": "DOS437",
                "brand": "JD",
                "source": "s3://resmagic/uploads/DOS437.ttf"
            }
            ```
            """
            return add_font(font)

        @api_router.put(f"/{SECTION}/fonts")
        def update_font(self, font: design_schemas.Font):
            """
            Update a font in the database
            """
            return update_font(font)

        @api_router.delete(f"/{SECTION}/fonts")
        def delete_font(self, id: str):
            """
            Soft delete a font from the database
            """
            return delete_font(id)

        @api_router.post(f"/{SECTION}/directional")
        def add_directional(
            self,
            body_code: str,
            material_code: str,
            color_code: str,
            body_version: int,
            brand: str,
            image_uri: str,
            image_name: str,
        ):
            """
            Create a directional

            e.g.

            `"TT-3072", "COMCT", "APISTY", 1, "JL", uri, "test auto style creation"`

            """
            return create_directional(
                body_code,
                material_code,
                color_code,
                body_version,
                brand,
                image_uri,
                image_name,
            )

    return api_router


router = get_design_routes()
