# nudged ...
from http import HTTPStatus
from res.utils import logger
from fastapi import APIRouter, Query
from fastapi_utils.cbv import cbv
from res.flows.meta.trims import trims
from fastapi import HTTPException
from schemas.pydantic.trims import (
    SortDirection,
    SqlSortFields,
    Trim,
    TrimsInput,
    ReturnGetTrims,
    SqlSort,
    CreateTrimTaxonomy,
    ReturnTrimTaxonomySet,
    ReturnTrimTaxonomyList,
    BillOfMaterials,
)
from typing import List, Optional

from res.utils import ping_slack

router = APIRouter()


@cbv(router)
class Router:
    """TRIMS ROUTES"""

    @router.get("/", response_model=List[ReturnGetTrims], status_code=HTTPStatus.OK)
    def get_trims(
        self,
        limit: int = 1,
        page_number: int = 1,
        id: Optional[str] = None,
        brand_code: Optional[str] = None,
        trim_name: Optional[str] = None,
        in_stock: Optional[bool] = None,
        color_shape: Optional[str] = None,
        airtable_trim_taxonomy_id: Optional[str] = None,
        sort_by: SqlSortFields = Query(SqlSortFields.NAME),
        sort_direction: SortDirection = Query(SortDirection.ASC),
    ):
        """
        Get all trims \n
        The provided code is a route handler for a GET request in a FastAPI application. The route is defined at the path "/", and it's expected to return a list of ReturnGetTrims objects with a status code of 200 (OK).\n
        The get_trims function is the handler for this route. It accepts several optional parameters that can be used to filter and sort the trims that are returned:\n
            limit and page_number are used for pagination. By default, the function returns the first page of results, with one result per page.\n
            id, brand_code, trim_name, in_stock, color_shape, and airtable_trim_taxonomy_id can be used to filter the results based on these fields.\n
            sort_by and sort_direction are used to sort the results. By default, the results are sorted by name in ascending order.\n
        If an exception occurs while getting the trims, an error message is logged, a message is sent to Slack, and an HTTP 500 error is raised.
        """
        sort = SqlSort(name=sort_by, direction=sort_direction)

        try:
            return trims.get_trims(
                limit=limit,
                page_number=page_number,
                id=id,
                brand_code=brand_code,
                trim_name=trim_name,
                in_stock=in_stock,
                color_shape=color_shape,
                airtable_trim_taxonomy_id=airtable_trim_taxonomy_id,
                sort=sort,
            )
        except Exception as e:
            logger.error(f"Error getting trims: {e}")
            ping_slack(
                f"[TRIMS-API] ERROR 🚨 {e}",
                "metabots",
            )
            raise HTTPException(status_code=500, detail=str(e))

    @router.post("/", response_model=Trim, status_code=HTTPStatus.CREATED)
    def create_trim(self, request_payload: TrimsInput):
        """
        Create a trim\n
        The provided code is a route handler for a POST request in a FastAPI application. The route is defined at the path "/", and it's expected to return a Trim object with a status code of 201 (CREATED).\n
        The create_trim function is the handler for this route. It accepts a request_payload parameter of type TrimsInput, which is a Pydantic model that validates the data in the request payload.\n
        for create a trim, the request payload must be like this:\n
            {
                "brand_code": "RS", -> brand code
                "airtable_brand_id": "recXHC5NWBNclJjA5", -> id of the brand
                "status": "PENDING_RECEIPT", -> this status is always the same for create a trim
                "type": "SUPPLIED",-> this will indicate if the trim is supplied or made by resonance
                "airtable_color_id": "recEGwJECxNFVILsP", -> if we already had the selected, will sent the color id
                "color_name_by_brand": "Red", -> if we don't have the color free text will save for later used
                "color_hex_code_by_brand": "#FF1010",
                "airtable_trim_taxonomy_id": "recRhG5loxRsDc6TJ", -> trim taxonomy id
                "airtable_trim_category_id": "recRhG5loxRsDc6TJ", -> this trim category is a trim taxonomy id
                "image": [
                    {
                        "url": "https://test.com"
                    }
                ], -> this is the image url
                "custom_brand_ai": [
                    {
                        "url": "https://test.com"
                    }
                ], -> custom brand AI file
                "order_quantity": 1, -> this indicate the quantity of the trims that will sent to factory
                "vendor_name": "Trim Supplied Corp", -> vendor name of the trim
                "airtable_vendor_id": "recNysMIC0O5UfVqv", -> if id of the vendor
                "airtable_size_id": "reccTh6AKamGGYrpZ", -> if trim had size sent the size id
                "expected_delivery_date": "2024-05-05" -> expected date that this trim will arrive at the factory
            }
        """
        logger.info(f"Creating trim {request_payload}")
        try:
            return trims.create_trim(request_payload)
        except Exception as e:
            logger.error(f"Error creating trim: {e}")
            ping_slack(
                f"[TRIMS-API] ERROR 🚨 {e}",
                "metabots",
            )
            raise HTTPException(status_code=500, detail=str(e))

    @router.post("/assign-bin-location/{trim_id}", status_code=HTTPStatus.OK)
    def assign_bin_location(self, trim_id: str):
        """
        When the record is mark as MAKE: Received & Registered, this will auto assign a bin location to the trim
        """
        try:
            return trims.assign_bin_location(trim_id)
        except Exception as e:
            logger.error(f"Error assigning bin location to trim {trim_id}: {e}")
            ping_slack(
                f"[TRIMS-API] ERROR 🚨 {e}",
                "metabots",
            )
            raise HTTPException(status_code=500, detail=str(e))

    @router.post("/checkin-approval-trim/{trim_id}", status_code=HTTPStatus.OK)
    def checkin_approval_trim(self, trim_id: str):
        """
        When the trim is checkin and had a bin location assigned, this will auto approve the trim
        """
        try:
            return trims.checkin_approval_trim(trim_id)
        except Exception as e:
            logger.error(f"Error checkin approval trim {trim_id}: {e}")
            ping_slack(
                f"[TRIMS-API] ERROR 🚨 {e}",
                "metabots",
            )
            raise HTTPException(status_code=500, detail=str(e))

    @router.put("/{trim_id}", response_model=Trim, status_code=HTTPStatus.OK)
    def update_trim(self, trim_id: str, request_payload: TrimsInput):
        """
        The provided code is a route handler for a PUT request in a FastAPI application. The route is defined at the path "/{trim_id}", where "trim_id" is a path parameter that should be replaced with the ID of the trim to be updated. The route is expected to return a Trim object with a status code of 200 (OK).\n

        The update_trim function is the handler for this route. It accepts two parameters: trim_id and request_payload. The trim_id parameter is a string that represents the ID of the trim to be updated. The request_payload parameter is of type TrimsInput, which is a Pydantic model that validates the data in the request payload.\n

        Inside the function, the trims.update_trim function is called with trim_id and request_payload as arguments. This function is expected to update the trim in the database and return a Trim object representing the updated trim.\n

        If an exception occurs while updating the trim, an error message is logged and an HTTP 500 error is raised. The error message includes the name of the trim being updated and the exception message. This error handling ensures that a meaningful error response is sent to the client if something goes wrong during the update operation.\n
        """
        try:
            return trims.update_trim(trim_id, request_payload)
        except Exception as e:
            logger.error(f"Error updating trim {request_payload.name}: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @router.post(
        "/insert-trim-taxonomy",
        response_model=ReturnTrimTaxonomySet,
        status_code=HTTPStatus.OK,
    )
    def migrate_trim_taxonony(self, request_payload: CreateTrimTaxonomy):
        """
        this route handler is used to create a new trim taxonomy in the database based on the data provided in the request payload. It validates the request payload, creates the trim taxonomy, and handles any exceptions that might occur during this process.
        """
        try:
            return trims.create_trim_taxonomy(request_payload)
        except Exception as e:
            logger.error(f"Error migrating trim taxonomy to postgres: {e}")
            ping_slack(
                f"[TRIMS-API] ERROR 🚨 {e}",
                "metabots",
            )
            raise HTTPException(status_code=500, detail=str(e))

    @router.post(
        "/update-trim-taxonomy",
        response_model=ReturnTrimTaxonomySet,
        status_code=HTTPStatus.OK,
    )
    def update_trim_taxonony(self, id: str, request_payload: CreateTrimTaxonomy):
        """
        Update the trim taxonomy from airtable to postgres
        """
        try:
            return trims.update_trim_taxonomy(id, request_payload)
        except Exception as e:
            logger.error(f"Error updating trim taxonomy to postgres: {e}")
            ping_slack(
                f"[TRIMS-API] ERROR 🚨 {e}",
                "metabots",
            )
            raise HTTPException(status_code=500, detail=str(e))

    @router.get(
        "/by-taxonomy/{airtable_trim_taxonomy_id}",
        response_model=List[ReturnGetTrims],
        status_code=HTTPStatus.OK,
    )
    def get_trims_by_taxonomy(self, airtable_trim_taxonomy_id: str):
        """
        Get all trims by airtable trim taxonomy
        """
        try:
            return trims.get_trims_by_trim_taxonomy(airtable_trim_taxonomy_id)
        except Exception as e:
            logger.error(f"Error getting trims by taxonomy: {e}")
            ping_slack(
                f"[TRIMS-API] ERROR 🚨 {e}",
                "metabots",
            )
            raise HTTPException(status_code=500, detail=str(e))

    @router.get(
        "/taxonomy",
        response_model=ReturnTrimTaxonomyList,
        status_code=HTTPStatus.OK,
    )
    def get_trim_taxonomy(self, airtable_trim_taxonomy_id: str):
        """
        Get taxonomy with their trims and leaf
        """
        try:
            return trims.get_list_of_taxonomies(airtable_trim_taxonomy_id)
        except Exception as e:
            logger.error(f"Error getting trims taxonomy: {e}")
            ping_slack(
                f"[TRIMS-API] ERROR 🚨 {e}",
                "metabots",
            )
            raise HTTPException(status_code=500, detail=str(e))

    @router.get(
        "/get-trim-by-bodyCode/{body_code}",
        response_model=List[BillOfMaterials],
        status_code=HTTPStatus.OK,
    )
    def get_trim_by_body_code(self, body_code: str):
        """
        Get all trims belong to body
        """
        try:
            return trims.get_trims_by_body_code(body_code)
        except Exception as e:
            logger.error(f"Error getting trims by body code: {e}")
            ping_slack(
                f"[TRIMS-API] ERROR 🚨 {e}",
                "metabots",
            )
            raise HTTPException(status_code=500, detail=str(e))
