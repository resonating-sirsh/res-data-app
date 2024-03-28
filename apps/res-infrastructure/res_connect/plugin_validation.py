import res
from tenacity import retry, stop_after_attempt, wait_exponential
from res.flows.meta.pieces import PieceName, get_piece_components_cache
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.utils.logging import logger

wait = wait_exponential(multiplier=1, min=4, max=10)
stop = stop_after_attempt(3)

AIRTABLE_DXA = "appqtN4USHTmyC6Dv"
AIRTABLE_DXA_IGNORED_ERRORS = "tbl772MyCjkUoo93W"


class PluginValidationHelper:
    def __init__(self):
        pass

    @retry(wait=wait, stop=stop)
    def ignore_validation_error(self, piece, error, username, ignored):
        hasura = res.connectors.load("hasura")
        INSERT_IGNORED_ERROR = """
            mutation AddNewIgnoredError($object: dxa_vs_plugin_ignored_errors_insert_input!) {
                insert_dxa_vs_plugin_ignored_errors_one(
                    object: $object,
                    on_conflict: {
                        constraint: vs_plugin_ignored_errors_pkey
                        update_columns: [active, updated_at]
                    }
                ) {
                    error_text
                }
            }"""

        logger.info("Ignoring error in hasura...")
        hasura.execute_with_kwargs(
            INSERT_IGNORED_ERROR,
            object={
                "piece_name": piece,
                "error_text": error,
                "username": username,
                "active": ignored,
            },
        )

        logger.info("Ignoring error in airtable...")
        record = {
            "Error": error,
            "Username": username,
            "Ignored": ("Yes" if ignored else "No"),
        }
        airtable = res.connectors.load("airtable")
        ignored_errors = airtable[AIRTABLE_DXA][AIRTABLE_DXA_IGNORED_ERRORS]
        matches = ignored_errors.to_dataframe(
            filters=f'AND(SEARCH("{error}",{{Error}}))'
        )
        if len(matches) != 0:
            record["record_id"] = matches.iloc[0]["record_id"]
        ignored_errors.update(record)

        logger.info("Error ignored")

    @retry(wait=wait, stop=stop)
    def submit_validation_report(self, report):
        # log the errors somewhere
        username = report.get("username")
        filename = report.get("filename")
        garment_name = report.get("garment_name")
        errors = report.get("errors")
        res.utils.logger.info(
            f"VSPlugin validation for {username} on {garment_name} at {filename}"
        )
        for key in errors:
            res.utils.logger.info(key)

        hasura = res.connectors.load("hasura")
        INSERT_VALIDATION_ERRORS = """
            mutation AddNewVSValidation($object: dxa_vs_plugin_validations_insert_input!) {
                insert_dxa_vs_plugin_validations_one(object: $object) {
                    id
                }
            }"""
        hasura.execute_with_kwargs(INSERT_VALIDATION_ERRORS, object=report)

    @retry(wait=wait, stop=stop)
    def get_ignored_errors(self, data):
        try:
            res.utils.logger.info("Getting ignored errors")
            body_code = data.get("body_code")

            airtable = res.connectors.load("airtable")
            ignored_errors = airtable[AIRTABLE_DXA][AIRTABLE_DXA_IGNORED_ERRORS]
            matches = ignored_errors.to_dataframe(
                filters=f'AND(SEARCH("{body_code}",{{Error}}),SEARCH("Yes",{{Ignored}}))'
            )
            return matches["Error"].tolist() if len(matches) != 0 else []
        except:
            return []

    @retry(wait=wait, stop=stop)
    def get_bad_names(self, data):
        res.utils.logger.info("Getting bad piece names")
        piece_names = data.get("piece_names")
        pc = get_piece_components_cache(just_use_airtable=True)

        bad_names = {
            name: PieceName(name, validate=True, known_components=pc).validate()
            for name in piece_names
        }
        bad_names = {k: v for k, v in bad_names.items() if v}

        if bad_names:
            res.utils.logger.warning(f"Found bad piece names: {bad_names}")

        return bad_names

    @retry(wait=wait, stop=stop)
    def get_cuttable_widths(self, data):
        res.utils.logger.info("Getting cuttable widths")
        GET_MATERIAL_QUERY = """
            query materials1(
                $after: String
                $where: MaterialsWhere!
                $sort: [MaterialsSort!]
            ) {
                materials(first: 50, after: $after, where: $where, sort: $sort) {
                    materials {
                        code
                        cuttableWidth
                        offsetSizeInches
                        printFileCompensationWidth
                    }
                }
            }
            
        """

        fabric_codes = data.get("fabric_codes")

        if not fabric_codes:
            return []

        # get all the cuttable widths for each specified fabric
        owhere = {
            "code": {"isAnyOf": fabric_codes},
        }
        fabric_response = self.gql.query_with_kwargs(GET_MATERIAL_QUERY, where=owhere)

        return fabric_response.get("data", {}).get("materials", {}).get("materials", [])

    @retry(wait=wait, stop=stop)
    def get_body_data(self, data):
        res.utils.logger.info("Getting body data")
        GET_BODY_PIECES = """
            query getBody($number: String) {
                body(number: $number) {
                    code
                    pieces{
                        code
                    }
                    availableSizes{
                        name
                        aliases
                        petiteSize {
                            name
                        }
                    }
                    allSupportedMaterials{
                        id
                        code
                        cuttableWidth
                        offsetSizeInches
                        printFileCompensationWidth
                    }
                }
            }
        """

        # get the body pieces and sizes
        body_code = data.get("body_code")
        body_response = self.gql.query_with_kwargs(GET_BODY_PIECES, number=body_code)

        required_sizes = (
            body_response.get("data", {}).get("body", {}).get("availableSizes", [])
        )

        piece_names = data.get("piece_names")

        required_pieces = (
            body_response.get("data", {}).get("body", {}).get("pieces", [])
        )
        required_pieces = set(p.get("code") for p in required_pieces)

        def piece_name_without_prefix(piece_name):
            try:
                without_stature = piece_name.split("_")[0]
                parts = without_stature.split("-")

                return "-".join(parts[-2:])
            except:
                return "__invalid__"

        existing_pieces = set(piece_name_without_prefix(p) for p in piece_names)
        missing_pieces = list(required_pieces - existing_pieces)

        if missing_pieces:
            res.utils.logger.warning(f"Found missing pieces: {missing_pieces}")

        supported_materials = (
            body_response.get("data", {})
            .get("body", {})
            .get("allSupportedMaterials", [])
        )

        return (
            required_sizes,
            missing_pieces,
            list(required_pieces),
            supported_materials,
            self.get_settings(),
        )

    @retry(wait=wait, stop=stop)
    def get_settings(self):
        try:
            hasura = res.connectors.load("hasura")
            PLUGIN_SETTINGS_QUERY = """
                query getPluginSettings {
                    dxa_vs_plugin_settings {
                        name
                        value
                    }
                }
            """
            settings = hasura.execute(PLUGIN_SETTINGS_QUERY)
            res.utils.logger.info(f"Got plugin settings: {settings}")

            return settings.get("dxa_vs_plugin_settings", [])
        except:
            return []

    def validate(self, data):
        res.utils.logger.info("Validating plugin data...")
        self.gql = ResGraphQLClient()

        (
            required_sizes,
            missing_pieces,
            required_pieces,
            supported_materials,
            settings,
        ) = self.get_body_data(data)

        result = {
            "bad_names": self.get_bad_names(data),
            "required_sizes": required_sizes,
            "missing_pieces": missing_pieces,
            "required_pieces": required_pieces,
            "cuttable_widths": self.get_cuttable_widths(data),
            "supported_materials": supported_materials,
            "ignored_errors": self.get_ignored_errors(data),
            "settings": settings,
        }

        res.utils.logger.info(result)

        return result
