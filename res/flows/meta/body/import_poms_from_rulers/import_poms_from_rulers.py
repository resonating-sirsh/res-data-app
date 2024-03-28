import json
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
import res.flows.meta.body.import_poms_from_rulers.graphql_queries as graphql_queries
from res.utils.meta.sizes.sizes import get_sizes_from_aliases
from res.utils import logger
import res
from res.flows.meta.body.import_poms_from_rulers.sinks import transform_json_and_write
import traceback


gql = ResGraphQLClient()


SIZES_ROW_INDEX = 1
POM_TBL_ROW_INDEX = 2
META_ONE_ASSETS_BUCKET = "meta-one-assets-prod"


def format_size_names(raw_size_names):
    size_names = []
    size_objects = []
    for i, raw_size_name in enumerate(raw_size_names):
        size = get_sizes_from_aliases([raw_size_name])[0]
        if size and size["name"]:
            size_names.append(size["name"])  # normalized size name
            size_objects.append(size)
        else:
            size_names.append(raw_size_name)
            size_objects.append({"name": raw_size_name})

    return size_names, size_objects


def validate_sizes(sizes, available_sizes):
    unknown_sizes = []
    missing_sizes = []
    errors = []

    # Log found sizes that aren't part of the bodies' available sizes
    for size in sizes:
        if size not in available_sizes:
            unknown_sizes.append(size)

    # Log the bodies' available sizes that were not found on the POM file
    for available_size in available_sizes:
        if available_size not in sizes:
            missing_sizes.append(available_size)

    if len(missing_sizes) > 0:
        missing_sizes_str = ", ".join(missing_sizes)
        errors.append(
            f"The following available sizes were not found in the POM File: {missing_sizes_str}. Please make sure "
            f"all required sizes are included in the POM file."
        )

    # log warnings
    if len(unknown_sizes) > 0:
        unknown_sizes_str = ", ".join(unknown_sizes)
        errors.append(
            f"Unknown sizes in the POM File: {unknown_sizes_str}. Not found among available sizes for this body. "
            f"Please make sure the sizes included in the POM File match the available sizes for this body."
        )

    logger.info(
        f"Sizes that aren't part of the body available sizes (unknown): {unknown_sizes}"
    )
    logger.info(f"Missing sizes: {missing_sizes}")

    return errors


def round_number_to_nearest_one_eigth(number):
    return round(number * 8) / 8


def parse_cms_to_inches(number):
    return number / 2.54


def is_pom_name_valid(pom_name):
    is_valid = True
    if "ruler" in pom_name.lower():
        is_valid = False

    return is_valid


def build_pom_json(explicit_sizes, pom_df):
    pom_json = {}
    errors = []
    invalid_pom_names = []  # pom name is not valid
    invalid_pom_numbers = []  # pom number is not valid
    sizes_missing_petite_size = []  # pom name is missing a petite size

    size_names = [size["name"] for size in explicit_sizes]

    logger.info("Building and parsing POM JSON...")
    for size in explicit_sizes:
        poms_sort_order_dict = {}
        size_measurements = {}
        petite_size_measurements = {}
        size_has_implicit_petite_poms = False
        size_invalid_pom_names = []
        size_invalid_pom_numbers = []
        size_missing_petite_size = []

        poms_sort_order_idx = 0
        for idx in range(0, len(pom_df.index)):
            size_name = size["name"]
            is_implicit_petite_pom = False

            pom_errors = []

            pom_name = pom_df.loc[idx][0]

            poms_sort_order_dict[pom_name] = poms_sort_order_idx
            poms_sort_order_idx = poms_sort_order_idx + 1

            if is_pom_name_valid(pom_name) is False:
                size_invalid_pom_names.append(pom_name)
                pom_errors.append(
                    f'Invalid POM name "{pom_name}", please verify and re-upload the 3d body file.'
                )

            try:
                pom_number = float(pom_df[size_name][idx])
                pom_number = parse_cms_to_inches(pom_number)
                pom_number_nearest_one_eight = round_number_to_nearest_one_eigth(
                    pom_number
                )
            except Exception as e:
                pom_number_nearest_one_eight = None
                size_invalid_pom_numbers.append(pom_name)
                pom_errors.append(
                    f'POM Value for measurement "{pom_name}" is not a number. Please review the measurements and '
                    f"re-upload the 3D Body File."
                )

            # workaround for implicit petites: If a measurement starts with P_, assign it the petite size instead of
            # the size itself
            if "P_" in pom_name[0:2]:
                is_implicit_petite_pom = True
                size_has_implicit_petite_poms = True
                pom_name = pom_name[2:]

            if is_implicit_petite_pom is False:
                size_measurements[pom_name] = {
                    "patternMeasurement": pom_number_nearest_one_eight,
                    "measurementId": idx + 1,
                }
                if len(pom_errors) > 0:
                    size_measurements[pom_name]["notValidReasons"] = pom_errors

            else:
                petite_size_measurements[pom_name] = {
                    "patternMeasurement": pom_number_nearest_one_eight,
                    "measurementId": idx + 1,
                }
                if len(pom_errors) > 0:
                    petite_size_measurements[pom_name]["notValidReasons"] = pom_errors

        # sort petites
        if size_has_implicit_petite_poms is True:
            sorted_pom_names = sorted(
                petite_size_measurements.keys(), key=lambda x: poms_sort_order_dict[x]
            )
            petite_size_measurements_final = {}
            for idx, i in enumerate(sorted_pom_names):
                petite_size_measurements[i]["measurementId"] = idx + 1
                petite_size_measurements_final[i] = petite_size_measurements[i]
            try:
                petite_size_name = size["petiteSize"]["name"]
                pom_json[petite_size_name] = petite_size_measurements_final
                if petite_size_name not in size_names:
                    size_names.append(petite_size_name)
            except Exception as e:
                size_missing_petite_size.append(size_name)

        # sort regular fit
        # we need to sort the measurements by name because because that's how create.ONE expects them
        sorted_pom_names = sorted(
            size_measurements.keys(), key=lambda x: poms_sort_order_dict[x]
        )
        size_measurements_final = {}
        for idx, i in enumerate(sorted_pom_names):
            size_measurements[i]["measurementId"] = idx + 1
            size_measurements_final[i] = size_measurements[i]
        pom_json[size_name] = size_measurements_final

        if len(size_invalid_pom_names) > 0:
            invalid_pom_names = invalid_pom_names + size_invalid_pom_names

        if len(size_invalid_pom_numbers) > 0:
            invalid_pom_numbers = invalid_pom_numbers + size_invalid_pom_numbers

        if len(size_missing_petite_size) > 0:
            sizes_missing_petite_size = (
                sizes_missing_petite_size + size_missing_petite_size
            )

    # Format general errors
    invalid_pom_names_message = (
        f"Invalid POM names were found, please verify and re-upload the 3d body file:"
        f' "{", ".join(list(set(invalid_pom_names)))}"'
        if len(invalid_pom_names) > 0
        else None
    )

    invalid_pom_numbers_message = (
        f"Invalid non numerical POM Values were found. Please review the following measurements and re-upload the 3D "
        f'Body File: "{", ".join(list(set(invalid_pom_numbers)))}"'
        if len(invalid_pom_numbers) > 0
        else None
    )
    sizes_missing_petite_size_message = (
        f"Sizes that don't have petite sizes assigned were found while petite measurements were included on the "
        f'rulers: "{", ".join(list(set(sizes_missing_petite_size)))}"'
        if len(sizes_missing_petite_size) > 0
        else None
    )

    if invalid_pom_names_message:
        errors.append(invalid_pom_names_message)

    if invalid_pom_numbers_message:
        errors.append(invalid_pom_numbers_message)

    if sizes_missing_petite_size_message:
        errors.append(sizes_missing_petite_size_message)

    logger.info(f"pom errors: {errors}")

    return pom_json, errors, size_names


def are_poms_graded(pom_sizes, available_sizes):
    is_graded = True
    for av_size in available_sizes:
        if av_size not in pom_sizes:
            is_graded = False
    return is_graded


def cleanup_dataframe(raw_df):
    df = raw_df
    # Remove row containing Units + empty cells since we currently don't use it
    if "Units:" in raw_df.loc[0][0]:
        df = raw_df.iloc[1:, :]
        df.reset_index(drop=True, inplace=True)

    return df


def validate_body(body, body_version, pom_file_uri):
    errors = []

    if not body:
        errors.append("Body not found.")
    elif body_version is None or body_version == 0:
        errors.append(f"Body version not valid: {body_version}.")
    elif not pom_file_uri:
        errors.append("Please revise if POM File was uploaded correctly.")
    elif body["availableSizes"]:
        if (
            body["availableSizes"] is None
            or body["availableSizes"] is []
            or len(body["availableSizes"]) < 1
        ):
            errors.append("This body does not have available sizes assigned.")

    return errors


def get_poms_from_rulers(body_code, body_version, pom_file_uri):
    pom_json = None
    errors = []

    try:
        s3 = res.connectors.load("s3")

        body = gql.query(
            graphql_queries.GET_BODY,
            variables={"number": body_code, "bodyVersion": body_version},
        )["data"]["body"]

        errors = validate_body(body, body_version, pom_file_uri)

        if len(errors) > 0:
            return (
                pom_json,
                errors,
            )  # we cannot properly validate the POM JSON if the body is not valid (doesnt have available sizes etc)

        logger.debug(body)
        logger.info(f"POM file: {pom_file_uri}")

        available_sizes = body["availableSizes"]
        available_sizes_names = [size["name"] for size in available_sizes]
        logger.info(f"Available Sizes: {available_sizes_names}")

        # Skip the "Units: Centimeters" row and the next row is the sizes
        df = s3.read(pom_file_uri, sep=",", quoting=3, header=1)
        df.columns = df.columns.str.strip('"')
        df = df.applymap(lambda x: x.strip('"'))

        raw_size_names = df.columns.values[1:]

        explicit_size_names, explicit_sizes = format_size_names(raw_size_names)

        df.columns = ["Size"] + explicit_size_names
        logger.info(df.to_string())

        pom_json, pom_json_errors, all_size_names = build_pom_json(explicit_sizes, df)

        pom_json_errors = list(set(pom_json_errors))

        logger.info(f"POM File Explicit Sizes: {raw_size_names}")
        logger.info(f"POM File all sizes: {all_size_names}")
        sizes_errors = validate_sizes(all_size_names, available_sizes_names)

        if len(sizes_errors) > 0:
            errors = errors + sizes_errors

        if len(pom_json_errors) > 0:
            errors = errors + pom_json_errors

    except Exception as e:
        errors.append(f"An error occurred: {e}")

    logger.info(f"POM JSON: {pom_json, errors}")
    logger.info(f"Errors: {errors}")

    return pom_json, errors


def handler(body_code, body_version, mode=None):
    body_version = int(body_version)

    pom_json = None
    errors = []

    logger.info(f"Import Body Points Of Mesure: {body_code}")

    try:
        body = gql.query(
            graphql_queries.GET_BODY,
            variables={"number": body_code, "bodyVersion": body_version},
        )

        body = body["data"]["body"] if body["data"] and body["data"]["body"] else body

        pom_file_uri = (
            body["pomFile3d"]["file"]["uri"]
            if body
            and body["pomFile3d"]
            and body["pomFile3d"]["file"]
            and body["pomFile3d"]["file"]["uri"]
            else None
        )
        pom_json, errors = get_poms_from_rulers(body_code, body_version, pom_file_uri)

        try:
            # experimental - we write parquet files and create an external table in snowflake over them
            transform_json_and_write(pom_json, body_code, body_version)
        except:
            res.utils.logger.warn(
                f"Failed an experimental step to write the transformed files for the poms {traceback.format_exc()}"
            )
    except Exception as e:
        errors.append(f"An error occurred: {e}")

    add_pom_json_3d(
        body_code,
        body_version,
        pom_json,
        errors,
        mode,
    )

    logger.info("Done registering POMS")
    return pom_json, errors


def add_pom_json_3d(
    body_code,
    body_version,
    pom_json,
    errors,
    mode=None,
):
    r = None

    if mode == "test" or mode == "dev":
        logger.info("mocking: updating bodyPointsOfMeasure database...")
        logger.info(
            {
                "code": body_code,
                "bodyVersion": body_version,
                "pointsOfMeasureBySizeJson": json.dumps(pom_json, indent=4),
                "errors": errors,
            }
        )
    else:
        logger.info("updating bodyPointsOfMeasure database")

        r = gql.query(
            graphql_queries.ADD_POM_JSON_3D,
            {
                "code": body_code,
                "bodyVersion": body_version,
                "input": {
                    "pointsOfMeasureBySizeJson": json.dumps(pom_json, indent=4)
                    if pom_json
                    else None,
                    "errors": errors,
                    # "pomGraded": is_graded,
                },
            },
        )
        logger.info(f"response:{r}")
        # logger.info(json.dumps(pom_json))..
    return r
