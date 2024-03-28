import re

validation_error_mapping = {
    "Shape name doesn't match": "PIECE_NAMES_MATCH",
    "Filename doesn't match": "PIECE_NAMES_MATCH_FILE",
    "Found duplicated piece names": "PIECE_NAMES_UNIQUE",
    "Invalid size suffix": "PIECE_NAMES_VALID_SIZE_SUFFIX",
    'No "All Pieces" snapshot': "FINAL_SNAPSHOT_ALL_PIECES",
    "Pieces wider than": "PIECE_FITS_CUTTABLE_WIDTH",
    "Pieces without material mapping": "PIECE_MATERIAL_MAPPING",
    "Invalid garment name": "GARMENT_NAME",
    "Garment name doesn't match": "GARMENT_NAME",
    "Unable to validate": "RAN_VALIDATION",
    "Invalid piece names": "PIECE_NAMES_VALID",
    "Missing pieces": "NO_MISSING_BODY_PIECES",
    "Extra pieces": "NO_EXTRA_BODY_PIECES",
    "Missing color pieces": "ALL_COLOR_PIECES",
    "Missing sizes": "NO_MISSING_BODY_SIZES",
    "Extra sizes": "NO_EXTRA_BODY_SIZES",
    "No `final` snapshot found!": "FINAL_SNAPSHOT",
    'No "Final" snapshot': "FINAL_SNAPSHOT",
    "Invalid ruler name": "POM_ASSETS_NAMES",
    "Invalid ruler length": "POM_ASSETS_LENGTHS",
    "Self piece with no fabric": "SELF_PIECES_WITH_FABRIC",
    "Multiple colorways": "SINGLE_COLORWAY",
    "Plugin Exception": "BERTHA_RAN",
    "Missing PDFs for": "EXPORTED_PDFS",
    "Not all PDFs produced": "EXPORTED_PDFS",
    "Inconsistent grading": "NO_INCONSISTENT_GRADING",
    "Pieces with sharp bends": "NO_SHARP_BENDS",
    "Pieces with close together points ": "NO_CLOSE_TOGETHER_POINTS",
    # "Height too large": "PIECE_HEIGHT_VSTITCHER_BUG", # warning only as we can use the geoms anyways
    "Shape version doesn't match body version": "PIECE_VERSION_MATCH",
    "Duplicate snapshot name": "NO_DUPLICATE_SNAPSHOT_NAMES",
}


def parse_status_details_for_issues(status_details):
    if not status_details:
        return []

    exception_token = "Exception: "
    issues = []

    if "raise_exception_if_not_valid" in status_details:
        pre = "Exception: "
        post = "}"

        exception_text = re.search(f"{pre}(.*){post}", status_details)
        if exception_text is None:
            issues.append({"issue_type_code": "BERTHA_RAN", "context": status_details})
        else:
            exception_text = exception_text.group(1)
            # print("vvv")
            # print(exception_text)

            # parsing the validation problematic, it's not JSON and ast.literal_eval doesn't work
            # all the validation errors have an array of instances after, so try to get the array
            # of piece names in between errors
            error_locations = {}
            for error, contract in validation_error_mapping.items():
                if error in exception_text:
                    error_locations[contract] = exception_text.index(error)

            # if we didn't find any we don't know what this is yet
            if not error_locations:
                issues.append(
                    {
                        "issue_type_code": "BERTHA_RAN",
                        "context": f"Validation threw an error, but we couldn't parse it: {status_details}",
                    }
                )
            else:
                # find the arrays of piece names between errors, sort by the index of the error
                keys = sorted(error_locations, key=error_locations.get)
                # print(keys)
                for i in range(len(keys)):
                    start = error_locations[keys[i]]
                    end = (
                        len(exception_text)
                        if i == len(keys) - 1
                        else error_locations[keys[i + 1]]
                    )

                    start_bracket = exception_text.find("[", start, end)
                    end_bracket = exception_text.rfind("]", start, end)

                    if start_bracket == -1 or end_bracket == -1:
                        context = exception_text[start:end]
                    else:
                        context = exception_text[start_bracket + 1 : end_bracket]

                    issues.append({"issue_type_code": keys[i], "context": context})
    elif "Failed to start" in status_details:
        issues.append(
            {
                "issue_type_code": "BERTHA_RAN",
                "context": "VStitcher failed to start - could be due to lack of licenses",
            }
        )
    elif (
        "Invalid Tech Pack" in status_details
        or "techpack processor failed" in status_details
    ):
        issues.append(
            {
                "issue_type_code": "VALID_TECH_PACK",
                "context": "Invalid tech pack, images may be corrupt or missing",
            }
        )
    elif "killed" in status_details:
        issues.append({"issue_type_code": "BERTHA_RAN", "context": status_details})
    elif "timed out" in status_details:
        issues.append(
            {"issue_type_code": "BERTHA_RAN", "context": "Job didn't finish in time"}
        )
    elif "MAX_IMAGE_PIXELS" in status_details:
        issues.append(
            {"issue_type_code": "MAX_IMAGE_PIXELS", "context": status_details}
        )
    elif "PIL" in status_details:
        issues.append({"issue_type_code": "IMAGE_PROCESSED", "context": status_details})
        #     #####################################
    elif "SnapshotInfoGet" in status_details and "JSONDecodeError" in status_details:
        issues.append(
            {"issue_type_code": "SNAPSHOT_INFO_GET", "context": status_details}
        )
    # elif (
    #     "hung" in status_details
    #     or "reason given" in status_details
    #     or "box-request-id" in status_details
    #     or "No such file or directory: '/Users/natalie/dev/jobs_output/"
    #     in status_details
    # ):
    #     issues.append({"issue_type_code": "BERTHA_RAN", "context": status_details})
    #     #####################################
    elif exception_token in status_details:
        exception_text = status_details[
            status_details.index(exception_token) + len(exception_token) :
        ]

        if "DPI" in exception_text:
            issues.append(
                {"issue_type_code": "ARTWORK_300_DPI", "context": exception_text}
            )
        elif "Duplicate snapshot" in exception_text:
            issues.append(
                {
                    "issue_type_code": "NO_DUPLICATE_SNAPSHOT_NAMES",
                    "context": exception_text,
                }
            )
        elif "snapshot" in exception_text:
            issues.append(
                {"issue_type_code": "FINAL_SNAPSHOT", "context": exception_text}
            )
        elif "No material named" in exception_text:
            issues.append(
                {"issue_type_code": "APPLY_COLOR_SUCCESSFUL", "context": exception_text}
            )
        elif "Found duplicated" in exception_text:
            issues.append(
                {"issue_type_code": "PIECE_NAMES_UNIQUE", "context": exception_text}
            )
        elif "Missing PDFs" in exception_text:
            issues.append(
                {"issue_type_code": "EXPORTED_PDFS", "context": exception_text}
            )
        elif "Turntable export failed" in exception_text:
            issues.append(
                {"issue_type_code": "TURNTABLE_GENERATED", "context": exception_text}
            )
        else:
            issues.append({"issue_type_code": "BERTHA_RAN", "context": exception_text})
    else:
        issues.append({"issue_type_code": "BERTHA_RAN", "context": status_details})

    # print(multi)
    # print("^^^")
    return issues
