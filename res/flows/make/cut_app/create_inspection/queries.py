QUERY_GET_MARKER_INFO = """
     query getCutMarker($where: CutOneMarkerWhere!) {
        cutOneMarkers(first: 100, where: $where) {
            cutOneMarkers {
                id
                name
                rank
                inspectionStatus
                makeOneProductionRequest {
                    id
                    name
                    currentStation
                    orderNumber
                    type
                    sku
                    flagForReviewTags
                    nodeExitInspectionStatus
                    nodeAssigment
                    body {
                        id
                        code
                    }
                }
                resourceAssignment
                rollAssignmentName
            }
            count
            hasMore
            cursor
        }
    }
"""

QUERY_MEASUREMENT_REQUEST = """
    query getMeasurementRequest($where: MeasurementRequestsWhere!){
        measurementRequests(first: 100, where: $where) {
            measurementRequests{
                id
                name
            }
        }
    }
"""

DELETE_MEASUREMENT = """
    mutation deleteMeasurement($id: ID!) {
        deleteMeasurementByOneNumberAndStation(id: $id) {
            measurementRequest {
                id
            }
        }
    }
"""

CREATE_MEASUREMENT = """
    mutation createMeasurementRequest($input: createMeasurementRequestInput!) {
        createMeasurementRequest(input: $input) {
            measurementRequest {
                id
                requestNumber
            }
        }
    }
"""

CREATE_ONE_MEASUREMENT = """
    mutation createUnitMeasurement($input: createOneMeasurementInput!) {
        createOneMeasurement(input: $input) {
            oneMeasurement {
                id
            }
        }
    }
"""

PROCESS_REQUEST_MUTATION = """
    mutation updateMeasurement($requestId: ID!, $input: updateMeasurementRequestInput!) {
        updateMeasurementRequest(id: $requestId, input: $input) {
            measurementRequest {
                id
                requestNumber
            }
        }
    }
"""

UPDATE_CUT_MARKER = """
    mutation updateCutMarkerCloseInspection($id: ID!, $input: CutOneMarkerInput!) {
        updateCutOneMarker(id: $id, input: $input) {
            cutOneMarker {
                id
            }
        }
    }
"""
