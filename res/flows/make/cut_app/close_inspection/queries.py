GET_PRODUCTION_REQUEST_QUERY = """
      query getMakeOneProductionRequest($where: MakeOneProductionRequestsWhere!){
            makeOneProductionRequests(first: 1, where: $where){
                makeOneProductionRequests{
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
                        code
                    }
                }
            }
      }
"""

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
            cursor
            hasMore
        }
    }
"""

UPDATE_PRODUCTION_REQUEST_QUERY = """
      mutation updateRequest($id: ID!, $input: ProcessProductionRequestInput!) {
           processProductionRequest(id: $id, input: $input) {
               makeOneProductionRequest {
                   id
               }
           }
       }
"""

QUERY_MEASUREMENT_REQUEST = """
query getMeasurementRequest($where: MeasurementRequestsWhere!){
    measurementRequests(first: 100, where: $where) {
        measurementRequests{
            id
            name
            requestNumber
            nameInStation
            drProductionRecordId
            oneMeasurements(
                    first: 100
                    sort: [{ field: POMNUMBER, direction: ASCENDING }]
                ) {
                    oneMeasurements {
                        id
                        name
                        pointofmeasure
                        accepted
                        rejected
                        severity
                        location
                        checkPointName
                        approvedMeasurement
                        rejectedMeasurement
                    }
                }
        }
        cursor
        hasMore
        count
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

process_request_mutation = """
        mutation updateMeasurement($requestId: ID!, $input: updateMeasurementRequestInput!) {
            updateMeasurementRequest(id: $requestId, input: $input) {
                measurementRequest {
                    id
                    requestNumber
                }
            }
        }
    """
