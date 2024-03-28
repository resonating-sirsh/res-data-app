GET_CUT_MARKER = """
    query getCutMarkerInfo($where: CutOneMarkerWhere!){
        cutOneMarkers(first: 100, where: $where){
            cutOneMarkers{
              id
              name
              roll {
                id
                name
              }
              resource {
                id
                name
              }
            }
        }
    }
"""

GET_MEASUREMENT = """
    query getMeasurementRequest($where: MeasurementRequestsWhere!){
        measurementRequests(first: 100, where: $where) {
            measurementRequests{
                id
                name
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
