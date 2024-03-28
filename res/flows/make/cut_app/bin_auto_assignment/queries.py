GET_CUT_REQUEST = """
    query getCutRequest($where: CutRequestWhere!){
        cutRequests(first: 100, where: $where){
            cutRequests{
                id
                isFlagForReview
                flaggedForReviewTag
            }
        }
    }
"""

GET_BIN_LOCATION = """
    query getAvailableBinLocation($where: CutOneBinLocationWhere!) {
        cutOneBinLocations(first: 100, where: $where) {
            cutOneBinLocations {
                id
                name
            }
            count
            cursor
            hasMore
        }
    }
"""

UPDATE_CUT_REQUEST = """
    mutation updateCutRequest($id: ID!, $input: updateCutRequestInput!){
        updateCutRequest(id: $id, input: $input){
            cutRequest{
                id
                binLocationId
                locationReservedAt
            }
        }
    }
"""
