GET_APPLY_COLOR_REQUEST = """
    query getApplyColorRequest($id: ID!){
        applyColorRequest(id:$id){
            id
            key
            priority
            requestType
            colorType
            applyColorFlowType
            bodyVersion
            colorApplicationMode
            artworkBundleDirS3Uri
            exportAssetBundleJobId
            applyColorFlowType
            assignee {
                id
                email
                name
            }
            requestReasonsContractVariables{
                id
                code
            }
            requestBody3dSimulationFile{
                file {
                    uri
                }
                bodyVersion
            }
            coloredPiecesFile {
                uri
            }
            style {
                id
                code
                isStyle3dOnboarded
                body {
                    id
                    patternVersionNumber
                    code
                    number
                    isOneReady
                    isReadyForSample
                }
                artworkFile {
                    file {
                        s3 {
                            bucket
                            key
                        }
                    }
                }
                pieceMapping{
                    bodyPiece {
                        code
                    }
                    material{
                        code
                        cuttableWidth
                        offsetSizeInches
                        printFileCompensationWidth
                    }
                }
                stylePiecesMappingStatus {
                    isMakeOneReady
                    reasons
                    internalReasons
                }
                material {
                    code
                    cuttableWidth
                    offsetSizeInches
                    printFileCompensationWidth
                }
                color {
                    code
                }
                name
            }
        }
    }
"""

GET_APPLY_COLOR_REQUESTS = """
query applyColorRequests($where: ApplyColorRequestsWhere $first: Int! $sort:[ApplyColorRequestsSort!] $after:String) {
  applyColorRequests(where: $where, first: $first sort:$sort after:$after) {
    applyColorRequests {
      id
      key
      assignee{
        id
        email
      }
      requestReasonsContractVariables {
        id
        code
      }
    }
    hasMore
    count
    cursor
  }
}
"""

CREATE_APPLY_COLOR_REQUEST = """
mutation createApplyColorRequest($input: CreateApplyColorRequestInput!){
    createApplyColorRequest(input:$input){
        applyColorRequest {
            id
        }
    }
}
"""

REPRODUCE_APPLY_COLOR_REQUEST = """
mutation reproduceApplyColorRequest($id: ID! $input: ReproduceApplyColorRequestInput!) {
  reproduceApplyColorRequest(id: $id, input: $input) {
    applyColorRequest {
      id
    }
  }
}
"""

UPDATE_APPLY_COLOR_REQUEST = """
    mutation updateApplyColorRequest($id: ID!, $input: UpdateApplyColorRequestInput!){
        updateApplyColorRequest(id:$id, input:$input){
            applyColorRequest {
                id
                applyColorFlowStatus
                exportColoredPiecesJobStatus
                turntableRequiresBrandFeedback
                style {
                    id
                    code
                    body{
                        id
                        code
                    }
                }
            }
        }
    }
"""

FLAG_APPLY_COLOR_REQUEST_MUTATION = """
mutation flagApplyColorRequest($id: ID!, $input:FlagApplyColorRequestInput!){
    flagApplyColorRequest(id:$id, input:$input){
        applyColorRequest {
            id
            key
            isFlaggedForReview
        }
    }
}
"""

UNFLAG_APPLY_COLOR_REQUEST_MUTATION = """
mutation unflagApplyColorRequest($id: ID!, $input: UnflagApplyColorRequestInput!) {
    unflagApplyColorRequest(id: $id, input: $input) {
        applyColorRequest {
            id
            isFlaggedForReview
        }
    }
}
"""

GET_APPLY_COLOR_REQUEST_ASSIGNEE_INFO = """
query applyColorRequest($id: ID!) {
  applyColorRequest(id: $id) {
    id
    colorApplicationMode
    assignee {
      id
      email
      name
    }
  }
}
"""

GET_ONE_RESOURCES = """
query oneResources(
  $where: OneResourcesWhere
  $first: Int!
  $sort: [OneResourcesSort!]
  $after: String
) {
  oneResources(where: $where, first: $first, sort: $sort, after:$after) {
    oneResources {
      resourceId
      userId
      id
    }
    count
    cursor
    hasMore
  }
}
"""
