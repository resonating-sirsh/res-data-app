from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient


# MAKE ONE REQUEST QUERIES
CREATE_REQUEST_MUTATION = """
mutation createProductionRequest($input: CreateRequestInput!){
    createProductionRequest(input:$input) {
        makeOneProductionRequest {
            id
            orderNumber
        }
    }
}
"""

UPDATE_MAKE_ONE_REQUEST_MUTATION = """
mutation updateProductionRequest($id: ID!, $input: UpdateMakeOneProductionRequestInput!){
    updateProductionRequest(id:$id, input:$input){
        makeOneProductionRequest {
            id
            orderNumber
        }
    }
}
"""

QUERY_MAKE_ONE_REQUEST = """
query getMakeOneRequest($id:ID!){
    makeOneProductionRequest(id:$id){
        id
        name
        sku
        type
        bodyVersion
        requestName
        originalOrderPlacedAt
        orderNumber
        daysUntilFactoryExit
        manualOverride
        factoryOrderUrl
        willExitFactoryBy
        colorPlacedIn3d
        originalOrderPlacedAt
        countReproduce
        sewResource
        reproducedOnesIds
        salesChannel
        belongsToOrder
        brand {
            name
            code
        }
        size {
            code
        }
        style {
            id
            code
            printType
            is3DColorPlacementEnabled
            isStyle3dOnboarded
            version
            body {
                code
                patternVersionNumber
            }
            color {
                code
            }
            material {
                code
                fabricType
            }
        }
     
        orderLineItem {
            id
            channelOrderLineItemId
            createdAt
            order {
                number
                email
                requestType
            }
        }
        dxaNodeInfo(first: 1) {
            dxaNodes {
                id
                number
                name
                escalationEmail
            }
        }
    }
}
"""

# ISSUEs

CREATE_ISSUE = """
mutation createIssueReproduce($input: CreateIssueInput!){
  createIssues(input: $input){
    issues{
      id
    }
  }
}
"""

UPDATE_ISSUE = """
mutation updateIssueReproduce($id: ID!, $input: UpdateIssueInput){
  updateIssue(id: $id, input: $input){
    issue{
      id
    }
  }
}
"""

GET_ISSUE_TYPE = """
query getIssueType($where: IssueTypesWhere!){
    issueTypes(first: 1, where: $where){
        issueTypes{
            id
        }
    }
}
"""

CREATE_ISSUE_TYPE = """
mutation createIssueType($input: CreateIssueTypeInput!){
    createIssueType(input: $input){
        issueType{
            id
        }
    }
}
"""


# ORDER LINE ITEM QUERIES

QUERY_LINE_ITEM = """
query orderLineItem($id:ID!){
    orderLineItem(id:$id){
        id
        customizations {
            id
            value
            styleCustomization {
                id
            }
        }
        order {
            number
        }
        isAnExtraItem
    }
}
"""

UPDATE_LINE_ITEM_MUTATION = """
mutation updateOrderLineItem($id:ID!, $input: UpdateOrderLineItemInput){
    updateOrderLineItem(id:$id, input:$input){
        orderLineItem {
            id
        }
    }
}
"""


UPDATE_FULFILLMENT_ONE_FLOW_QUERY = """
mutation updateFulfillmentOneFlow($id: ID!, $input: OneFlowInput!){
    updateOneFlow(id:$id, input:$input){
        orderLineItem {
            id
        }
    }
}
"""

# STYLE QUERIES
QUERY_STYLE = """
query ($id: ID) {
    style(id: $id) {
        id
        code
        hasBeenMade
        name
        printType
        body {
            name
            numberOfPieces
            numberOfOperations
            images {
                largeThumbnail
            }
            category {
                name
            }
            patternVersionNumber
        }
        color {
            name
            images {
                largeThumbnail
            }
        }
        coverImages {
            url
        }
    }
}
"""

UPDATE_STYLE_MUTATION = """
mutation updateStyle($id:ID!, $input: UpdateStyleInput!){
    updateStyle(id:$id, input:$input){
        style {
            id
            hasBeenMade
        }
    }
}
"""

QUERY_STYLE_MAKE_ONE_READY_STATUS = """
query style($id:ID!, $sizeCode:String){
    style(id:$id){
        id
        makeOneReadyStatus {
            statusBySize(sizeCode:$sizeCode) {
                size {
                    id
                    code
                }
                isMakeOneReady
            }
        }
    }
} 
"""

#
QUERY_MARKERS = """
query searchMarkers($first:Int! $where: MarkersWhere) {
    markers(first:$first, where:$where){
        markers {
            id
            colorCode
            type
            rank
            colorImage {
                url
            }
            bodyPiecesCount
            material {
                code
            }
        }
    }
}
"""

# MISC QUERIES

CREATE_ASSET_REQUEST_MUTATION = """
mutation createAssetRequest($input: CreateAssetRequestInput){
    createAssetRequest(input:$input){
        assetRequests {
            id
        }
    }
}
"""

CREATE_APPLY_COLOR_REQUEST_MUTATION = """
mutation createApplyColorRequest($input: CreateApplyColorRequestInput!){
    createApplyColorRequest(input:$input){
        applyColorRequest {
            id
        }
    }
}
"""

QUERY_BRAND = """
query brand($code:String){
    brand(code:$code){
        id
        code
    }
}
"""

MUTATION_CREATE_QR_CODE = """
mutation createQrCode($input:RequestQrCodeInput){
    requestQrCode(input: $input){
        qrCode {
            id
            type
            wasFileGenerated
        }
    }
}
"""

QUERY_PDF_STYLE_INFO = """
query ($id: ID!) {
    style(id: $id) {
        name
        body {
            images {
                largeThumbnail
            }
        }
        color {
            name
            images {
                largeThumbnail
            }
        }
        coverImages {
            url
        }
        pieceMapping {
            bodyPiece {
                pieceType
            }
        }
        styleBillOfMaterials {
            trimItem {
                longName
            }
        }
    }
}
"""


def update_order_line_item(order_line_item_id, input):
    gql = ResGraphQLClient()

    return gql.query(
        UPDATE_LINE_ITEM_MUTATION,
        {
            "id": order_line_item_id,
            "input": input,
        },
    )


def process_mopr(mopr_id, process_input):
    # this is just an update that also does some crapola to turn image urls into gql image objects
    # realistically we should just merge this into the update operation.
    gql = ResGraphQLClient()
    return gql.query(
        """
        mutation ($id: ID!, $input: ProcessProductionRequestInput!) {
            processProductionRequest(id: $id, input: $input) {
                makeOneProductionRequest {
                    id
                }
            }
        }
        """,
        {
            "id": mopr_id,
            "input": process_input,
        },
    )


def update_mopr(mopr_id, update_input):
    gql = ResGraphQLClient()
    return gql.query(
        UPDATE_MAKE_ONE_REQUEST_MUTATION,
        {
            "id": mopr_id,
            "input": update_input,
        },
    )
