CREATE_BODY_ONE_READY_REQUEST = """
mutation createBodyOneReadyRequest(
  $input: CreateBodyOneReadyRequestInput!
) {
  createBodyOneReadyRequest(input: $input) {
    bodyOneReadyRequest {
      id
      status
    }
  }
}
"""

GET_BODY_ONE_READY_REQUEST = """
query bodyOneReadyRequest($id: ID!) {
  bodyOneReadyRequest(id: $id) {
    id
    status
    bodyNumber
    bodyVersion
    orderFirstOneStatus
    registerNewBodyVersionStatus
    isReadyForSample
    firstOneStyle{
      id
      code
      printType
      isReadyForSample
      body{
        id
        code
      }
      material{
        id
      }
      color{
        id
      }
    }
    firstOneOrder{
      id
    }
    orderFirstOneMode
  }
}

"""

GET_BODY_ONE_READY_REQUESTS = """
query bodyOneReadyRequests($where: BodyOneReadyRequestsWhere $first: Int! $sort:[BodyOneReadyRequestSort!] $after:String) {
  bodyOneReadyRequests(where: $where, first: $first sort:$sort after:$after) {
    bodyOneReadyRequests {
      id
      status
    }
  }
}
"""

UPDATE_BODY_ONE_READY_REQUEST = """
mutation updateBodyOneReadyRequest(
  $id: ID!
  $input: UpdateBodyOneReadyRequestInput!
) {
  updateBodyOneReadyRequest(id: $id, input: $input) {
    bodyOneReadyRequest {
      id
      status
      orderFirstOneStatus
      isReadyForSample
      firstOneStyle{
        id
        code
        printType
        isReadyForSample
        body{
          id
          code
        }
        material{
          id
        }
        color{
          id
        }
      }
    }
  }
}
"""

QUERY_LABEL = """
query label($id: String!) {
  label(id: $id) {
    id
    trimTaxonomyEntityId
  }
}
"""

QUERY_BODY_BILL_OF_MATERIALS = """
query getBodyBoms($id:String){
  body(entityId: $id) {
    id
    code
    name
    billOfMaterials {
      id
      name
      trimTaxonomy{
        id
        name
      }
      trimLength
      trimQuantity
      trimTaxonomyUsage
      fusingId
      pieceType
      bodyPiecesIds
    }
  }
}
"""

CREATE_BILL_OF_MATERIAL = """
mutation createBillOfMaterial ($input:CreateBillOfMaterialInput!){
  createBillOfMaterial(input:$input){
    billOfMaterial{
      id
      code
    }
  }
}
"""

UPDATE_BODY = """
    mutation updateBody($id: ID!, $input: UpdateBodyInput!){
        updateBody(id:$id, input:$input){
            body {
                id
            }
        }
    }
"""
