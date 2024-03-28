GET_ASSET_REQUEST = """
query getAssetRequest($id: ID!){
    assetRequest(id:$id){
        id
        name
        autoApproveColorOnShapePieces
        numberOfPiecesInMarker
        totalPiecesInNetFile
        size {
            id
            code
        }
        style {
            id
            code
            version
            body {
                code
                patternVersionNumber
            }
            color {
                code
            }
        }
        marker {
            id
            rank
            material {
                code
            }
        }
        preparedNetFile {
            id
            s3 {
                key
                bucket
            }
        }
        assignee {
            id
            email
        }
    }
}
"""

GET_MATERIAL_QUERY = """
query getMaterial($code:String) {
    material(code:$code) {
        id
        code
        printFileCompensationLength
        printFileCompensationWidth
        cuttableWidth
    }
}
"""

GET_STYLE_PIECES = """
query getStylePieces($where:StylesWhere){
  styles(first:10, where: $where){
    hasMore, count,
    styles{
      id
      body {
        code
        name
        patternVersionNumber
        pieces {
          name
          code
        }
      }
      material {
        name
        code
      }
      stylePieces {
        material {
          name
          color
          code
        }
        bodyPiece {
          id
          name
          code
        }
      }
    }
  }
}
"""

# TODO: SHould use `gerber_size` or whatever digital_eng eventually calls it. dxf.py
GET_SIZES = """
query getSize($size_code: String) {
	size(code:$size_code) {
    name,
    code,
  }
}
"""


# ===== Mutations =====

UPDATE_ASSET_REQUEST_MUTATION = """
mutation updateAssetRequest($id:ID!, $input: UpdateAssetRequestInput!){
    updateAssetRequest(id:$id, input:$input){
        assetRequest {
            id
            autoApproveColorOnShapePieces
            numberOfPiecesInMarker
            totalPiecesInNetFile
            size {
                id
                code
            }
            style {
                id
                code
                version
                body {
                    code
                    patternVersionNumber
                }
                color {
                    code
                }
            }
            marker {
                id
                rank
            }
            preparedNetFile {
                id
                s3 {
                    key
                    bucket
                }
            }
        }
    }
}
"""


UPDATE_MARKER_PIECES_MUTATION = """
mutation addColorOnShapePieces($id:ID!, $input: UpdateColorOnShapePiecesInput){
    addColorOnShapePieces(id:$id input:$input){
        marker {
            id
        }
    }
}
"""


APPROVE_COLOR_ON_SHAPE_PIECES = """
mutation approvePieces($id: ID!) {
    approveColorOnShapePiecesCollection(id: $id) {
        assetRequest {
            id
        }
    }
    }
"""


SET_COLOR_ON_SHAPE_PIECES_MUTATION = """
mutation setColorOnShapePiecesInformation($id:ID!, $input: SetColorOnShapeInformationInput!){
    setColorOnShapePiecesInformation(id:$id, input:$input){
        assetRequest {
            id
            autoApproveColorOnShapePieces
            numberOfPiecesInMarker
            totalPiecesInNetFile
            size {
                id
                code
            }
            style {
                id
                code
                version
                body {
                    code
                    patternVersionNumber
                }
                color {
                    code
                }
            }
            marker {
                id
                rank
            }
            preparedNetFile {
                id
                s3 {
                    key
                    bucket
                }
            }
        }
    }
}
"""

FLAG_ASSET_REQUEST = """
mutation flagAssetRequest($id:ID!, $input: FlagAssetRequestInput){
    flagAssetRequest(id:$id, input:$input){
        assetRequest {
            id
        }
    }
}
"""
