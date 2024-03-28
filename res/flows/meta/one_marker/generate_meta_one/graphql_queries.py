QUERY_APPLY_COLOR_REQUEST = """
    query getApplyColorRequest($id: ID!) {
        applyColorRequest(id: $id) {
            id
            colorApplicationMode
            turntableRequiresBrandFeedback
            colorType
            bodyVersion
            assetsGenerationMode
            bodyChangesValidatedAt
            areBodyChangesInsignificant
            turntableZipFile {
                uri
            }
            style {
                id
                code
                isStyle3dOnboarded
                simulationBrandFeedback
                stylePieces {
                    bodyPiece {
                        id
                        code
                    }
                    material {
                        code
                    }
                    color {
                        code
                    }
                    artwork {
                        id
                        color {
                            code
                        }
                    }
                }
                body {
                    code
                }
                color {
                    code
                }
                material {
                    code
                }
                pieceMapping{
                    bodyPiece{
                        id
                        code
                    }
                    material{
                        code
                        offsetSizeInches
                    }
                    artwork{
                        id
                        file{
                            s3{
                                key
                                bucket
                            }
                        }      
                    }
                    color{
                        code
                    }
                }
            }
        }
    }
"""

UPDATE_APPLY_COLOR_REQUEST_MUTATION = """
    mutation updateApplyColorRequest($id: ID!, $input: UpdateApplyColorRequestInput!){
        updateApplyColorRequest(id:$id, input:$input){
            applyColorRequest {
                id
                bodyVersion
                assetsGenerationMode
                bodyChangesValidatedAt
                areBodyChangesInsignificant
                exportedPiecesFile {
                    key
                    uri
                }
                style {
                    code
                    id
                    body {
                        code
                    }
                    color {
                        code
                    }
                    pieceMapping {
                        bodyPiece {
                            code
                        }
                        material{
                            code
                            offsetSizeInches
                        }
                    }
                    material {
                        code
                        offsetSizeInches
                    }
                }
            }
        }
    }
"""

QUERY_SIZES = """
    query getSizes($search: String){
        sizes(first:100, search: $search){
            sizes {
                id
                name
                code
            }
        }
    }
"""


UPDATE_STYLE_MUTATION = """
    mutation updateStyle($id:ID!, $input: UpdateStyleInput!){
        updateStyle(id:$id, input:$input){
            style {
                id
                simulationStatus
            }
        }
    }
"""

QUERY_STYLE_MAKE_ONE_READY_STATUS = """
    query styleMakeOneReadyStatusV2($id: ID) {
        style(id: $id) {
            id
            makeOneReadyStatusV2{
            isMakeOneReady
            contractsFailing
           }
        }
    }
"""
