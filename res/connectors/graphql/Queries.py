class Queries:

    GET_ASSET_REQUEST = """
        query getAssetRequest($id: ID!){
            assetRequest(id:$id){
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
        """

    GET_MATERIAL_QUERY = """
        query getMaterial($code:String) {
            material(code:$code) {
                id
                code
                printFileCompensationLength
                printFileCompensationWidth
                cuttableWidth
                offsetSizeInches
            }
        }
    """
