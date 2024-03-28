GET_STYLE = """
    query style($id: ID!){
        style(id:$id){
            id
            createdAt
            code
            version
            printType
            isReadyForSample
            styleReadyForSampleStatus
            color{
                id
            }
            brand {
                id
                code
                name
                materialMainLabel{
                  id
                }
            }
            material {
                id
                code
            }
            body{
                id
                code
                isReadyForSample
                readyForSampleStatus
                basePatternSize {
                  id
                  code
                }
                pieces{
                    id
                    code
                    pieceTypeCode
                }
                onboardingMaterials{
                    id
                    code
                }
                onboardingComboMaterials{
                    id
                    code
                }
                onboardingLiningMaterials{
                    id
                    code
                }
            }
        }
    }
"""

GET_STYLES = """
    query styles($where: StylesWhere, $search: String) {
      styles(
        first: 100
        where: $where
        search: $search
        sort: [{ field: CREATED_AT, direction: DESCENDING }]
      ) {
        styles {
          id
          code
          printType
        }
      }
    }
"""

CREATE_STYLE = """
    mutation createStyle($input: CreateStyleMutation!) {
        createStyle(input: $input) {
            style {
                id
                code
            }
        }
    }
"""

UPDATE_STYLE = """
    mutation updateStyle($id: ID!, $input: UpdateStyleInput!){
        updateStyle(id:$id, input:$input){
            style {
            id
            code
            printType
            isReadyForSample
            styleReadyForSampleStatus
            body {
                id
                isOneReady
                isReadyForSample
                readyForSampleStatus
            }
            stylePiecesMappingStatus {
                isMakeOneReady
                reasons
                internalReasons
            }
          }
        }
    }
"""

UPDATE_STYLE_PIECES = """
    mutation updateStylePieces2($id: ID!, $stylePieces: [UpdateStylePiecesTypedInput!]) {
        updateStylePieces(id: $id, stylePieces: $stylePieces) {
            style {
                id
                code
                isReadyForSample
                brand{
                    id
                    code
                    name
                }
                styleReadyForSampleStatus
                body{
                    id
                    code
                    isReadyForSample
                }
                stylePiecesMappingStatus {
                    isMakeOneReady
                    reasons
                    internalReasons
                }
                pieceMapping{
                    bodyPiece{
                        code
                    }
                    material{
                        code
                    }
                    color{
                        code
                    }
                
                }
            }
        }
    }
"""


GET_BILL_OF_MATERIAL_BY_BODY_CODE = """
query getBomByBodyCode($where: BillOfMaterialsWhere) {
  billOfMaterials(first: 100, where: $where) {
    billOfMaterials {
      id
      styleBomFriendlyName
      trimTaxonomy {
        id
      }
    }
  }
}
"""

GET_ITEM_TRIM_TAXONOMY = """
query getPurchasingTrimTaxonomy(
  $where: PurchasingTrimTaxonomiesWhere!
  $search: String
) {
  purchasingTrimTaxonomies(first: 10, where: $where, search: $search) {
    purchasingTrimTaxonomies {
      id
      name
      metaOneId
      attachments {
        id
        url
      }
      materials {
        id
        name
        key
        code
        fabricType
        trimColorHexCode
        trimColorName
        skuCode
        trimPhysicalProperty
        trimType
        trimSecondaryComponent
        brand {
          id
          code
          name
        }
        trimLength
        images {
          id
          url
          fullThumbnail
        }
      }
      childTaxonomy(first: 100) {
        purchasingTrimTaxonomies {
          id
          name
          metaOneId
          attachments {
            id
            url
          }
          materials {
            id
            name
            key
            code
            trimPhysicalProperty
            trimType
            trimColorHexCode
            trimColorName
            brand {
              id
              code
              name
            }
            fabricType
            trimColorHexCode
            skuCode
            trimLength
            trimSecondaryComponent
            images {
              id
              url
              fullThumbnail
            }
          }
        }
      }
    }
  }
}
"""

CREATE_STYLE_BILL_OF_MATERIAL = """
    mutation createStyleBillOfMaterials($input: CreateStyleBillOfMaterialsInput){
        createStyleBillOfMaterials(input: $input){
            styleBillOfMaterials{
                id
            }
        }
    }
"""

GET_STYLE_BILL_OF_MATERIAL = """
query getStyleBoM($where: StyleBillOfMaterialsWhere!) {
  styleBillsOfMaterials(first: 100, where: $where) {
    styleBillsOfMaterials {
      id
      billOfMaterial {
        id
        styleBomFriendlyName
      }
      trimItem {
        id
        longName
      }
    }
  }
}
"""
