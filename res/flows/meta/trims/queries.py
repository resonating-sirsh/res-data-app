QUERY_GET_ONE_REQUEST = """
    query getMakeOneProductionRequest($id: ID!){
        makeOneProductionRequest(id:$id){
            name
            orderNumber
            resPackUrl
            manualOverride
            exitFactoryDate
            hasPaperControl
            hasStamper
            style{
                id
            }
            brand {
                name
            }
            size {
                name
            }
            body{
                code
            }
            orderNumber
            id
            type
            style{
                version
                id
                code
                material {
                    code
                }
                trimNotes
            }
            sku
            productionCoverPhotos {
                url
            }
            makeOrderPdf {
                url
            }
            binLocations{
                name
            }
        }
    }
"""

GET_MATERIAL_CARE = """
    query getMaterialInfo($where: MaterialsWhere){
      materials(first: 1, where:$where){
        materials{
          id
          washCare
          dryCare
          ironCare
          content
        }
      }
    }
"""

GET_BILL_OF_MATERIAL = """
    query getBillOfMaterial($where: BillOfMaterialsWhere) {
        billOfMaterials(first: 100, where: $where) {
            billOfMaterials {
                id
                quantity
                name
                type
                code
                body
                trimCategory
                trimTaxonomy {
                    id
                }
            }
        }
    }
"""

GET_STYLE = """
    query getBillOfMaterial($where: StylesWhere){
    styles(first: 100, where: $where) {
        styles {
          id
          name
          isMainLabelWithSizeEnabled
          brand {
            name
            code
            isMainLabelWithSize
            sizeLabelColor
          }
          styleSpecificConstructionPack{
            id
            url
          }
          styleBillOfMaterials {
              id
              billOfMaterial {
                id
                unit
                size
                sizeScale
                quantity
                name
                type
                code
                body
                styleBomFriendlyName
                trimCategory
                trimTaxonomy {
                    id
                    name
                }
                trim {
                  images{
                    url
                  }
                }
            }
            trimItem {
              id
              longName
              name
              skuCode
            }
          }
        }
    }
}
"""

CREATE_PREP_REQUEST = """
    mutation createPrepRequest($input: CreatePrepRequestInput!) {
      createPrepRequest(input: $input) {
        prepRequest {
          id
        }
      }
    }
"""

DELETE_PICK_TICKET = """
    mutation deletePickTicket($id: ID!){
        deletePickTicket(id: $id){
            prepPickTicket{
                id
            }
        }
    }
"""

GET_PREP_REQUEST = """
    query getPrepRequestById($id: ID!){
      prepRequest(id: $id){
        id
        oneNumber
        makeOneProductionId
        styleCode
        size
      }
    }
"""

GET_MATERIAL = """
    query getMaterialsPickTicket($where: MaterialsWhere!) {
        materials(first: 100, where: $where) {
            materials {
                id
                name
                longName
                developmentStatus
            }
        }
    }
"""

GET_TAXONOMY = """
    query getPurchasingTrimTaxonomy(
        $where: PurchasingTrimTaxonomiesWhere!
        $search: String
    ) {
        purchasingTrimTaxonomies(first: 100, where: $where, search: $search) {
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
                    size
                    trimColorHexCode
                    trimColorName
                    skuCode
                    developmentStatus
                    brand {
                        id
                        code
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
                            size
                            brand {
                                id
                                code
                            }
                            developmentStatus
                            fabricType
                            trimColorHexCode
                            trimColorName
                            skuCode
                            trimLength
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

GET_POSTGRES_BRAND = """
    SELECT 
        b.id,
        b.active_subscription_id,
        b.is_brand_whitelist_payment,
        b.name,
        b.meta_record_id,
        b.fulfill_record_id
    FROM sell.brands as b
    WHERE b.brand_code = %s
"""

GET_ALL_POSTGRES_TRIMS = """
    SELECT 
        t.id, 
        t.name,
        t.status,
        t.type,
        t.sell_brands_pkid,
        t.supplied_type,
        t.airtable_color_id,
        t.order_quantity,
        t.airtable_size_id,
        t.airtable_trim_taxonomy_id,
        t.hash,
        t.expected_delivery_date,
        t.available_quantity,
        t.warehouse_quantity,
        t.trim_node_quantity,
        t.order_quantity
    FROM meta.trims as t
    INNER JOIN sell.brands as b ON b.id = t.sell_brands_pkid
"""

INSERT_POSTGRES_TRIM = """
INSERT INTO meta.trims 
(
    name,
    sell_brands_pkid,
    status,
    type,
    supplied_type,
    airtable_color_id,
    image_url,
    order_quantity,
    airtable_size_id,
    airtable_trim_taxonomy_id,
    hash,
    expected_delivery_date
) 
VALUES 
(
    %(name)s,
    %(sell_brands_pkid)s,
    %(status)s,
    %(type)s,
    %(supplied_type)s,
    %(airtable_color_id)s,
    %(image_url)s,
    %(order_quantity)s,
    %(airtable_size_id)s,
    %(airtable_trim_taxonomy_id)s,
    %(hash)s,
    %(expected_delivery_date)s
)
"""

UPDATE_POSTGRES_TRIM = """
    UPDATE meta.trims
    SET
        name = %(name)s,
        sell_brands_pkid = %(sell_brands_pkid)s,
        status = %(status)s,
        type = %(type)s,
        supplied_type = %(supplied_type)s,
        airtable_color_id = %(airtable_color_id)s,
        image_url = %(image_url)s,
        order_quantity = %(order_quantity)s,
        available_quantity = %(available_quantity)s,
        trim_node_quantity = %(trim_node_quantity)s,
        warehouse_quantity = %(warehouse_quantity)s,
        airtable_size_id = %(airtable_size_id)s,
        airtable_trim_taxonomy_id = %(airtable_trim_taxonomy_id)s,
        expected_delivery_date = %(expected_delivery_date)s,
        hash = %(hash)s
    WHERE id = %(id)s
"""

INSERT_TRIM_TAXONOMY = """
    INSERT INTO meta.trim_taxonomy
    (
        name,
        type,
        friendly_name,
        airtable_record_id,
        airtable_trim_size
    )
    VALUES
    (
        %(name)s,
        %(type)s,
        %(friendly_name)s,
        %(airtable_record_id)s,
        %(airtable_trim_size)s
    )
"""

GET_TRIM_TAXONOMY_POSTGRES = """
    SELECT 
        id,
        name,
        type,
        friendly_name,
        airtable_record_id,
        airtable_trim_size
    FROM meta.trim_taxonomy    
"""

UPDATE_TRIM_TAXONOMY = """
    UPDATE meta.trim_taxonomy
    SET
        name = %(name)s,
        type = %(type)s,
        friendly_name = %(friendly_name)s,
        airtable_record_id = %(airtable_record_id)s,
        airtable_trim_size = %(airtable_trim_size)s,
        parent_meta_trim_taxonomy_pkid = %(parent_meta_trim_taxonomy_pkid)s
    WHERE id = %(id)s
"""
