"""
    Queries and mutations for use in the resmagic GraphQL API
"""

GET_STYLE_COSTS = """
        query getPricing($after:String, $where: StylesWhere) {
            styles(first:10, after:$after, where:$where) {
                styles {
                    id
                    code
                    name
                    onePrices {
                        cost
                        margin
                        price
                        priceBreakdown {
                            item
                            category
                            rate
                            quantity
                            unit
                            cost
                        }
                        size {
                            id
                            name
                            code
                        }
                    }
                }
            count
            cursor
            }
        }   
    """

# Within create.one, the following query is used in order to produce the trims
# table for users

# query getBillOfMaterialByBodyCode($where: BillOfMaterialsWhere!) {
#     billOfMaterials(first: 100, where: $where) {
#         billOfMaterials {
#             id
#             styleBomFriendlyName
#             trimLength
#             trimTaxonomy {
#                 id
#                 name
#             }
#             trimQuantity
#             unit
#             trim {
#                 id
#                 images {
#                     id
#                     url
#                 }
#             }
#         }
#     }
# }
#
# This query captures that information plus additional IDs for linking data to
# various data sources. ID definitions below:
# - styles.id: record ID for a style within the create styles table in the
# res.magic airtable base
# - styles.styleBillOfMaterials.billOfMaterial.Id: bill of material record ID
# for the res.meta.one base
# - styles.styleBillOfMaterials.billOfMaterial.trimTaxonomyId: trim taxonomy
# record ID for the res.meta.one base
# - styles.styleBillOfMaterials.billOfMaterial.trim.id: trim record ID for the
# res.meta.one base

GET_STYLE_TRIMS = """
        query Styles($after:String, $where: StylesWhere) {
            styles(first:10, after:$after, where:$where) {
                styles {
                    id
                    code
                    name
                    styleBillOfMaterials {
                        billOfMaterial {
                            id                            
                            trimTaxonomyId
                            name
                            styleBomFriendlyName
                            trimLength
                            trimQuantity
                            type
                            unit
                            trim {
                                id
                            }
                        }                    
                    }
                }
            count
            cursor
            }
        }   
    """
