GET_STYLES = """
    query getStyles($where: StylesWhere $after: String){
    styles(first:100, where:$where, after:$after){
       styles {
            id
            code
            brandCode
            shopifyOption1Name
            shopifyOption2Name
            shopifyOption3Name
            shopifyOption1Value
            shopifyOption2Value
            shopifyOption3Value
            availableSizes{
                id
                code
                name
                category
                ecommerceTag
                sizeNormalized
            }
        }
        cursor
        hasMore
        count
    }
}
"""

GET_PRODUCTS = """
    query getProducts($where: ProductsWhere $after: String){
        products(first: 100, where: $where, after: $after){
            products {
                id
                ecommerceId
                storeCode
                price
                style{
                    id
                    availableSizes{
                        id
                        category
                    }
                }
            }
            cursor
            hasMore
            count
        }
    }
"""

GET_BRAND_SIZE = """
    query getBrandSize($where: BrandSizesWhere){
        brandSizes(first: 1, where: $where){
            brandSizes{
                id
                brandSizeName
            }
        }
    }
"""

GET_SIZE = """
    query getSize($id: ID $code: String){
        size(id: $id, code: $code){
            id
            code
            name
            category
            ecommerceTag
            sizeNormalized
        }
    }
"""

CREATE_BRAND_SIZE = """
    mutation brandSize($input: BrandSizeInputMutation){
        brandSize(input: $input){
            brandSize{
                id
                brandSizeName
            }
        }
    }
"""
