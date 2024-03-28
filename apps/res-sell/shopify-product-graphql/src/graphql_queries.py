UPDATE_PRODUCT_MUTATION = """
    mutation updateProduct($id: ID!, $input: UpdateProductInput!) {
        updateProduct(id: $id, input: $input){
            product{
                id
                ecommerceId
                title
            }
             
        }
    }
"""

GET_PRODUCT_QUERY = """
    query products($where: ProductsWhere) {
        products(first: 10, where: $where){
            products{
                id
                brandCode
                ecommerceId
                title
                style{
                    id
                    isLocked
                }
                updatedAt
                isLive
            }
        }
    }
"""

GET_BODY_QUERY = """
    query body($number: String!) {
        body(number: $number){
            id
            name
        }
    }
"""

GET_STYLE = """
    query getStyle($id: ID!){
        style(id: $id){
            id
            isLocked
        }
    }
"""

UPDATE_STYLE = """
    mutation updateStyle($id: ID!, $input: UpdateStyleInput!) {
        updateStyle(id: $id input: $input) {
            style {
                id
            }
        }
    }
"""

SHOPIFY_GET_STORE_PUBLICATION = """
query publicationFromProducts($productId: ID!) {
    product(id: $productId){
        id
        resourcePublications(first: 25, onlyPublished: true) {
            edges {
                node{
                    publication{
                        id
                        name
                    }
                    publishDate
                }
            }
        }
    }
}
"""
