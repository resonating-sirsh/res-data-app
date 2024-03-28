QUERY_SIZES = """
    query getSizes($search: String, $where: SizesWhere){
        sizes(first:100, search: $search,where: $where){
            sizes {
                id
                name
                code
                petiteSize {
                    id
                    name
                    code
                }
            }
        }
    }
"""

QUERY_ALL_SIZES = """
    query getSizes($where: SizesWhere){
        sizes(first:10000, where: $where){
            sizes {
                id
                name
                code
                aliases
            }
        }
    }
"""
