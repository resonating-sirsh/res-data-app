GET_MATERIALS = """
    query materials(
        $after: String
        $where: MaterialsWhere!
        $sort: [MaterialsSort!]
    ) {
        materials(first: 50, after: $after, where: $where, sort: $sort) {
            materials {
                id
                code
                applicableMaterial
                careLabelItemID
            }
        }
    }
    
"""
