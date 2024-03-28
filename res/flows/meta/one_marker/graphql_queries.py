FLAG_APPLY_COLOR_REQUEST_MUTATION = """
mutation flagApplyColorRequest($id: ID!, $input:FlagApplyColorRequestInput!){
    flagApplyColorRequest(id:$id, input:$input){
        applyColorRequest {
            id
            key
            isFlaggedForReview
        }
    }
}
"""

UNFLAG_APPLY_COLOR_REQUEST_MUTATION = """
mutation unflagApplyColorRequest($id: ID!, $input: UnflagApplyColorRequestInput!) {
    unflagApplyColorRequest(id: $id, input: $input) {
        applyColorRequest {
            id
            isFlaggedForReview
        }
    }
}
"""
