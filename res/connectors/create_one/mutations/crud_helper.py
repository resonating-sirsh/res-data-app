# SA Notes

# I think there us a general template for updating [table_endpoint]
# id, input where input is the dictionary
# if we have an indexed dataframe, we could simple call save(endpoint, dataframe) and infer the mutation template OR overwire if its custom

# e.g. template for 'printFile' or 'assetRequest'

# ADD_ONE_MARKER_MUTATION = """
#     mutation addPrintFile($id:ID!, $input: AddPrintFileInput!) {
#         addPrintFile(id:$id, input:$input){
#             marker {
#                 id
#             }
#         }
#     }
# """

# REMOVE_ONE_MARKER_MUTATION = """
#     mutation removePrintFile($id:ID!, $input: RemovePrintFileInput!) {
#         removePrintFile(id:$id, input:$input){
#             marker {
#                 id
#             }
#         }
#     }
# """

# UPDATE_ASSET_REQUEST_MUTATION = """
#     mutation updateAssetRequest($id:ID!, $input: UpdateAssetRequestInput!){
#         updateAssetRequest(id:$id, input:$input){
#             assetRequest {
#                 id
#                 name
#             }
#         }
#     }
