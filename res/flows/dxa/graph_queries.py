GET_PRINT_ASSET_REQUEST = """
query getPrintAssetRequest($id: ID!){
    printAssetRequest(id:$id){
        id
        material {
            code
        }
        marker {
            id
            rank
                    isCustomized
                    material {
                        code
                    }
                    preparedNetFiles {
                        shouldApplyCutline
                        size {
                            id
                            code
                        }
                        file {
                            id
                            name
                            s3 {
                                key
                                bucket
                            }
                        }
                    }
                    customizablePreparedNetFiles {
                        shouldApplyCutline
                        size {
                            id
                            code
                        }
                        file {
                            id
                            name
                            s3 {
                                key
                                bucket
                            }
                        }
                    }
        }
        makeOneProductionRequest {
            id
            orderNumber
            customizations {
                styleCustomization {
                    id
                    key
                    type
                    font {
                        s3 {
                            key
                        }
                    }
                }
                value
            }
            preparedNetFilesByMaterial {
                marker {
                    id
                }
                material {
                    code
                }
                file {
                    id
                    name
                    s3 {
                        bucket
                        key
                    }
                }
            }
            size {
                code
            }
            style {
                code
                isCustomized
                markers {
                    rank
                    isCustomized
                    material {
                        code
                    }
                    preparedNetFiles {
                        shouldApplyCutline
                        size {
                            id
                            code
                        }
                        file {
                            id
                            name
                            s3 {
                                key
                                bucket
                            }
                        }
                    }
                    customizablePreparedNetFiles {
                        shouldApplyCutline
                        size {
                            id
                            code
                        }
                        file {
                            id
                            name
                            s3 {
                                key
                                bucket
                            }
                        }
                    }
                }
            }
        }
    }
}
"""
