GET_IMAGE_URL = """
query getImageUrl($id: ID!){
  file(id: $id){
    id
    thumbnail(size: 1024){
      url
    }
  }
}
"""
