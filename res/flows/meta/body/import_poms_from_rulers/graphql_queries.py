GET_BODY = """
    query getBody($number: String! $bodyVersion:Int!) {
        body(number: $number) {
            id
            code
            pomFile3d(bodyVersion: $bodyVersion){
              file{
                url
                uri
              }
            }
            availableSizes{
                id
                name
            }
        }
    }
"""

ADD_POM_JSON_3D = """
mutation addPomJson3d($code: String!, $bodyVersion:Int!, $input: AddPomJsonInput!){
  addPomJson3d(code:$code bodyVersion:$bodyVersion, input:$input){
    bodyPointsOfMeasure{
      id
      body{
        code
      }
      errors
      warnings
    }
  }
}
"""
