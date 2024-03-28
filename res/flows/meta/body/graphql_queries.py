GET_BODY = """
query body($number: String) {
  body(number: $number) {
    id
    code
    isOneReady
    isReadyForSample
    readyForSampleStatus
    patternVersionNumber
    basePatternSize{
      id
      code
    }
    activeBodyOneReadyRequest{
      id
    }
    onboardingMaterials{
      id
      code
    }
    brand{
      id
      code
    }
  }
}

"""

CREATE_BODY = """
    mutation createBody($input: CreateBodyInput!) {
        createBody(input: $input) {
            body {
                id
                name
                code
                brand{
                  id
                  code
                }
            }
        }
    }    
"""
