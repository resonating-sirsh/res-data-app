# res-connect

Res-connect provides an API interface to many of the connectors in res-data such as the connectors for airtable or argo. For example the webhook functionality of requires exposing endpoints while the argo functionality requires a way to submit tasks via the argo connector over test. These some features can typically be used within the res-data-platform and the res-connect exposes these features when necessary.



## Getting Meta ONE and Style costs

You need to get a bearer token from AWS Secrets Manager - looked for `RES_CONNECT_TOKEN`. This is not recycled now but in future it may be so your code will ideally fetch it live
You will see in the code that the one and style costs requires this bearer token. For example you can test this in Postman or similar by posting a request with the bearer token.

## Style costs route
We refer to these as costs but they are really cost variables such as material or ink consumption. The route `/meta/style-costs` accepts a post.
Generally a 4 part SKU should be used when making requests as three part skus or resonance codes only provide a meta object that does not have costs per se.
However for convenience you can request details for the base/sample size as shown below

```
{
    "sku":  "TK-3080 CHRST CORAFA"
    "use_base_size": true
}
```

This will provide some details including a presigned s3 url (timeouts after 30 minutes) for the GLB 3D file, style name and other attributes for the style.
We also have various facts about how much the ONE consumes when 1-nesting (nesting a ONE by itself) and an estimate about how much can be saved by comparing a 1-nest with a simulated nest of 50 of these ONEs.
This value is called `material_saving_pct_estimate` which will typically be in the region of 0 and 20%.

In order to get derived savings e.g. water and chemical usage, there are parameters external to ONE platform that are used to estimate costs due to digital printing. Suppose we save 20% on material.
We can then say what someone else would use. See [Savings notes](https://coda.io/d/_dZNX5Sf3x2R/Savings-Note_sue9e?utm_source=slack) for forms. Usage here is to take the value of what we save `material_saving_pct_estimate`. Lets call it `s` for savings. Then "their" inefficiency (or how much some other legacy process uses compared to us) is `p=100*(1-s)`. We can use this value with other things to describe how much less water etc. we use as described in [Savings notes](https://coda.io/d/_dZNX5Sf3x2R/Savings-Note_sue9e?utm_source=slack).


### using requests to get the data
```
from res.utils.secrets import secrets 
import requests

#get the token by this or other means from AWS secrets manager
TOKEN = secrets.get_secret('RES_CONNECT_TOKEN')

data = requests.post("https://data.resmagic.io/res-connect/meta/style-costs",
              #use base size or a 4 part SKU
              json={
                        "sku": "TK-3080 CHRST CORAFA",
                        "use_base_size": True
                    },
              #pass the token
             headers= {"Authorization": f"Bearer {TOKEN}"})
data.json()
```
