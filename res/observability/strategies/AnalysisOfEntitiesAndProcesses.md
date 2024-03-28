# Beyond search: how to troubleshoot and understand deep relationships between entities

You need to understand that certain entities are referenced in questions and data - check about-resonance

## Understanding Entities at Resonance

You should understand what entity codes look like so you know them when you see them and also understand the causal relationship between entities for understanding processes and issues.

- We keep a key value store of major entities like 1. ONEs (Production Requests), 2. customer orders, 3. rolls for printing on, digital assets for 4. styles and 5. bodies
- Note that a customer orders is broken down into separate items i.e. ONES which are each made (manufactured) separately
- Before we can make something the first time, we need to "onboard" the body and then the style which means preparing digital assets that describe how to make something physically
- These 5 entities have specific formats;
- A ONE number usually has a numeric format like 10333792 - each production request belongs to a specific customer order (see below)
- A Body code usually has a brand prefix and a 4 digit number e.g. `KT-2011`
- A style code is made up of a Body, Material and Color (BMC) component e.g. `KT-2011 CHRST WHITE` has Body=KT-2011, Material=CHRST and Color=White
- When we sell a style in a specific size, it will have a fourth component such as `CF-5002 CC254 AUTHGJ 3ZZMD`. We call these SKUs. They appear in conversation as `CF5002 CC254 AUTHGJ 3ZZMD` without the hyphen but we try to normalize the to have a hyphen
- Customer orders also start with a brand specific code and a number e.g. `KT-45678`. Multiple SKUs are ordered and each one is linked to a production request
You can use this information to understanding entities. You can also make logic inferences such as if a custom order is delayed, you can look into the health of the different production requests, styles and bodies (in that order)

## Example troubleshooting of delays and general route cause analysis

### Contract violations given customer orders

1. You are asked about a customer order. You look up the number e.g. KT-12345 and you find it has three associated one numbers which are the Make production Requests.
2. You then use the entity store to get the statues of each and you find some have contract variables, comments are seemed delayed in some node
3. You report to the user which of the ONEs are causing an issue (see also troubleshooting Production Requests below)

### Production Requests given so-called ONE numbers

1. You are given one or more production requests. You may be told they are part of a customer order
2. You notice that some have not existed a particular node e.g. DXA but you can not see any obvious comments or failing contracts
3. You know that DXA assets relate to bodies and styles and you also have the body cod and style codes for the production requests
4. You use the entity store to lookup the body and style easily and you can the report any contract variables or status information for those
5. If there are contract variables you tell the user this is a likely reason for delays or if it does not seemed to have been recently updated in some DXA node, you tell the user it is probably stuck there
