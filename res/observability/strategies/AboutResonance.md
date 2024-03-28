# About Resonance

Resonance is a fashion technology company that allows for the end-end design, sell and manufacturing (Make) of fashion garments like dresses, shirts, hoodies etc.

Each garment os referred to as a "ONE" and the ONE number is the Manufacturing or Make production request number.
Business units in Resonance are called "Nodes" and processes are called "Flows" - work items on queues according to specified Contract Variables (CVs)
Sometimes in manufacturing there are failures and we may need to remake some components which is called "Healing"
Some of the nodes in order of process are DXA, Make (Assembly (Print, Cut), Sew), and Warehouse
In DXA we do 3D design of garment fit and color placement and in Make we digitally print and cut pieces from chained rolls called Plate Processing Units (PPUs)

Slack and Coda are use for communication and ShortCut is used to manage the Agile software development. There may be functions to lookup these information sources.
The physical manufacturing happens in Santiago Dominican Republic (DR). Garments are made by digitally printing pieces on long rolls of fabric in the Print Node and then cut out as pieces using a laser cutter in the Cut Node.
Pieces, trims and other assets like fusing and stamper pieces are collected in a completer node after cut and then sent to be sewn and shipped from a warehouse to the customer.
Sometimes pieces need to be printed multiple times due to defects and this is called Healing. healing is problematic as it can lead to delays in getting product to customers and also is an expensive in terms of using more resources (materials and ink etc)
Examples of healing reasons are based on physical defects like lines that appear in printing, dirt or distortion.

Some of the most common brands we work with are  THE KIT, Theme, JCRT, Tucker, Blue Glen and others

Use the various statistics and vector searches to learn more but do not call this `about_resonance_company` function more than once (TODO i may remove it from context)
For example:

- Body Development Standards can be looked up in various vector stores under the meta, coda or slack namespaces in that order of preference
- search for specific contracts metadata using the columnar contracts.contracts store and search for contract variables using the contracts.contract_variables or for more general information afterwards use the coda docs.

For state, queues or general current information on bodies, styles, ONES order orders you can use those columnar stores

- use meta.styles to understand the status of the Apply Color Queue and the creation of Meta.ONes
- use the meta.bodies table to understand
- if asked for contracts on other entities like styles, bodies or ONES search those respective stores instead of the contracts namespaces ones
- if asked about orders these are customer "selL" namespace orders. Use the Columnar search for sell.orders
- if asked about production requests or ONEs (in Make) use the make.one search

## Understanding contracts and contract variables

- A coda document on resonance-contracts provide a source of truth on an attempt to defined contracts
- Ongoing conversations about contracts can be found in the slack channel
- Some specific contracts if requested can be found in the columnar and entity stores but this is best used to answer specific and not general questions

## Understanding Entities at Resonance

You should understand what entity codes looked like so you know them when you see them and also understand the causal relationship between entities for understanding processes and issues.

- We keep a key value store of major entities like 1. ONEs (Production Requests), 2. customer orders, 3. rolls for printing on, digital assets for 4. styles and 5. bodies
- Note that a customer orders is broken down into separate items i.e. ONES which are each made (manufactured) separately
- Before we can make something the first time, we need to "onboard" the body and then the style which means preparing digital assets that describe how to make something physically
- These 5 entities have specific formats;
- A ONE number usually has a numeric format like 10333792 - each production request belongs to a specific customer order (see below)
- A Body code usually has a brand prefix and a 4 digit number e.g. `KT-2011`
- A style code is made up of a Body Material and Color (BMC) component e.g. `KT-2011 CHRST WHITE` has Body=KT-2011, Material=CHRST and Color=White
- When we sell a style in a specific size, it will have a fourth component such as `CF-5002 CC254 AUTHGJ 3ZZMD`. We call these SKUs. They appear in conversation as `CF5002 CC254 AUTHGJ 3ZZMD` without the hyphen but we try to normalize the to have a hyphen
- Customer orders also start with a brand specific code and a number e.g. `KT-45678`. Multiple SKUs are ordered and each one is linked to a production request
You can use this information to understanding entities. You can also make logic inferences such as if a custom order is delayed, you can look into the health of the different production requests, styles and bodies (in that order)

For more details on entities and processes, lookup_strategy for Analysis of Entities and PRocesses
