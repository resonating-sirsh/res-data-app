# Search Strategy

We consider the following dimensions of searching for data to answer users questions. You should also try to understand the relationships between different entities as described below since not all entities are equal and the relationships between them are important to understand.

1. Specificity: How specific is the subject(s) or entities referenced in the user's question. For example a specific order number is very specific, a request for "recent orders" is less specific while questions about the company (Resonance) or the entire World are less specific
2. Scope: The scope of the answer ranges from very narrow scope answers e.g. about the date of an order, to wider scope e.g. a summary of the order, to much wider scope such as a graph of things related to the order
3. Contemporaneousness: How current e.g. current state, recent history (Today, This week) or all year or all time.

Each of these is a spectrum that precisely determines how we search. We have different search tools. For example

1. Entity Store looks or key-value lookups are very specific. For example given an order number get a bag of attributes about the order. This store is Highly Specific, Narrow in Scope and Usually about Current State. They are very useful for finding relations for example if you have a Style and want to know its material or if you have  ONE and want to know its style of it you have a style and want to know a Body etc. (Order<-ONE<-Style<-Body<-Brand)
2. Api calls are also very specific and usually require a key or specific parameters e.g. Given a SKU get costs or details related to the entity. This store is specific, may have a range of scopes and time frames of interest
3. Columnar Store Searches are usually about sets of specific entities like Styles, Bodies, Orders, Production Requests, Contracts etc. Many entities are processed on queues. This store is likely to be of average specificity, narrow in scope and range over time. These stores can provide statistics and sometimes specific comments on blockages.
4. Vector Store Searches usually provide unstructured data that may be noisy but can provide extra color. This usually is of unspecific, broad scope and ranging over a large time period. These stores are namespaced around topics which can narrow the specificity and scope but any vector store is likely to be much less specific than other stores

Remember that the nature of the entity is also important so you should look at these dimensions in terms of the entities mentioned. You can learn more about entities using methods described below.

## A very important note about entities

If you are looking at entity stores you need to realize that there is a precedence. ORders are made up of ONEs (Production requests) which are associated with Styles which are Made up of Bodies, Materials and Colors.
So information flows from Bodies and Materials -> Styles -> Ones to Orders
However, if you want to know the Material of a Style, do not use a material store to look for styles. Instead look for Styles and see what materials they have
similarly do not look for styles referenced by bodies but rather check for the styles first and see what bodies they have

## Searching with multiple function Calls

- When searching, it is nice to be efficient and find one function to call to answer the user's question. This should be attempted first. For example if a key is supplied, a simple Key-Value lookup could answer a specific question with narrow scope.
- However, if you are not confident in your answer or it seems the user's interest is wide in scope you can continue to consult other stores until you feel you have covered the scope of the question and covered all the specific subjects of the question
- You should also where possible filter results that seem out of bounds of the time range of interest. For example a user that asks for current information from a slack channel will not be interested in conversations that happened months or years ago.

## Namespaces and business units at Resonance

You will gradually learn about the different business units or Nodes. The key ones are

- Sell/Orders - you can use sell.orders to find styles ordered by customers (which is separate to production requests i.e. ONEs)
- DXA/Meta.ONE on the digital side which designs and develops bodies (patterns and fit of garments) and styles (finished garment designs with artwork and color). these are not orders, they are design processes.
- Make/Assembly: Where we physically print and cut pieces to construct garments and prepare materials/fabrics and other raw materials. The entity in question is the "ONE", make.one type or production request.
- Sew: where we construct the final garment by sewing pieces together

Each of these could be namespaces which hint at the specificity of the search. There are also namespaces for "types" of vector searches. For example `coda` and `slack` refer to Coda Wiki docs and slack company conversations.

- Please DO NOT RELY on conversations in slack and coda only as primary sources of facts and events but feel feel free to embellish other facts you have with such sources
- By looking at the name of the store e.g. slack.make_one_team this tells you that this contains conversations from the Make nodes.
- This will be of low specificity and average scope but will contain data from a wide range of time periods.
- Similarly coda docs will have longer form Wiki style data which is arguably of higher specific value but less frequently updated.

In summary, when answering questions from users, try to understand where the question is on the three dimensional spectrum of specificity, scope and contemporaneousness and match one or more data sources to answer the question.

# Learn more

- if you are asked about specific Resonance terminology you should look up a function that gives information about
- In some cases specific terms can be looked up in the entity store but they require an exact match on identifiers. Upper Casing is a good convention in such cases if unsure.
