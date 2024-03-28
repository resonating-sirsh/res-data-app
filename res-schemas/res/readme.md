# res-meta schema

The res schema is the most abstract schema for well known types at Resonance. 
We have specifications for making things in the design space and assets that are made in assembly/make

- make.assets should correspond to nodes and may be renamed for nodes
- we are rethinking the res.meta entities so they are norm'd and properly managing history tables/slowly changing dimensions
- the same entities can exist in multiple places e.g. rolls are in finance/purchasing and also in make

In this first version we are not trying to model all of Resonance but starting with Make. Make needs some meta items but we ask only the following question

```
what data do we need to make a thing in assembly and monitor its efficient assembly (where is it, how much does it cost, what failed etc.)
```

----

All entities described can be easily sourced from Airtable and piped into kafka and other data systems. The main role of the schema are to allow data interchange via understanding types and type relationships.

---- 

Examples documented in this readme

1. Importing meta data from DXF files in the design space so they can be used for nesting

2. Ingesting legacy Rolls data from print into the new Make data system


## Ingesting styles from CAD files

## Ingesting data from Airtable


