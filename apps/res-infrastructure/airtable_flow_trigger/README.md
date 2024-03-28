This long-running process loops through all config files under "configs",
evaluates the airtable formula against the given table, and then runs a Flow
for each row in the result (or optionally gives all rows to a flow).

This is a replacement for the existing Event Triggers system in Airtable.

To test this, go to https://airtable.com/appu11yom3UIdHF2M/tblfjTT2mVQhsC8vm/viwtjHDtPv9RV1HFv?blocks=hide and make a new row, with a message (and "Triggered" left unchecked). Then open Argo in development (Telepresence or Kube Forwarder) and watch for a job named "examples-example-event-trigger-XXX"


__To add a new flow + trigger__

1. Add your flow under `res-data-platform/res/flows`, making sure it has a function like:
    `def handler(event, context=None)` which will be called by the event triggers.

2. Test your flow using the guide in Platform Documentation

3. Look in `configs/` for a config related to your flow -- if one doesn't exist, you can make a   new one following the examples in other files.

4. Fill in the config for your flow, using the file name you created in step 1 above for the flow name. For the type, either put `for_each` if you want a flow to be triggered for each resulting row from Airtable, or `for_all` if you want all rows to be sent to 1 instance of the flow!

5. WARNING: Make sure your flow has a switch for the development environment to avoid writing back to Airtable!!! You can get the environment and write an if statement like this:

```if os.getenv("RES_ENV") == "production":
    # Write to Airtable```



Open Questions:

- how do we make sure dev/prod don't overlap?
- what if it takes a while to process rows, and a second job gets kicked off?
- will we hit API rate limitations with this?
- should we add a column to every output table, with the name of the flow that last changed it?