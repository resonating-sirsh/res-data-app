------
-- for each asset type like a body, style or one, we can also have a queue state
-- for bodies its the body one requests queue, for ones its the make order request queue
-- on these queues we manage the node states and there is a logic on the queue items (interface) for managing core states
-- node, status, deltas, contracts failing etc 
-- whenever we update the asset we can also update the queue status in a separate step
-- we can have the queue logic built onto the obhect itself but we choose to keep it separate even though its a separate write
-- this seems better for sep of concerns but also the q-state can be updated in many places so best to decuple
-- for bodies and styles at some point (any point) after we update we can check the queue status e.g. in reducers
-- when we write the response to the flow api, the response can join asset attributes. we write responses to airtable and kafka
-- it is fine to use a single table as both the object and the queue but we still decuple the mutations
--NOTE the flow api only stores the queue mutation types under its upserts by default for queue types - we could treat entity mutations too
-- a flow id would be usel. This would be an instance of a flow configured somewhere in a flow types database
--  for example, a request on a flow chains together a number of processes and we usuall struggle to trace it e.g. bertha unpacks, body bundle unpacks etc.
--  how to we trace the lineage of a specific request that may have failed somewhere along the way; how do we return to a state
-- a response objective can have metadata in the kafka message and maybe in the database which contains logs for the last event in the queue
------
CREATE OR REPLACE FUNCTION track_queue_state () RETURNS trigger AS $$
BEGIN
     --two tests, one for a change and one for idempotence, the latter is sufficient
    IF  NEW.last_node_id <>  OLD.node_id  AND NEW.node_id <>  OLD.node_id  THEN
        NEW.node_id_last_updated_at := OLD.updated_at;
        NEW.last_node_id :=  OLD.node_id;
        NEW.last_updated_at := OLD.updated_at;
    END IF;
    --two tests, one for a change and one for idempotence, the latter is sufficient
    IF  NEW.last_status <>  OLD.status AND NEW.status <>  OLD.status THEN
        NEW.status_last_updated_at := OLD.updated_at;
        NEW.last_status :=  OLD.status;
        NEW.last_updated_at := OLD.updated_at;
    END IF;
     
  RETURN NEW;
END; $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS on_queue_bodies_track_queue_state on flow.queue_bodies;
CREATE TRIGGER on_queue_bodies_track_queue_state
  BEFORE INSERT OR UPDATE ON flow.queue_bodies
  FOR EACH ROW
  EXECUTE PROCEDURE track_queue_state();