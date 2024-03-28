-- Only create the one_pieces_history table if it doesn't already exist
CREATE TABLE IF NOT EXISTS make.one_pieces_history (
    id uuid  NOT NULL,
    piece_id uuid NULL,
    piece_oid uuid NULL,
    observed_at timestamp NOT NULL,
    node_id uuid,
    node_status text,
    set_key text,
    PRIMARY KEY (id)
);

-- Create a unique index on the piece_id column if it doesn't already exist
CREATE UNIQUE INDEX IF NOT EXISTS idx_one_pieces_history_id ON make.one_pieces_history (id);


COMMENT ON TABLE "make"."one_pieces_history" IS E'shows history for a given piece eg which node, which statsus, when';

alter table "make"."one_pieces_history"
  add constraint "one_pieces_history_node_id_fkey"
  foreign key ("node_id")
  references "flow"."nodes"
  ("id");

alter table "make"."one_pieces_history"
  add constraint "one_pieces_history_piece_oid_fkey"
  foreign key ("piece_oid")
  references "make"."one_pieces"
  ("oid") ;

 








