
alter table "flow"."nodes" add column "type" text
 null;

alter table "flow"."users" alter column "node_id" drop not null;

alter table "flow"."contracts" add column "status" text
 not null;

alter table "flow"."contracts" rename column "owner_user_id" to "owner";

alter table "flow"."contracts" rename column "code" to "key";

alter table "flow"."contracts" rename column "owner" to "owner_id";

alter table "flow"."contracts" alter column "uri" drop not null;

alter table "flow"."contracts" add column "metadata" jsonb
 null default jsonb_build_object();

alter table "flow"."contracts"
  add constraint "contracts_owner_id_fkey"
  foreign key ("owner_id")
  references "flow"."users"
  ("id") on update restrict on delete restrict;

alter table "flow"."contracts"
  add constraint "contracts_node_id_fkey"
  foreign key ("node_id")
  references "flow"."nodes"
  ("id") on update restrict on delete restrict;
