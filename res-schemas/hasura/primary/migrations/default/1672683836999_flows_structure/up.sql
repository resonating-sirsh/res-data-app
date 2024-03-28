
CREATE TABLE "flow"."users" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "updated_at" timestamptz NOT NULL DEFAULT now(), "created_at" timestamptz NOT NULL DEFAULT now(), "email" text NOT NULL, "alias" text NOT NULL, "slack_uid" text NOT NULL, "node_id" uuid NOT NULL, PRIMARY KEY ("id") , FOREIGN KEY ("node_id") REFERENCES "flow"."nodes"("id") ON UPDATE restrict ON DELETE restrict);COMMENT ON TABLE "flow"."users" IS E'a table of users in the flow';
CREATE OR REPLACE FUNCTION "flow"."set_current_timestamp_updated_at"()
RETURNS TRIGGER AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER "set_flow_users_updated_at"
BEFORE UPDATE ON "flow"."users"
FOR EACH ROW
EXECUTE PROCEDURE "flow"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_flow_users_updated_at" ON "flow"."users" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE "flow"."dags" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "name" text NOT NULL, "metadata" jsonb NOT NULL DEFAULT jsonb_build_object(), "graph" jsonb NOT NULL DEFAULT jsonb_build_object(), "parent_node_id" uuid NOT NULL, PRIMARY KEY ("id") , FOREIGN KEY ("parent_node_id") REFERENCES "flow"."nodes"("id") ON UPDATE restrict ON DELETE restrict);COMMENT ON TABLE "flow"."dags" IS E'describe flows graphically';
CREATE OR REPLACE FUNCTION "flow"."set_current_timestamp_updated_at"()
RETURNS TRIGGER AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER "set_flow_dags_updated_at"
BEFORE UPDATE ON "flow"."dags"
FOR EACH ROW
EXECUTE PROCEDURE "flow"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_flow_dags_updated_at" ON "flow"."dags" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

alter table "flow"."users" add column "metadata" jsonb
 null default jsonb_build_object();

CREATE TABLE "flow"."contracts" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "name" text NOT NULL, "owner" uuid NOT NULL, "description" text NOT NULL, "node_id" uuid NOT NULL, PRIMARY KEY ("id") );
CREATE OR REPLACE FUNCTION "flow"."set_current_timestamp_updated_at"()
RETURNS TRIGGER AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER "set_flow_contracts_updated_at"
BEFORE UPDATE ON "flow"."contracts"
FOR EACH ROW
EXECUTE PROCEDURE "flow"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_flow_contracts_updated_at" ON "flow"."contracts" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

alter table "flow"."contracts" add column "code" text
 not null;

alter table "flow"."nodes" add column "metadata" jsonb
 not null default jsonb_build_object();

DROP table "flow"."dags";

CREATE TABLE "flow"."edges" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "source_node_id" uuid NOT NULL, "target_node_id" uuid NOT NULL, "edge_type" text NOT NULL, "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), "metadata" jsonb NOT NULL DEFAULT jsonb_build_object(), PRIMARY KEY ("id") );COMMENT ON TABLE "flow"."edges" IS E'flow topology';
CREATE OR REPLACE FUNCTION "flow"."set_current_timestamp_updated_at"()
RETURNS TRIGGER AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER "set_flow_edges_updated_at"
BEFORE UPDATE ON "flow"."edges"
FOR EACH ROW
EXECUTE PROCEDURE "flow"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_flow_edges_updated_at" ON "flow"."edges" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE "flow"."resources" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "name" text NOT NULL, "metadata" jsonb NOT NULL DEFAULT jsonb_build_object(), "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), PRIMARY KEY ("id") );
CREATE OR REPLACE FUNCTION "flow"."set_current_timestamp_updated_at"()
RETURNS TRIGGER AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER "set_flow_resources_updated_at"
BEFORE UPDATE ON "flow"."resources"
FOR EACH ROW
EXECUTE PROCEDURE "flow"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_flow_resources_updated_at" ON "flow"."resources" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

alter table "flow"."resources" add column "node_id" uuid
 not null;

alter table "flow"."resources" add column "type" text
 not null;

CREATE TABLE "flow"."actions" ("id" uuid NOT NULL DEFAULT gen_random_uuid(), "name" text NOT NULL, "edge_id" uuid NOT NULL, "metadata" jsonb NOT NULL, "filter" jsonb NOT NULL, "created_at" timestamptz NOT NULL DEFAULT now(), "updated_at" timestamptz NOT NULL DEFAULT now(), PRIMARY KEY ("id") );COMMENT ON TABLE "flow"."actions" IS E'decisions lead to actions on edges';
CREATE OR REPLACE FUNCTION "flow"."set_current_timestamp_updated_at"()
RETURNS TRIGGER AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER "set_flow_actions_updated_at"
BEFORE UPDATE ON "flow"."actions"
FOR EACH ROW
EXECUTE PROCEDURE "flow"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_flow_actions_updated_at" ON "flow"."actions" 
IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

DROP table "flow"."actions";

alter table "flow"."contracts" rename column "owner" to "owner_user_id";
