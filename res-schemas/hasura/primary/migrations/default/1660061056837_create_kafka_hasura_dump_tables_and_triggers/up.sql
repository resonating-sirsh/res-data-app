CREATE TABLE IF NOT EXISTS "infraestructure"."kafka_message_processing_states" (
  "state_name" text,
  "state_description" text,
  PRIMARY KEY ("state_name")
);
COMMENT ON TABLE "infraestructure"."kafka_message_processing_states" IS E'Enum table for storing processing states';

INSERT INTO "infraestructure"."kafka_message_processing_states"
  ("state_name", "state_description") VALUES
    (
      E'RECEIVED',
      E'Message was received by the dump table and is awaiting processing'
    ),
    (
      E'FAILED',
      E'The message was not able to be successfully passed to a processor.'
    ),
    (
      E'TRIGGERED',
      E'The message was passed to the processor.'
    ),
    (
      E'TRIGGERED_ACCEPTED',
      E'The message was accepted by the processor and is being processed.'
    ),
    (
      E'TRIGGERED_REJECTED',
      E'The message was rejected by the processor, but is available to be passed to other processors.'
    ),
    (
      E'PROCESSED_SUCCEEDED',
      E'The message was processed by the processor and successfully ingested. It may be cleared.'
    ),
    (
      E'PROCESSED_FAILED_FINAL',
      E'The message was processed by the processor, but failed for some reason. See processing_message for error details. This message may be cleared. NB: Processors should use this state only for messages that cannot be retried.'
    )
    ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS "infraestructure"."make_kafka_message_dump_1" 
(
  -- message columns
  "value" jsonb,
  "key" jsonb,
  "topic" text NOT NULL,
  "partition" integer NOT NULL DEFAULT 0,
  "offset" integer NOT NULL DEFAULT 0,
  "timestamp" timestamp,
  "timestamp_type" text NOT NULL,
  "timestamp_code" integer NOT NULL DEFAULT 0,
  "length" integer NOT NULL,
  "headers" jsonb NOT NULL DEFAULT jsonb_build_array(),
  "metadata" jsonb NOT NULL DEFAULT jsonb_build_array(),

  -- processing columns
  "processing_state" text NOT NULL DEFAULT 'RECEIVED',
  "processor" jsonb,
  "processing_message" text,
  "target_processor" jsonb,
  "processor_lock" uuid null unique,

  -- triggered columns - messages
  "num_receipts" integer NOT NULL DEFAULT 0,
  "value_changed" boolean NOT NULL DEFAULT TRUE,
  "version_history" jsonb NOT NULL DEFAULT jsonb_build_array(),
  -- triggered columns - processing
  "processing_history" jsonb NOT NULL DEFAULT jsonb_build_array(),
  "processing_state_last_transitioned_at" timestamptz NOT NULL DEFAULT now(),
  -- triggered columns - timestamps
  "created_at" timestamptz NOT NULL DEFAULT now(),
  "updated_at" timestamptz NOT NULL DEFAULT now(),

  -- constraints
  PRIMARY KEY ("topic", "partition", "offset"), 
  FOREIGN KEY ("processing_state") REFERENCES "infraestructure"."kafka_message_processing_states"("state_name") ON UPDATE cascade ON DELETE restrict
);
COMMENT ON TABLE "infraestructure"."make_kafka_message_dump_1" IS
E'Table that will receive kafka messages dumps in JSONB. Records follow an FSM for processing by event triggers.';

CREATE OR REPLACE FUNCTION "infraestructure"."set_current_timestamp_updated_at"()
RETURNS TRIGGER AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER "set_infraestructure_make_kafka_message_dump_1_updated_at"
BEFORE UPDATE ON "infraestructure"."make_kafka_message_dump_1"
FOR EACH ROW
EXECUTE PROCEDURE "infraestructure"."set_current_timestamp_updated_at"();
COMMENT ON TRIGGER "set_infraestructure_make_kafka_message_dump_1_updated_at" ON "infraestructure"."make_kafka_message_dump_1" 
IS E'trigger to set value of column "updated_at" to current timestamp on row update';

CREATE OR REPLACE FUNCTION "infraestructure"."before_kafka_message_processing_state_change"()
  RETURNS trigger 
  LANGUAGE plpgsql
  VOLATILE NOT LEAKPROOF
AS $$
DECLARE
  _new record;
  processing_update JSONB;
BEGIN
  _new := NEW;

  if _new."target_processor"::text = '""' then
    _new."target_processor" = null;
  end if;

  _new."processing_state_last_transitioned_at" = NOW();
  processing_update = jsonb_build_object(
    'state', _new."processing_state",
    'processor', _new."processor",
    'target_processor', _new."target_processor",
    'processor_lock', _new."processor_lock",
    'message', _new."processing_message",
    'transitioned_at', _new."processing_state_last_transitioned_at"
  );

  if OLD."processor_lock" is null then
    if _new."processing_state" = ANY('{"TRIGGERED","TRIGGERED_ACCEPTED"}') then
      -- Ensure processor_lock is set if transitioning to a state that should be locked
      if _new."processor_lock" is null then
        _new."processor_lock" = gen_random_uuid();
      end if;
      processing_update = jsonb_set(
        processing_update, '{processor_lock}', to_jsonb(_new."processor_lock"), true
      );
      processing_update = jsonb_set(
        processing_update, '{notes}', '["Lock set"]'::jsonb, true
      );
    end if;
  else
    if _new."processor_lock" != OLD."processor_lock" then
      RAISE EXCEPTION 
        'Received processing update with lock `%`, but row locked with `%` (%)!',
        _new."processor_lock", OLD."processor_lock", jsonb_pretty(processing_update);
    elsif _new."processing_state" = ANY('{"TRIGGERED_REJECTED","PROCESSED_SUCCEEDED","PROCESSED_FAILED_FINAL"}') then
      -- Unset processor_lock if it is correct and we hit an unlocked state
      _new."processor_lock" = null;
      processing_update = jsonb_set(
        processing_update, '{processor_lock}', to_jsonb(_new."processor_lock"), true
      );
      processing_update = jsonb_set(
        processing_update, '{notes}', '["Lock removed"]'::jsonb, true
      );
    end if;
  end if;

  if OLD."target_processor" is not null and OLD."target_processor"::text != '""' AND _new."processor" @> OLD."target_processor" AND _new."processor" <@ OLD."target_processor" then
    RAISE EXCEPTION
      'Received processing update from %, but `target_processor` is % (%)',
      _new."processor", OLD."target_processor", jsonb_pretty(processing_update);
  end if;

  if processing_update is not null then
    _new."processing_history" =
      COALESCE(_new."processing_history", '[]'::jsonb) || jsonb_build_array(processing_update);
  end if;

  RETURN _new;
END
$$;

CREATE TRIGGER "before_kafka_message_processing_state_change"
  BEFORE INSERT OR UPDATE OF
    "processing_state", "processor", "processing_message", "target_processor"
  ON "infraestructure"."make_kafka_message_dump_1"
  FOR EACH ROW
  EXECUTE FUNCTION "infraestructure"."before_kafka_message_processing_state_change"();

COMMENT ON TRIGGER "before_kafka_message_processing_state_change" ON "infraestructure"."make_kafka_message_dump_1" 
IS E'trigger to handle history and validation of processing updates';

CREATE OR REPLACE FUNCTION "infraestructure"."before_kafka_message_contents_write"()
  RETURNS trigger
  LANGUAGE plpgsql
  VOLATILE NOT LEAKPROOF
AS $$
DECLARE
  _new record;
  previous_version JSONB;
BEGIN
  _new := NEW;

  if _new."num_receipts" > 0 then
    previous_version = to_jsonb(OLD);
    previous_version = previous_version - 'num_receipts';
    previous_version = previous_version - 'version_history';
    previous_version = previous_version - 'processing_state';
    previous_version = previous_version - 'processor';
    previous_version = previous_version - 'processing_message';
    previous_version = previous_version - 'processing_state_last_transitioned_at';
    previous_version = previous_version - 'processing_history';
    previous_version = previous_version - 'created_at';
    previous_version = jsonb_strip_nulls(previous_version);
    previous_version = jsonb_set(
      previous_version, '{receipt_no}'::text[], to_jsonb(OLD."num_receipts"), TRUE
    );
    if NOT OLD."value_changed" then
      previous_version = previous_version - 'value';
      previous_version = previous_version #- '{metadata,value}';
      previous_version = previous_version #- '{metadata,raw_value}';
      previous_version = jsonb_set(
        previous_version,
        '{see_receipt_for_value}'::text[],
        to_jsonb(
          COALESCE(
            OLD."version_history"#>>'{-1,see_receipt_for_value}', -- present when value not changed
            OLD."version_history"#>>'{-1,receipt_no}',
            '-1'
          ) :: integer
        ),
        TRUE
      );
    end if;

    if _new."value" @> OLD."value" AND _new."value" <@ OLD."value" then
      _new."value_changed" = FALSE;
    else
      _new."value_changed" = TRUE;
        _new."version_history" =
        COALESCE(_new."version_history", '[]'::jsonb) || jsonb_build_array(previous_version);
    end if;


  end if;
  _new."num_receipts" = _new."num_receipts" + 1;


  RETURN _new;
END
$$;

CREATE TRIGGER "before_kafka_message_contents_write"
  BEFORE INSERT OR UPDATE OF 
    "value",
    "key",
    "topic",
    "partition",
    "offset",
    "timestamp",
    "timestamp_type",
    "length",
    "headers",
    "metadata"
  ON "infraestructure"."make_kafka_message_dump_1"
  FOR EACH ROW
  EXECUTE FUNCTION "infraestructure"."before_kafka_message_contents_write"();

COMMENT ON TRIGGER "before_kafka_message_contents_write" ON "infraestructure"."make_kafka_message_dump_1" 
IS E'trigger to handle history and automatic fields of message content updates';