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
