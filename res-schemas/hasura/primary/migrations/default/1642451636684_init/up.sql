SET check_function_bodies = false;
CREATE SCHEMA "create";
CREATE SCHEMA platform;
CREATE SCHEMA res_create;
CREATE FUNCTION "create".set_current_timestamp_updated_at() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$;
CREATE FUNCTION platform.set_current_timestamp_updated_at() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$;
CREATE FUNCTION res_create.set_current_timestamp_updated_at() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  _new record;
BEGIN
  _new := NEW;
  _new."updated_at" = NOW();
  RETURN _new;
END;
$$;
CREATE TABLE "create".asset (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    type text NOT NULL,
    status text NOT NULL,
    style_id text NOT NULL,
    job_id uuid NOT NULL,
    details text,
    path text,
    is_public_facing boolean,
    name text NOT NULL
);
CREATE TABLE "create".asset_status (
    name text NOT NULL,
    description text NOT NULL
);
CREATE TABLE "create".asset_type (
    name text NOT NULL,
    description text
);
CREATE TABLE platform.job (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    status text DEFAULT 'NEW'::text NOT NULL,
    status_details text,
    type text NOT NULL,
    preset_key text NOT NULL,
    requestor_id text,
    details jsonb
);
CREATE TABLE platform.job_preset (
    key text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    name text NOT NULL,
    version integer NOT NULL,
    details jsonb NOT NULL,
    description text
);
CREATE TABLE platform.job_status (
    status text NOT NULL,
    description text
);
CREATE TABLE platform.job_type (
    type text NOT NULL,
    description text
);
CREATE TABLE res_create.asset (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    asset_request_id uuid NOT NULL,
    details jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    path text,
    type text NOT NULL,
    sub_type text,
    status text,
    style_id text
);
CREATE TABLE res_create.asset_request (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    job_details jsonb NOT NULL,
    status text,
    manifest jsonb,
    status_details text,
    type text,
    preset_key text NOT NULL
);
CREATE TABLE res_create.asset_request_preset (
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    key text NOT NULL,
    name text NOT NULL,
    version integer NOT NULL,
    details jsonb NOT NULL,
    description text NOT NULL
);
COMMENT ON TABLE res_create.asset_request_preset IS 'Configurations for asset requests';
CREATE TABLE res_create.asset_request_status (
    status text DEFAULT 'NEW'::text NOT NULL,
    description text
);
CREATE SEQUENCE res_create.config_version_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE res_create.config_version_seq OWNED BY res_create.asset_request_preset.version;
ALTER TABLE ONLY "create".asset
    ADD CONSTRAINT asset_pkey PRIMARY KEY (id);
ALTER TABLE ONLY "create".asset_status
    ADD CONSTRAINT asset_status_pkey PRIMARY KEY (name);
ALTER TABLE ONLY "create".asset_type
    ADD CONSTRAINT asset_type_pkey PRIMARY KEY (name);
ALTER TABLE ONLY platform.job
    ADD CONSTRAINT job_pkey PRIMARY KEY (id);
ALTER TABLE ONLY platform.job_preset
    ADD CONSTRAINT job_preset_pkey PRIMARY KEY (key);
ALTER TABLE ONLY platform.job_status
    ADD CONSTRAINT job_status_pkey PRIMARY KEY (status);
ALTER TABLE ONLY platform.job_type
    ADD CONSTRAINT job_type_pkey PRIMARY KEY (type);
ALTER TABLE ONLY res_create.asset_request
    ADD CONSTRAINT asset_creation_job_pkey PRIMARY KEY (id);
ALTER TABLE ONLY res_create.asset
    ADD CONSTRAINT asset_request_artifact_pkey PRIMARY KEY (id);
ALTER TABLE ONLY res_create.asset_request_status
    ADD CONSTRAINT asset_request_status_pkey PRIMARY KEY (status);
ALTER TABLE ONLY res_create.asset_request_preset
    ADD CONSTRAINT config_key_key UNIQUE (key);
ALTER TABLE ONLY res_create.asset_request_preset
    ADD CONSTRAINT config_pkey PRIMARY KEY (key);
CREATE TRIGGER set_create_asset_updated_at BEFORE UPDATE ON "create".asset FOR EACH ROW EXECUTE FUNCTION "create".set_current_timestamp_updated_at();
COMMENT ON TRIGGER set_create_asset_updated_at ON "create".asset IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE TRIGGER set_platform_job_preset_updated_at BEFORE UPDATE ON platform.job_preset FOR EACH ROW EXECUTE FUNCTION platform.set_current_timestamp_updated_at();
COMMENT ON TRIGGER set_platform_job_preset_updated_at ON platform.job_preset IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE TRIGGER set_platform_job_updated_at BEFORE UPDATE ON platform.job FOR EACH ROW EXECUTE FUNCTION platform.set_current_timestamp_updated_at();
COMMENT ON TRIGGER set_platform_job_updated_at ON platform.job IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE TRIGGER set_public_asset_creation_job_updated_at BEFORE UPDATE ON res_create.asset_request FOR EACH ROW EXECUTE FUNCTION public.set_current_timestamp_updated_at();
COMMENT ON TRIGGER set_public_asset_creation_job_updated_at ON res_create.asset_request IS 'trigger to set value of column "updated_at" to current timestamp on row update';
CREATE TRIGGER set_res_create_config_updated_at BEFORE UPDATE ON res_create.asset_request_preset FOR EACH ROW EXECUTE FUNCTION res_create.set_current_timestamp_updated_at();
COMMENT ON TRIGGER set_res_create_config_updated_at ON res_create.asset_request_preset IS 'trigger to set value of column "updated_at" to current timestamp on row update';
ALTER TABLE ONLY "create".asset
    ADD CONSTRAINT asset_job_id_fkey FOREIGN KEY (job_id) REFERENCES platform.job(id) ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE ONLY "create".asset
    ADD CONSTRAINT asset_type_fkey FOREIGN KEY (type) REFERENCES "create".asset_type(name) ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE ONLY platform.job
    ADD CONSTRAINT job_preset_key_fkey FOREIGN KEY (preset_key) REFERENCES platform.job_preset(key) ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE ONLY platform.job
    ADD CONSTRAINT job_status_fkey FOREIGN KEY (status) REFERENCES platform.job_status(status) ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE ONLY platform.job
    ADD CONSTRAINT job_type_fkey FOREIGN KEY (type) REFERENCES platform.job_type(type) ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE ONLY res_create.asset
    ADD CONSTRAINT asset_asset_request_id_fkey FOREIGN KEY (asset_request_id) REFERENCES res_create.asset_request(id) ON UPDATE RESTRICT ON DELETE CASCADE;
ALTER TABLE ONLY res_create.asset_request
    ADD CONSTRAINT asset_request_config_key_fkey FOREIGN KEY (preset_key) REFERENCES res_create.asset_request_preset(key) ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE ONLY res_create.asset_request
    ADD CONSTRAINT asset_request_status_fkey FOREIGN KEY (status) REFERENCES res_create.asset_request_status(status) ON UPDATE RESTRICT ON DELETE RESTRICT;
