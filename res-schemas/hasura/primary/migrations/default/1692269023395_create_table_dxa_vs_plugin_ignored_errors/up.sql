CREATE TABLE "dxa"."vs_plugin_ignored_errors" (
    "error_text" text NOT NULL, 
    "piece_name" text NOT NULL, 
    "username" text NOT NULL, 
    "active" boolean NOT NULL DEFAULT true, 
    "created_at" timestamptz NOT NULL default now(),
    "updated_at" timestamptz NOT NULL default now(),
    PRIMARY KEY ("error_text") , 
    UNIQUE ("error_text")
);
