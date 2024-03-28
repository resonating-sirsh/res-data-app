alter table "meta"."colors" add column "code" text
 not null default '______';

alter table "meta"."colors" add column "description" text
 null;

 alter table "meta"."colors" add column "metadata" jsonb
 not null default jsonb_build_object();

alter table "meta"."colors" add constraint "colors_code_key" unique ("code");