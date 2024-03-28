
alter table "meta"."pieces" add column "rank" integer
 null;

alter table "meta"."pieces" rename column "rank" to "piece_index";

alter table "meta"."pieces" rename column "piece_index" to "piece_ordinal";

alter table "meta"."pieces" add column "normed_size" text
 null;

alter table "meta"."pieces" add column "piece_set_size" integer
 null;
