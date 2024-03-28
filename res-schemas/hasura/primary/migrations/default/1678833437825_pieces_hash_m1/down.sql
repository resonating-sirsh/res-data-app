
alter table "meta"."styles" drop constraint "styles_pieces_hash_id_fkey";

alter table "meta"."styles" rename column "pieces_hash_id" to "pieces_hash_ref";

DROP TABLE "meta"."pieces_hash_registry";
