
alter table "meta"."bodies" rename column "updated_at" to "modified_at";

DROP TABLE "meta"."body_pieces";

alter table "meta"."bodies" drop column "profile"
