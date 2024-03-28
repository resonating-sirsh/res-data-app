
alter table "premises"."rfid_channels" drop constraint "rfid_channels_id_fkey";

alter table "premises"."rfid_readers" drop constraint "rfid_readers_id_fkey";

DROP TABLE "premises"."rfid_signals";

alter table "premises"."rfid_channels" rename to "channels";

DROP TABLE "premises"."channels";

DROP TABLE "premises"."rfid_readers";

drop schema "premises" cascade;
