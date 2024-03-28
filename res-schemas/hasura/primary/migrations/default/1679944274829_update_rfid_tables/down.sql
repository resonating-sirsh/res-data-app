
alter table "premises"."rfid_channels"
  add constraint "rfid_channels_id_fkey"
  foreign key ("id")
  references "premises"."rfid_signals"
  ("id") on update restrict on delete restrict;

alter table "premises"."rfid_readers"
  add constraint "rfid_readers_id_fkey"
  foreign key ("id")
  references "premises"."rfid_channels"
  ("id") on update restrict on delete restrict;
