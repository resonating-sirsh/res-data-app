alter table "platform"."job_preset"
  add constraint "job_preset_job_type_fkey"
  foreign key ("job_type")
  references "platform"."job_type"
  ("type") on update restrict on delete restrict;
