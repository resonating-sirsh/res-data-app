
alter table "make"."roll_inspection_progress" alter column "synced_ts" set not null;

alter table "make"."roll_inspection_progress" alter column "finished_ts" set not null;

alter table "make"."roll_inspection_progress" drop constraint "roll_inspection_progress_pkey";
alter table "make"."roll_inspection_progress"
    add constraint "roll_inspection_progress_pkey"
    primary key ("id");

DROP TABLE "make"."roll_inspection_progress";
