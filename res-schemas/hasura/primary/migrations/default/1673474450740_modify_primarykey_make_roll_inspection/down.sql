alter table "make"."roll_inspection" drop constraint "roll_inspection_pkey";
alter table "make"."roll_inspection"
    add constraint "roll_inspection_pkey"
    primary key ("piece_id", "nest_key", "inspector");
