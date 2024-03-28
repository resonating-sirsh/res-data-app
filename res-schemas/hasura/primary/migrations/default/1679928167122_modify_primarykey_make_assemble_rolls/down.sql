alter table "make"."assemble_rolls" drop constraint "assemble_rolls_pkey";
alter table "make"."assemble_rolls"
    add constraint "assemble_rolls_pkey"
    primary key ("roll_id");
