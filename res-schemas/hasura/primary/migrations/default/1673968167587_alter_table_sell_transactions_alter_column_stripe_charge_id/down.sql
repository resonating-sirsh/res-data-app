alter table "sell"."transactions" add constraint "transactions_stripe_charge_id_key" unique ("stripe_charge_id");
