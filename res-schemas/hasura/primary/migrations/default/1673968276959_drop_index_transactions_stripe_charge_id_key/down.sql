CREATE  INDEX "transactions_stripe_charge_id_key" on
  "sell"."transactions" using btree ("stripe_charge_id");
