alter table "sell"."faceted_search_filters_collection" add column "created_at" timestamptz
 null default now();
