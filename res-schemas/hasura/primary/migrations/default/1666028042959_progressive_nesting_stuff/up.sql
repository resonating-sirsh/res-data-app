
CREATE TABLE "make"."material_solutions" (
  "id" UUID NOT NULL DEFAULT gen_random_uuid(),
  "solution_key" text NOT NULL,
  "solution_status" text NOT NULL,
  "material_code" text NOT NULL,
  "pending_asset_count" integer NOT NULL,
  "total_roll_length_px" integer NOT NULL,
  "total_roll_length_yd" float4 NOT NULL,
  "total_reserved_length_px" integer NOT NULL,
  "total_reserved_length_yd" float4 NOT NULL,
  "total_asset_count" integer NOT NULL,
  "total_utilization" float4 NOT NULL,
  "total_length_utilization" float4 NOT NULL,
  "metadata" jsonb NOT NULL,
  PRIMARY KEY ("id")
);

CREATE TABLE "make"."nests" (
  "id" UUID NOT NULL DEFAULT gen_random_uuid(),
  "nest_key" text NOT NULL,
  "nest_file_path" text NOT NULL,
  "nest_length_px" integer NOT NULL,
  "nest_length_yd" float4 NOT NULL,
  "asset_count" integer NOT NULL, 
  "utilization" float4 NOT NULL,
  "metadata" jsonb NOT NULL,
  PRIMARY KEY ("id"),
  UNIQUE ("id")
);

CREATE TABLE "make"."roll_solutions" (
  "id" UUID NOT NULL DEFAULT gen_random_uuid(),
  "roll_name" text NOT NULL,
  "roll_length_px" integer NOT NULL,
  "roll_length_yd" float4 NOT NULL,
  "reserved_length_px" integer NOT NULL,
  "reserved_length_yd" float4 NOT NULL,
  "asset_count" integer NOT NULL,
  "utilization" float4 NOT NULL,
  "length_utilization" float4 NOT NULL,
  "material_solution_id" UUID NOT NULL,
  "metadata" jsonb NOT NULL,
  PRIMARY KEY ("id"),
  FOREIGN KEY ("material_solution_id") REFERENCES "make"."material_solutions"("id") ON UPDATE restrict ON DELETE restrict
);

CREATE TABLE "make"."roll_solutions_nests" (
  "id" UUID NOT NULL DEFAULT gen_random_uuid(),
  "roll_solution_id" UUID NOT NULL,
  "nest_id" UUID NOT NULL,
  PRIMARY KEY ("id"),
  UNIQUE("roll_solution_id", "nest_id"),
  FOREIGN KEY ("roll_solution_id") REFERENCES "make"."roll_solutions"("id") ON UPDATE restrict ON DELETE restrict,
  FOREIGN KEY ("nest_id") REFERENCES "make"."nests"("id") ON UPDATE restrict ON DELETE restrict
);
