ALTER TABLE "meta"."brand_body_requests" DROP COLUMN IF EXISTS material_onboarding_ids;
ALTER TABLE "meta"."brand_body_requests" DROP COLUMN IF EXISTS cover_image_ids;

ALTER TABLE "meta"."brand_body_requests" ADD COLUMN material_onboarding_ids text;
ALTER TABLE "meta"."brand_body_requests" ADD COLUMN cover_image_ids text;
