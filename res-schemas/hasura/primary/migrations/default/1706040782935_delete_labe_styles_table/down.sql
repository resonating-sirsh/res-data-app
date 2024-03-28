CREATE TABLE IF NOT EXISTS "meta"."label_styles" ("id" uuid NOT NULL, "name" text NOT NULL, "thumbnail_uri" text NOT NULL, PRIMARY KEY ("id") , UNIQUE ("id"));

INSERT INTO "meta"."label_styles" (name, thumbnail_uri) 
    VALUES ('group', 's3://meta-one-assets-prod/label_styles/group.svg');

INSERT INTO "meta"."label_styles" (name, thumbnail_uri) 
    VALUES ('rectangle', 's3://meta-one-assets-prod/label_styles/rectangle.svg');

INSERT INTO "meta"."label_styles" (name, thumbnail_uri) 
    VALUES ('rectangleSideways', 's3://meta-one-assets-prod/label_styles/rectangleSideways.svg');

INSERT INTO "meta"."label_styles" (name, thumbnail_uri) 
    VALUES ('square', 's3://meta-one-assets-prod/label_styles/square.svg');

INSERT INTO "meta"."label_styles" (name, thumbnail_uri) 
    VALUES ('substract', 's3://meta-one-assets-prod/label_styles/substract.svg');
