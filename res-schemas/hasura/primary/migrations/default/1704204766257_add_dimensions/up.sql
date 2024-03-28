alter table meta.label_styles add column "dimensions" text;

UPDATE "meta"."label_styles" SET dimensions = '1" x 1.5"' WHERE name = 'group';
UPDATE "meta"."label_styles" SET dimensions = '2" x 2"' WHERE name = 'rectangle';
UPDATE "meta"."label_styles" SET dimensions = '1"x1"' WHERE name = 'rectangleSideways';
UPDATE "meta"."label_styles" SET dimensions = '1.5"x 1"' WHERE name = 'square';
UPDATE "meta"."label_styles" SET dimensions = '1" x 1"' WHERE name = 'substract';
