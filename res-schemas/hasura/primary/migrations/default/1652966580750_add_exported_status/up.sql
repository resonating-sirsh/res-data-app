INSERT INTO "sell"."ecommerce_collection_status"
    VALUES 
    ('exported', 'The collection is exported to Ecommerce')
ON CONFLICT DO NOTHING;

UPDATE "sell"."ecommerce_collection_status"
    SET
        value='created',
        comment='The collection is created in our DB but not in the ecommerce platform'
    WHERE
        value='created';
