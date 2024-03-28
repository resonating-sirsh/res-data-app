DELETE FROM "sell"."ecommerce_collection_status"
	WHERE "value" = 'exported';

UPDATE "sell"."ecommerce_collection_status"
    SET
        value='created',
        comment='The collection is exported in Ecommerce but is not published in any Sale Channel'
    WHERE
        value='created';
