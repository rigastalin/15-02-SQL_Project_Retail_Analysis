DROP FUNCTION IF EXISTS func_personal_cross_shellings_offers (
    groups_count INT,
    max_churn_rate NUMERIC,
    max_stability_index NUMERIC,
    max_index_sku NUMERIC,
    margin_share NUMERIC
) CASCADE;

CREATE OR REPLACE FUNCTION func_personal_cross_shellings_offers(
    groups_count INT,
    max_churn_rate NUMERIC,
    max_stability_index NUMERIC,
    max_index_sku NUMERIC,
    margin_share NUMERIC
)
RETURNS TABLE (
    customer_id INT,
    sku_name VARCHAR,
    offer_discount_depth NUMERIC
)
AS $$
BEGIN
    RETURN QUERY
    WITH cte AS (
        SELECT DISTINCT
            mv_group.customer_id::int,
            product_grid.sku_name,
            mv_group.group_churn_rate,
            mv_group.group_stability_index,
            MAX(stores.sku_retail_price - stores.sku_purchase_price) OVER (
                PARTITION BY mv_group.customer_id, mv_group.group_id, product_grid.sku_id
            ),
            (COUNT(stores.transaction_store_id) OVER (
                PARTITION BY product_grid.sku_id
            ))::FLOAT / (COUNT(stores.transaction_store_id) OVER (
                PARTITION BY mv_group.group_id
            ))::FLOAT AS share_sku_group,
            CEILING((sku_retail_price - stores.sku_purchase_price) * (margin_share / 100) / stores.sku_retail_price * 20) / 20 * 100
                AS offer_discount_depth,
            (DENSE_RANK() OVER (PARTITION BY mv_group.customer_id ORDER BY mv_group.group_id)) AS ranks
        FROM mv_group
        JOIN mv_customers ON mv_group.customer_id = mv_customers.customer_id
        JOIN cards ON mv_customers.customer_id = cards.customer_id
        JOIN product_grid ON mv_group.group_id = product_grid.group_id
        JOIN stores ON product_grid.sku_id = stores.sku_id
        JOIN transactions ON cards.customer_card_id = transactions.customer_card_id
        JOIN scu_group ON product_grid.group_id = scu_group.group_id
    )
    SELECT DISTINCT
        cte.customer_id,
        cte.sku_name,
        cte.offer_discount_depth
    FROM cte
    WHERE cte.group_churn_rate <= max_churn_rate
    AND cte.group_stability_index <= max_stability_index
    AND cte.ranks <= groups_count
    AND cte.share_sku_group * 100  >= max_index_sku;
END;
$$ LANGUAGE plpgsql;

SELECT *
FROM func_personal_cross_shellings_offers(
    5,
    3,
    0.5,
    100,
    30
);