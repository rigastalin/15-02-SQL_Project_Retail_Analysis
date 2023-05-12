DROP FUNCTION IF EXISTS fnc_generate_personal_offers_for_frequency_growth(first_date TIMESTAMP,
                                                                          last_date TIMESTAMP,
                                                                          transactions_count INT,
                                                                          max_churn_rate NUMERIC,
                                                                          max_discount NUMERIC,
                                                                          MAX_MARGIN NUMERIC) CASCADE;

CREATE OR REPLACE FUNCTION fnc_generate_personal_offers_for_frequency_growth(
    first_date TIMESTAMP,
    last_date TIMESTAMP,
    transactions_count INT DEFAULT 100,
    max_churn_rate NUMERIC DEFAULT 0.9,
    max_discount NUMERIC DEFAULT 85,
    max_margin NUMERIC DEFAULT 10000
)
    RETURNS TABLE
            (
                customer_id                 BIGINT,
                start_date                  TIMESTAMP,
                end_date                    TIMESTAMP,
                required_transactions_count INT,
                group_name                  VARCHAR,
                offer_discount              NUMERIC
            )
AS
$$
    WITH group_data AS (
            SELECT distinct mv_group.group_id,
                   mv_group.group_affinity_index,
                   mv_group.group_minimum_discount,
                   AVG(mv_purchase_history.group_summ_paid - mv_purchase_history.group_cost)
                   OVER (PARTITION BY mv_purchase_history.customer_id, mv_purchase_history.group_id) / 100 * max_margin AS margin,
                   CASE
                       WHEN (mv_group.group_minimum_discount * 100 % 5) = 0 THEN mv_group.group_minimum_discount * 100
                       ELSE 5 - (mv_group.group_minimum_discount * 100 % 5) + (mv_group.group_minimum_discount * 100)
                   END AS offer_discount_depth
            FROM mv_group
            JOIN mv_purchase_history ON mv_group.group_id = mv_purchase_history.group_id
            WHERE mv_group.group_churn_rate <= max_churn_rate
                AND mv_group.group_discount_share * 100 < max_discount
                AND mv_group.group_minimum_discount > 0
        ),
        customer_data AS (
                SELECT DISTINCT mv_customers.customer_id,
                                mv_group.group_id,
                                group_data.group_affinity_index,
                                (EXTRACT(EPOCH FROM last_date - first_date)::float / 86400.0 / mv_customers.customer_frequency)::int + transactions_count AS required_transactions_count
                FROM mv_customers
                JOIN mv_group ON mv_customers.customer_id = mv_group.customer_id
                JOIN group_data ON mv_group.group_id = group_data.group_id
                WHERE mv_group.group_churn_rate <= max_churn_rate
                    AND mv_group.group_discount_share * 100 < max_discount
                    AND mv_group.group_minimum_discount > 0
            ),
        max_affinity AS (
                    SELECT DISTINCT customer_data.customer_id,
                                    MAX(customer_data.group_affinity_index) AS max_group_affinity_index
                    FROM customer_data
                    GROUP BY customer_data.customer_id
                )

        SELECT DISTINCT customer_data.customer_id,
               first_date,
               last_date,
               required_transactions_count,
               scu_group.group_name,
               group_data.offer_discount_depth
        FROM customer_data
        JOIN group_data ON customer_data.group_id = group_data.group_id
        JOIN max_affinity ON customer_data.customer_id = max_affinity.customer_id
        JOIN scu_group ON customer_data.group_id = scu_group.group_id
        WHERE group_data.offer_discount_depth < group_data.margin
            AND group_data.group_affinity_index = max_affinity.max_group_affinity_index
        ORDER BY 1;

$$ LANGUAGE sql;

SELECT *
FROM fnc_generate_personal_offers_for_frequency_growth('2018-03-18 10:07:00', '2022-06-28 12:12:12');

SELECT *
FROM fnc_generate_personal_offers_for_frequency_growth('2022-08-18 00:00:00', '2022-08-18 00:00:00',
                             1, 3, 70, 30);