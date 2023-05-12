DROP FUNCTION IF EXISTS fnc_calculate_required_check_measure(
    calc_method INTEGER,
    first_period DATE,
    last_period DATE,
    number_transactions INTEGER,
    coeff_increase_ave_check NUMERIC,
    max_transactions_discount NUMERIC,
    max_churn_index NUMERIC,
    share_margin NUMERIC
) CASCADE;

CREATE OR REPLACE FUNCTION fnc_calculate_required_check_measure(
    calc_method INTEGER DEFAULT 1,
    first_period DATE DEFAULT '2018-01-20',
    last_period DATE DEFAULT '2022-08-20',
    number_transactions INTEGER DEFAULT 10,
    coeff_increase_ave_check NUMERIC DEFAULT 1.2,
    max_transactions_discount NUMERIC DEFAULT 50,
    max_churn_index NUMERIC DEFAULT 10,
    share_margin NUMERIC DEFAULT 15
)
RETURNS TABLE (
    customer_id BIGINT,
    required_check_measure NUMERIC,
    group_name VARCHAR,
    offer_discount_depth NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT ON (mv_group.customer_id)
        mv_group.customer_id,
        ROUND((
            CASE
                WHEN calc_method = 1 THEN period_method.required_check_measure
                WHEN calc_method = 2 THEN number_method.required_check_measure
            END
        )::numeric, 2) AS required_check_measure,
        scu_group.group_name,
        CEIL(mv_group.group_minimum_discount / 5) * 5 AS offer_discount_depth
    FROM mv_group
    JOIN scu_group ON mv_group.group_id = scu_group.group_id
    JOIN (
        SELECT
            cards.customer_id,
            AVG(t.transaction_summ) * coeff_increase_ave_check AS required_check_measure
        FROM cards
        JOIN transactions t ON cards.customer_card_id = t.customer_card_id
        WHERE t.transaction_datatime BETWEEN first_period AND last_period
        GROUP BY cards.customer_id
    ) AS period_method ON mv_group.customer_id = period_method.customer_id
    JOIN (
        SELECT
            tmp.customer_id,
            AVG(tmp.transaction_summ) * coeff_increase_ave_check AS required_check_measure
        FROM (
            SELECT
                cards.customer_id,
                t.transaction_summ,
                ROW_NUMBER() OVER (PARTITION BY cards.customer_id ORDER BY t.transaction_datatime DESC) AS count,
                t.transaction_datatime
            FROM cards
            JOIN transactions t ON cards.customer_card_id = t.customer_card_id
        ) AS tmp
        WHERE tmp.count <= number_transactions
        GROUP BY tmp.customer_id
    ) AS number_method ON mv_group.customer_id = number_method.customer_id
    WHERE mv_group.group_churn_rate < max_churn_index
        AND mv_group.group_discount_share > max_transactions_discount / 100
        AND mv_group.group_margin * share_margin > CEIL(mv_group.group_minimum_discount / 5) * 5
    ORDER BY mv_group.customer_id, mv_group.group_affinity_index DESC;
END;
$$ LANGUAGE plpgsql;

SELECT *
FROM fnc_calculate_required_check_measure();

SELECT *
FROM fnc_calculate_required_check_measure(2, '2022-01-18', '2022-08-18',
    100, 1.15, 70, 3, 30);