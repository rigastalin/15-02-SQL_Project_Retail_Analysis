-- 2.1
DROP MATERIALIZED VIEW IF EXISTS mv_customers CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_customers AS
WITH avg_check AS (SELECT cards.customer_id,
                          ROUND(AVG(tr.transaction_summ), 2) AS customer_average_check,
                          ROUND((MAX(tr.transaction_datatime::date) - MIN(tr.transaction_datatime::date))::numeric /
                                COUNT(*), 2)                 AS customer_frequency,
                          ROUND(EXTRACT(EPOCH FROM ((SELECT analysis_formation FROM date_of_analysis) -
                                                    MAX(tr.transaction_datatime))) / 86400, 2)
                                                             AS customer_inactive_period
                   FROM personal_information
                            JOIN cards ON personal_information.customer_id = cards.customer_id
                            JOIN transactions tr ON cards.customer_card_id = tr.customer_card_id
                   GROUP BY cards.customer_id),

     rank AS (SELECT customer_id,
                     customer_average_check,
                     CUME_DIST() OVER (ORDER BY customer_average_check)      AS rank_check,
                     CUME_DIST() OVER (ORDER BY customer_frequency)          AS rank_freq,
                     ROUND(customer_inactive_period / customer_frequency, 2) AS customer_churn_rate,
                     customer_frequency,
                     customer_inactive_period
              FROM avg_check),


     segment_check AS (SELECT customer_id,
                              customer_average_check,
                              CASE
                                  WHEN rank_check <= 0.1 THEN 'High'
                                  WHEN rank_check <= 0.35 THEN 'Medium'
                                  ELSE 'Low' END    AS customer_average_check_segment,
                              customer_frequency,
                              CASE
                                  WHEN rank_freq <= 0.1 THEN 'Often'
                                  WHEN rank_freq <= 0.35 THEN 'Occasionally'
                                  ELSE 'Rarely' END AS customer_frequency_segment,
                              customer_inactive_period,
                              customer_churn_rate,
                              CASE
                                  WHEN customer_churn_rate < 2 THEN 'Low'
                                  WHEN customer_churn_rate < 5 THEN 'Medium'
                                  ELSE 'High' END   AS customer_churn_segment

                       FROM rank),


     customer_segm AS (SELECT customer_id,
                              customer_average_check,
                              customer_average_check_segment,
                              customer_frequency,
                              customer_frequency_segment,
                              customer_inactive_period,
                              customer_churn_rate,
                              customer_churn_segment,
                              CASE customer_average_check_segment
                                  WHEN 'Low' THEN 0
                                  WHEN 'Medium' THEN 9
                                  ELSE 18 END +
                              CASE customer_frequency_segment
                                  WHEN 'Rarely' THEN 0
                                  WHEN 'Occasionally' THEN 3
                                  ELSE 6 END +
                              CASE customer_churn_segment
                                  WHEN 'Low' THEN 1
                                  WHEN 'Medium' THEN 2
                                  ELSE 3 END AS customer_segment
                       FROM segment_check),

     customer_store AS (SELECT personal_information.customer_id,
                               t.transaction_store_id,
                               COUNT(*) OVER (PARTITION BY personal_information.customer_id ,t.transaction_store_id) /
                               COUNT(*) OVER (PARTITION BY personal_information.customer_id)::numeric AS share,
                               t.transaction_datatime
                        FROM personal_information
                                 INNER JOIN cards c ON c.customer_id = personal_information.customer_id
                                 INNER JOIN transactions t ON t.customer_card_id = c.customer_card_id
                        WHERE t.transaction_datatime <= (SELECT analysis_formation FROM date_of_analysis)
                        ORDER BY 1, 4 DESC)

SELECT *,
       CASE
           WHEN (SELECT COUNT(DISTINCT transaction_store_id) = 1
                 FROM customer_store
                 WHERE customer_id = customer_segm.customer_id
                 LIMIT 3) THEN (SELECT transaction_store_id
                                FROM customer_store
                                WHERE customer_id = customer_segm.customer_id
                                LIMIT 1)
           ELSE (SELECT transaction_store_id
                 FROM customer_store
                 WHERE customer_id = customer_segm.customer_id
                 ORDER BY share DESC, transaction_datatime DESC
                 LIMIT 1) END AS customer_primary_store
FROM customer_segm
ORDER BY 1;


SELECT *
FROM mv_customers;

SELECT *
FROM mv_customers
WHERE customer_average_check_segment = 'Low';

SELECT *
FROM mv_customers
WHERE customer_average_check_segment = 'Medium';

SELECT *
FROM mv_customers
WHERE customer_frequency_segment = 'Occasionally';


-- 2.2
DROP MATERIALIZED VIEW IF EXISTS mv_purchase_history CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_purchase_history AS
SELECT p.customer_id,
       t.transaction_id,
       t.transaction_datatime,
       group_id,
       ROUND(SUM(sku_purchase_price * ch.sku_amount), 2) AS group_cost,
       ROUND(SUM(ch.sku_summ), 2)                        AS group_summ,
       ROUND(SUM(ch.sku_summ_paid), 2)                   AS group_summ_paid
FROM personal_information p
         JOIN cards c ON p.customer_id = c.customer_id
         JOIN transactions t ON c.customer_card_id = t.customer_card_id
         JOIN checks ch ON t.transaction_id = ch.transaction_id
         JOIN product_grid pd ON ch.sku_id = pd.sku_id
         JOIN stores st ON pd.sku_id = st.sku_id AND t.transaction_store_id = st.transaction_store_id
GROUP BY p.customer_id, t.transaction_id, t.transaction_datatime, group_id;

SELECT *
FROM mv_purchase_history;

SELECT *
FROM mv_purchase_history
WHERE customer_id = 5;

SELECT *
FROM mv_purchase_history
WHERE transaction_datatime = '2021-11-01 11:19:00.000000';

-- 2.3
DROP MATERIALIZED VIEW IF EXISTS mv_periods CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_periods AS
SELECT ph.customer_id,
       ph.group_id,
       MIN(ph.transaction_datatime)                           AS first_group_purchase_date,
       MAX(ph.transaction_datatime)                           AS last_group_purchase_date,
       COUNT(DISTINCT ph.transaction_id)                      AS group_purchase,
       ROUND(((MAX(ph.transaction_datatime)::date - MIN(ph.transaction_datatime)::date + 1) /
              COUNT(DISTINCT ph.transaction_id)::numeric), 2) AS group_frequency,
       ROUND(COALESCE(MIN(CASE WHEN c.sku_discount > 0 THEN c.sku_discount / c.sku_summ::numeric ELSE NULL END), 0),
             2)                                               AS group_min_discount
FROM mv_purchase_history ph
         JOIN product_grid pg ON pg.group_id = ph.group_id
         LEFT JOIN checks c ON ph.transaction_id = c.transaction_id AND c.sku_discount > 0 AND c.sku_id = pg.sku_id
GROUP BY ph.customer_id, ph.group_id
ORDER BY ph.customer_id, ph.group_id;

SELECT *
FROM mv_periods;

SELECT *
FROM mv_periods
WHERE first_group_purchase_date = '2019-02-03 05:42:11.000000';

SELECT *
FROM mv_periods
WHERE customer_id > 4;

-- 2.4
DROP FUNCTION IF EXISTS calculate_customer_group_metrics(margin_method INTEGER, margin_amount INTEGER) CASCADE;

CREATE OR REPLACE FUNCTION calculate_customer_group_metrics(margin_method INTEGER, margin_amount INTEGER)
    RETURNS TABLE
            (
                customer_id BIGINT,
                group_id BIGINT,
                group_affinity_index NUMERIC(10, 4),
                group_churn_rate NUMERIC(10, 4),
                group_stability_index NUMERIC(10, 4),
                group_margin NUMERIC(10, 4),
                group_discount_share NUMERIC(10, 4),
                group_minimum_discount NUMERIC(10, 4),
                group_average_discount NUMERIC(10, 4)
            )
AS
$$
DECLARE
    data_of_analysis_date DATE := (SELECT date_of_analysis.analysis_formation FROM date_of_analysis);

BEGIN
    IF margin_method IN (1, 2) AND margin_amount > 0 THEN
        RETURN QUERY
            WITH transactions_at_discount AS (
                SELECT DISTINCT
                    mv_purchase_history.customer_id,
                    mv_purchase_history.group_id,
                    COUNT(DISTINCT mv_purchase_history.transaction_id) AS transactions_count
                FROM
                    public.mv_purchase_history
                    JOIN public.product_grid ON mv_purchase_history.group_id = product_grid.group_id
                    JOIN public.checks ON product_grid.sku_id = checks.sku_id
                WHERE
                    checks.transaction_id = mv_purchase_history.transaction_id
                    AND checks.sku_discount > 0
                GROUP BY
                    mv_purchase_history.customer_id,
                    mv_purchase_history.group_id
            ),

                affinity_index AS (
                    SELECT
                        mv_periods.customer_id,
                        mv_periods.group_id,
                        (mv_periods.group_purchase / COUNT(DISTINCT mv_purchase_history.transaction_id)::numeric) AS group_affinitty_index
                    FROM
                        public.mv_periods
                        JOIN public.mv_purchase_history ON mv_periods.customer_id = mv_purchase_history.customer_id AND mv_periods.group_id = mv_purchase_history.group_id
                    WHERE
                        mv_purchase_history.transaction_datatime BETWEEN mv_periods.first_group_purchase_date AND mv_periods.last_group_purchase_date
                    GROUP BY
                        mv_periods.customer_id,
                        mv_periods.group_id,
                        mv_periods.group_purchase
                ),

                 relative_deviation AS (
                        SELECT mv_periods.customer_id,
                               mv_periods.group_id,
                               mv_purchase_history.transaction_datatime,
                               mv_purchase_history.group_summ_paid,
                               group_cost,
                               ROW_NUMBER() OVER (
                                   PARTITION BY mv_purchase_history.customer_id, mv_purchase_history.group_id
                                   ORDER BY transaction_datatime DESC) AS row_count,
                               ROUND(((data_of_analysis_date - last_group_purchase_date::date) /
                                group_frequency::numeric), 2)                AS group_churn_rate,
                               ABS(transaction_datatime::date - LAG(transaction_datatime) OVER (
                                   PARTITION BY mv_purchase_history.customer_id, mv_purchase_history.group_id
                                   ORDER BY transaction_datatime
                                   )::date - mv_periods.group_frequency
                                   ) / mv_periods.group_frequency    AS deviation,
                               AVG(mv_purchase_history.group_summ_paid / mv_purchase_history.group_summ::numeric)
                               OVER (PARTITION BY mv_periods.customer_id, mv_periods.group_id) AS group_average_discount,
                               group_min_discount                        AS group_minimum_discount,
                               ROUND(COALESCE((transactions_at_discount.transactions_count / group_purchase::numeric),
                                        0),2) AS group_discount_share
                        FROM mv_purchase_history
                                 JOIN mv_periods USING (customer_id, group_id)
                                 LEFT JOIN transactions_at_discount USING (customer_id, group_id)
                )

            SELECT
                affinity_index.customer_id,
                affinity_index.group_id,
                ROUND(affinity_index.group_affinitty_index, 4),
                relative_deviation.group_churn_rate,
                ROUND(COALESCE(AVG(relative_deviation.deviation) OVER w_part_customerid_groupid, 0), 2) AS group_stability_index,
                SUM(
                    CASE
                        WHEN margin_method = 1 AND (transaction_datatime BETWEEN data_of_analysis_date - margin_amount AND data_of_analysis_date)
                             OR (margin_method = 2 AND row_count <= margin_amount)
                        THEN group_summ_paid - group_cost
                        ELSE 0
                    END
                ) OVER w_part_customerid_groupid AS group_margin,
                relative_deviation.group_discount_share,
                relative_deviation.group_minimum_discount,
                ROUND(relative_deviation.group_average_discount, 4)
            FROM
                relative_deviation
                INNER JOIN affinity_index
                    ON affinity_index.customer_id = relative_deviation.customer_id
                    AND affinity_index.group_id = relative_deviation.group_id
            WINDOW
                w_part_customerid_groupid AS (
                    PARTITION BY
                        affinity_index.customer_id,
                        affinity_index.group_id
                );
    END IF;
END;
$$ LANGUAGE plpgsql;


DROP MATERIALIZED VIEW IF EXISTS mv_group CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_group
            (
             customer_id, group_id, group_affinity_index, group_churn_rate,
             group_stability_index, group_margin, group_discount_share,
             group_minimum_discount, group_average_discount
            )
AS
SELECT *
FROM calculate_customer_group_metrics(1, 1000);

SELECT *
FROM mv_group;

SELECT *
FROM mv_group
WHERE customer_id = 11;

SELECT *
FROM mv_group
WHERE group_churn_rate > 10;