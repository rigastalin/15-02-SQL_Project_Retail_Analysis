-- ======================== DROP ==============================
DROP TABLE IF EXISTS personal_information CASCADE;
DROP TABLE IF EXISTS cards CASCADE;
DROP TABLE IF EXISTS scu_group CASCADE;
DROP TABLE IF EXISTS product_grid CASCADE;
DROP TABLE IF EXISTS stores CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS checks CASCADE;
DROP TABLE IF EXISTS date_of_analysis CASCADE;
-- ======================== CREATE ==============================

CREATE TABLE personal_information (
    customer_id            BIGSERIAL PRIMARY KEY,
    customer_name          VARCHAR NOT NULL,
    customer_surname       VARCHAR NOT NULL,
    customer_primary_email VARCHAR NOT NULL,
    customer_primary_phone VARCHAR NOT NULL
);


CREATE TABLE cards (
    customer_card_id BIGSERIAL PRIMARY KEY,
    customer_id      BIGINT NOT NULL,
    CONSTRAINT customer_card_id FOREIGN KEY (customer_id) REFERENCES personal_information (customer_id)
);


CREATE TABLE IF NOT EXISTS scu_group (
    group_id   BIGSERIAL PRIMARY KEY,
    group_name VARCHAR NOT NULL
);


CREATE TABLE IF NOT EXISTS product_grid (
    sku_id   BIGSERIAL PRIMARY KEY,
    sku_name VARCHAR NOT NULL,
    group_id BIGINT  NOT NULL
);


CREATE TABLE IF NOT EXISTS stores (
    transaction_store_id BIGINT,
    sku_id               BIGINT  NOT NULL,
    sku_purchase_price   NUMERIC NOT NULL,
    sku_retail_price     NUMERIC NOT NULL
);


SET datestyle = dmy;
CREATE TABLE transactions (
    transaction_id       BIGSERIAL PRIMARY KEY,
    customer_card_id     BIGINT    NOT NULL,
    transaction_summ     NUMERIC   NOT NULL,
    transaction_datatime TIMESTAMP NOT NULL,
    transaction_store_id BIGINT    NOT NULL
);


CREATE TABLE IF NOT EXISTS checks (
    transaction_id BIGSERIAL PRIMARY KEY,
    sku_id         BIGINT  NOT NULL,
    sku_amount     NUMERIC NOT NULL,
    sku_summ       NUMERIC NOT NULL,
    sku_summ_paid  NUMERIC NOT NULL,
    sku_discount   NUMERIC NOT NULL
);


CREATE TABLE IF NOT EXISTS date_of_analysis (
    analysis_formation TIMESTAMP NOT NULL
);

-- ================================= IMPORT ====================================
DROP PROCEDURE IF EXISTS import() CASCADE;

SET path.var TO '/opt/goinfre/cflossie/SQL3_RetailAnalitycs_v1.0-1/datasets/';
CREATE OR REPLACE PROCEDURE import(
    IN name_table text,
    IN name_file text,
    IN delim text DEFAULT E'\t'
)
AS $$
BEGIN
    EXECUTE FORMAT(
        'COPY %I FROM %L WITH DELIMITER %L', name_table, name_file, delim
    );
END;
$$ LANGUAGE plpgsql;

CALL import('personal_information', CURRENT_SETTING('path.var') || 'Personal_Data_Mini.tsv');
CALL import('cards', CURRENT_SETTING('path.var') || 'Cards_Mini.tsv');
CALL import('scu_group', CURRENT_SETTING('path.var') || 'Groups_SKU_Mini.tsv');
CALL import('product_grid', CURRENT_SETTING('path.var') || 'SKU_Mini.tsv');
CALL import('stores', CURRENT_SETTING('path.var') || 'Stores_Mini.tsv');
CALL import('transactions', CURRENT_SETTING('path.var') || 'Transactions_Mini.tsv');
CALL import('checks', CURRENT_SETTING('path.var') || 'Checks_Mini.tsv');
CALL import('date_of_analysis', CURRENT_SETTING('path.var') || 'Date_Of_Analysis_Formation.tsv');


-- ================================= EXPORT ====================================
DROP PROCEDURE IF EXISTS export(name_table text, name_file text, delim text) CASCADE;

CREATE OR REPLACE PROCEDURE export(
    IN name_table text,
    IN name_file text,
    IN delim text DEFAULT E'\t'
)
AS $$
BEGIN
    EXECUTE FORMAT(
        'COPY %I TO %L WITH DELIMITER %L', name_table, name_file, delim
    );
END;
$$ LANGUAGE plpgsql;

SET path.var TO '/opt/goinfre/cflossie/SQL3_RetailAnalitycs_v1.0-1/src/data/';

CALL export('personal_information', CURRENT_SETTING('path.var') || 'Personal_Data_Mini.tsv');
CALL export('cards', CURRENT_SETTING('path.var') || 'Cards_Mini.tsv');
CALL export('scu_group', CURRENT_SETTING('path.var') || 'Groups_SKU_Mini.tsv');
CALL export('product_grid', CURRENT_SETTING('path.var') || 'SKU_Mini.tsv');
CALL export('stores', CURRENT_SETTING('path.var') || 'Stores_Mini.tsv');
CALL export('transactions', CURRENT_SETTING('path.var') || 'Transactions_Mini.tsv');
CALL export('checks', CURRENT_SETTING('path.var') || 'Checks_Mini.tsv');
CALL export('date_of_analysis', CURRENT_SETTING('path.var') || 'Date_Of_Analysis_Formation.tsv');