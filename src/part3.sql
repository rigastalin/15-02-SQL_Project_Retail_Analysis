DROP OWNED BY administrator;
DROP ROLE IF EXISTS administrator;

DROP OWNED BY visitor;
DROP ROLE IF EXISTS  visitor;

CREATE ROLE administrator LOGIN PASSWORD 'admin';
GRANT ALL PRIVILEGES ON DATABASE postgres TO administrator;
GRANT ALL ON SCHEMA information_schema TO administrator;
GRANT USAGE ON SCHEMA information_schema TO administrator;
GRANT pg_read_all_data TO administrator;
GRANT pg_write_all_data TO administrator;

CREATE ROLE visitor LOGIN PASSWORD 'phuckyoupayme';
GRANT USAGE ON SCHEMA information_schema TO visitor;
GRANT SELECT ON ALL TABLES IN SCHEMA information_schema TO visitor;
ALTER DEFAULT PRIVILEGES IN SCHEMA information_schema GRANT SELECT ON TABLES TO visitor;