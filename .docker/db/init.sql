-- A procedure to populate a database with tables and data
-- We define it in template1 so it is available in all databases created AFTER it.
\c template1
CREATE OR REPLACE FUNCTION populate_db_func() RETURNS void LANGUAGE plpgsql AS $$ 
DECLARE t_name TEXT; 
BEGIN 
    FOR i IN 1..10 LOOP 
        t_name := 'table' || i; 
        EXECUTE format('CREATE TABLE %I (id SERIAL PRIMARY KEY, val TEXT, created_at TIMESTAMP DEFAULT now())', t_name); 
        EXECUTE format('INSERT INTO %I (val) SELECT ''value '' || g FROM generate_series(1, 1000000) g', t_name);
    END LOOP; 
END; $$;

-- Create 10 databases (they will inherit populate_db_func from template1)
CREATE DATABASE db1;
CREATE DATABASE db2;
CREATE DATABASE db3;
CREATE DATABASE db4;
CREATE DATABASE db5;
CREATE DATABASE db6;
CREATE DATABASE db7;
CREATE DATABASE db8;
CREATE DATABASE db9;
CREATE DATABASE db10;

-- Populate each database
\c db1
SELECT populate_db_func();

\c db2
SELECT populate_db_func();

\c db3
SELECT populate_db_func();

\c db4
SELECT populate_db_func();

\c db5
SELECT populate_db_func();

\c db6
SELECT populate_db_func();

\c db7
SELECT populate_db_func();

\c db8
SELECT populate_db_func();

\c db9
SELECT populate_db_func();

\c db10
SELECT populate_db_func();
