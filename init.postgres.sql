-- create schemas
CREATE SCHEMA IF NOT EXISTS extra;

-- create tables in public schema
CREATE TABLE IF NOT EXISTS public.client_types (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS public.clients (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    type_id INTEGER NOT NULL,
    FOREIGN KEY(type_id) REFERENCES client_types(id)
);

-- create table in extra schema
CREATE TABLE IF NOT EXISTS extra.info_types (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS extra.client_custom_info (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    client_id INTEGER NOT NULL,
    info_type_id INTEGER NOT NULL,
    FOREIGN KEY(client_id) REFERENCES clients(id),
    FOREIGN KEY(info_type_id) REFERENCES extra.info_types(id)
);

CREATE TABLE IF NOT EXISTS extra.client_invoices (
    id SERIAL PRIMARY KEY,
    client_id INTEGER NOT NULL,
    created_at timestamp with time zone,
    FOREIGN KEY(client_id) REFERENCES clients(id)
);

-- lets copy the same table so that we can check if duplicates are handle ok
CREATE TABLE IF NOT EXISTS extra.clients ( 
    extra_data_field double precision,
    LIKE public.clients INCLUDING ALL 
);

-- create partition tables for extra.client_invoices
DO $$
DECLARE
    schema_name TEXT := 'partition';
    table_prefix TEXT := 'client_invoices_';
    number_of_tables_to_create INT := 1000;
    cur_date DATE := '2024-03-01';
    next_date DATE := NULL;
BEGIN
    -- create schema if it doesnt exist
    EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', schema_name);

    -- create tables
    FOR i IN 1..number_of_tables_to_create LOOP
        next_date = cur_date + INTERVAL '1 month';
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I.%I  (
                CONSTRAINT chk_created_at CHECK (created_at >=''%s'' and created_at < ''%s'')
            ) INHERITS (extra.client_invoices)', schema_name, table_prefix || to_char(cur_date, 'YYYY_MM'), cur_date, next_date);

        cur_date = next_date;
    END LOOP;
END $$;
