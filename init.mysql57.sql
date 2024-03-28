GRANT ALL PRIVILEGES ON *.* TO 'example_user'@'%' WITH GRANT OPTION;

-- create schemas
CREATE SCHEMA IF NOT EXISTS extra;
CREATE SCHEMA IF NOT EXISTS public;

-- create tables in public schema
CREATE TABLE IF NOT EXISTS public.client_types (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS public.clients (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    type_id INT NOT NULL,
    FOREIGN KEY(type_id) REFERENCES client_types(id)
);

-- create table in extra schema
CREATE TABLE IF NOT EXISTS extra.info_types (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS extra.client_custom_info (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    client_id INT NOT NULL,
    info_type_id INT NOT NULL,
    FOREIGN KEY(client_id) REFERENCES public.clients(id),
    FOREIGN KEY(info_type_id) REFERENCES extra.info_types(id)
);

CREATE TABLE IF NOT EXISTS extra.client_invoices (
    id INT AUTO_INCREMENT PRIMARY KEY,
    client_id INT NOT NULL,
    created_at TIMESTAMP,
    FOREIGN KEY(client_id) REFERENCES public.clients(id)
);

-- lets copy the same table so that we can check if duplicates are handled ok
CREATE TABLE IF NOT EXISTS extra.clients LIKE public.clients;
ALTER TABLE extra.clients ADD COLUMN extra_data_field DOUBLE;

-- create partition tables for extra.client_invoices
DELIMITER $$
CREATE PROCEDURE CreatePartitionTables()
BEGIN
    DECLARE schema_name VARCHAR(255) DEFAULT 'partition';
    DECLARE table_prefix VARCHAR(255) DEFAULT 'client_invoices_';
    DECLARE number_of_tables_to_create INT DEFAULT 1000;
    DECLARE cur_date DATE DEFAULT '2024-03-01';
    DECLARE next_date DATE;
    DECLARE table_name VARCHAR(255);

    -- create schema if it doesn't exist
    SET @schema_query = CONCAT('CREATE SCHEMA IF NOT EXISTS ', sys.quote_identifier(schema_name));
    PREPARE schema_stmt FROM @schema_query;
    EXECUTE schema_stmt;
    DEALLOCATE PREPARE schema_stmt;

    -- create tables
    WHILE number_of_tables_to_create > 0 DO
        SET next_date = DATE_ADD(cur_date, INTERVAL 1 MONTH);

        SET table_name = sys.quote_identifier(concat(schema_name, '.', table_prefix, DATE_FORMAT(cur_date, '%Y_%m')));

        SET @table_query = CONCAT('CREATE TABLE IF NOT EXISTS ', table_name, ' LIKE extra.client_invoices');
        PREPARE table_stmt FROM @table_query;
        EXECUTE table_stmt;

        SET @table_query = CONCAT('ALTER TABLE ', table_name, ' ADD CONSTRAINT chk_created_at CHECK (created_at >= ''', cur_date, ''' AND created_at < ''', next_date, ''')');
        PREPARE table_stmt FROM @table_query;
        EXECUTE table_stmt;
        DEALLOCATE PREPARE table_stmt;

        SET cur_date = next_date;
        SET number_of_tables_to_create = number_of_tables_to_create - 1;
    END WHILE;
END$$
DELIMITER ;

CALL CreatePartitionTables();
DROP PROCEDURE CreatePartitionTables;
