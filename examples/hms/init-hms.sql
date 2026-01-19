-- Initialize Hive Metastore database and user
CREATE DATABASE metastore;
CREATE USER hive WITH PASSWORD 'hive';
GRANT ALL PRIVILEGES ON DATABASE metastore TO hive;

-- Connect to metastore database and grant schema permissions
\c metastore
GRANT ALL ON SCHEMA public TO hive;
