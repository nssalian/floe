-- Create Gravitino database and user
CREATE USER gravitino WITH PASSWORD 'gravitino';
CREATE DATABASE gravitino OWNER gravitino;
GRANT ALL PRIVILEGES ON DATABASE gravitino TO gravitino;
