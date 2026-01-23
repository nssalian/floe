-- Create Lakekeeper database and user
CREATE USER lakekeeper WITH PASSWORD 'lakekeeper';
CREATE DATABASE lakekeeper OWNER lakekeeper;
GRANT ALL PRIVILEGES ON DATABASE lakekeeper TO lakekeeper;
