```sql
-- Run this in DBeaver to confirm which database you're connected to
SELECT current_database(), current_schema;

-- Check if tables exist in the system catalog
SELECT table_schema, table_name 
FROM information_schema.tables 
WHERE table_name LIKE 'imdb_%';

-- Check if the Airflow user has permissions on these tables
SELECT grantee, privilege_type, table_schema, table_name
FROM information_schema.table_privileges 
WHERE table_name LIKE 'imdb_%';
```