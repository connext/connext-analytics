SELECT 
    table_schema, 
    table_name
FROM 
    information_schema.tables
WHERE table_schema = 'public'
ORDER BY 
    table_schema, 
    table_name;