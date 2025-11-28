TRUNCATE TABLE pdcd_schema.snapshot_tbl RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.md5_metadata_tbl RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.md5_metadata_staging_tbl RESTART IDENTITY CASCADE;
TRUNCATE TABLE pdcd_schema.md5_metadata_staging_functions RESTART IDENTITY CASCADE;

DROP SCHEMA IF EXISTS analytics_schema CASCADE;
CREATE SCHEMA analytics_schema;

-- Details and MD5
SELECT * FROM pdcd_schema.get_functions_details(ARRAY['analytics_schema']);
SELECT * FROM pdcd_schema.get_table_functions_md5(ARRAY['analytics_schema']);

CREATE OR REPLACE FUNCTION analytics_schema.fn_add(a INT, b INT)
RETURNS INT
LANGUAGE SQL AS $$ SELECT a + b; $$;

CREATE OR REPLACE FUNCTION analytics_schema.fn_sub(c INT, d INT)
RETURNS INT
LANGUAGE SQL AS $$ SELECT c + d; $$;

CREATE OR REPLACE FUNCTION analytics_schema.fn_sub(g INT, h NUMERIC)
RETURNS NUMERIC
LANGUAGE SQL AS $$ SELECT (g + h)::NUMERIC; $$;

SELECT * FROM pdcd_schema.load_snapshot_tbl();
SELECT * FROM pdcd_schema.load_md5_metadata_tbl(ARRAY['analytics_schema']);
SELECT * FROM pdcd_schema.load_md5_metadata_staging_functions(ARRAY['analytics_schema']);

SELECT snapshot_id, schema_name,object_type, object_type_name, object_subtype, object_subtype_name, object_md5,    change_type, object_subtype_details
from pdcd_schema.md5_metadata_tbl WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM pdcd_schema.md5_metadata_tbl);


   schema_name    | object_type | object_type_name | object_subtype | object_subtype_name |                                                                                                               object_subtype_details                                                                                                               |            object_md5
------------------+-------------+------------------+----------------+---------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------
 analytics_schema | Function    | fn_add           |                |                     | argument_types:a integer, b integer,argument_modes:a integer, b integer,return_type:integer,language:sql,volatility:VOLATILE,parallel_safe:UNSAFE,owner_role:jagdish_pandre,privileges:,dependencies:169949,function_body:SELECT a + b;            | 212e0613aa39f1134b8b105bfefb621e
 analytics_schema | Function    | fn_sub           |                |                     | argument_types:c integer, d integer,argument_modes:c integer, d integer,return_type:integer,language:sql,volatility:VOLATILE,parallel_safe:UNSAFE,owner_role:jagdish_pandre,privileges:,dependencies:169949,function_body:SELECT c + d;            | 4c6189fd80ece92c47ab0f47a8b9d8ea
 analytics_schema | Function    | fn_sub           |                |                     | argument_types:g integer, h numeric,argument_modes:g integer, h numeric,return_type:numeric,language:sql,volatility:VOLATILE,parallel_safe:UNSAFE,owner_role:jagdish_pandre,privileges:,dependencies:169949,function_body:SELECT (g + h)::NUMERIC; | ece008d97c789102b23adf4bd909fa34
(3 rows)

   schema_name    | object_type |       object_type_name       | object_subtype | object_subtype_name |            object_md5            | change_type |                                                                                                               object_subtype_details
-------------+------------------+-------------+------------------------------+----------------+---------------------+----------------------------------+-------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 analytics_schema | Function    | fn_add(a integer, b integer) |                |                     | 3a9c2019fe703d0b9ef07be755b3a40a | ADDED       | argument_types:a integer, b integer,argument_modes:a integer, b integer,return_type:integer,language:sql,volatility:VOLATILE,parallel_safe:UNSAFE,owner_role:jagdish_pandre,privileges:,dependencies:170508,function_body:SELECT a + b;
 analytics_schema | Function    | fn_sub(c integer, d integer) |                |                     | 9b645814819b4facdb23cbd8b7924b74 | ADDED       | argument_types:c integer, d integer,argument_modes:c integer, d integer,return_type:integer,language:sql,volatility:VOLATILE,parallel_safe:UNSAFE,owner_role:jagdish_pandre,privileges:,dependencies:170508,function_body:SELECT c + d;
 analytics_schema | Function    | fn_sub(g integer, h numeric) |                |                     | b5ec8d8e376aa50952fd57879be1d6cb | ADDED       | argument_types:g integer, h numeric,argument_modes:g integer, h numeric,return_type:numeric,language:sql,volatility:VOLATILE,parallel_safe:UNSAFE,owner_role:jagdish_pandre,privileges:,dependencies:170508,function_body:SELECT (g + h)::NUMERIC;
(3 rows)

-- =================
-- RENAMING
-- =================
ALTER FUNCTION analytics_schema.fn_sub
RENAME TO fn_subtraction;



CREATE OR REPLACE FUNCTION analytics_schema.fn_sub(e INT, f INT)
RETURNS INT
LANGUAGE SQL AS $$ SELECT e + f; $$;