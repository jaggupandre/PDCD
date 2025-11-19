--=== Table structure ======

CREATE SCHEMA IF NOT EXISTS analytics_schema;
--==============================
CREATE TABLE analytics_schema.departments (
    department_id SERIAL PRIMARY KEY,
    department_name VARCHAR(100) NOT NULL,
    main_location VARCHAR(100),
    ternary_location VARCHAR(100),
    manager_id INT,
    budget_code VARCHAR(50)
);
--==============================
CREATE TABLE analytics_schema.employees (
    employee_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100),
    email VARCHAR(150) UNIQUE NOT NULL,
    phone_number VARCHAR(20),
    hire_date DATE NOT NULL DEFAULT CURRENT_DATE,
    salary NUMERIC(10,2),
    department_id INT NOT NULL REFERENCES analytics_schema.departments(department_id)
);
--==============================


--- Trigger Functions -----

CREATE OR REPLACE FUNCTION analytics_schema.fn_check_salary()
RETURNS trigger AS $$
BEGIN
    IF NEW.salary < 0 THEN
        RAISE EXCEPTION 'Salary cannot be negative';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
--==============================
CREATE OR REPLACE FUNCTION analytics_schema.fn_employee_insert_audit()
RETURNS trigger AS $$
BEGIN
    RAISE NOTICE 'Employee % inserted at %', NEW.employee_id, now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
--==============================
CREATE OR REPLACE FUNCTION analytics_schema.fn_department_update_audit()
RETURNS trigger AS $$
BEGIN
    RAISE NOTICE 'Department % updated at %', NEW.department_id, now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

--==============================
CREATE OR REPLACE FUNCTION analytics_schema.fn_employee_delete_cleanup()
RETURNS trigger AS $$
BEGIN
    RAISE NOTICE 'Cleanup for deleted employee %', OLD.employee_id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

--==============================

-- Triggers -----

CREATE TRIGGER trg_check_salary
BEFORE INSERT OR UPDATE ON analytics_schema.employees
FOR EACH ROW
EXECUTE FUNCTION analytics_schema.fn_check_salary();

--==============================

CREATE TRIGGER trg_employee_insert_audit
AFTER INSERT ON analytics_schema.employees
FOR EACH ROW
EXECUTE FUNCTION analytics_schema.fn_employee_insert_audit();
--==============================

CREATE TRIGGER trg_department_update_audit
AFTER UPDATE ON analytics_schema.departments
FOR EACH ROW
EXECUTE FUNCTION analytics_schema.fn_department_update_audit();
--==============================

CREATE TRIGGER trg_employee_delete_cleanup
AFTER DELETE ON analytics_schema.employees
FOR EACH ROW
EXECUTE FUNCTION analytics_schema.fn_employee_delete_cleanup();

--==============================

-- Statement-level Trigger Example
CREATE OR REPLACE FUNCTION analytics_schema.fn_statement_audit()
RETURNS trigger AS $$
BEGIN
    RAISE NOTICE 'Statement-level audit executed';
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_employees_stmt_audit
AFTER UPDATE ON analytics_schema.employees
FOR EACH STATEMENT
EXECUTE FUNCTION analytics_schema.fn_statement_audit();

--==============================

-- Test the trigger details function
SELECT * FROM pdcd_schema.get_trigger_details(ARRAY['analytics_schema']);

   schema_name    | table_name |   trigger_name   |                                                                   trigger_definition                                                                   | trigger_event | trigger_timing | trigger_level | trigger_enabled | trigger_function_name | trigger_function_arguments |                  trigger_function_definition
------------------+------------+------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------+---------------+----------------+---------------+-----------------+-----------------------+----------------------------+---------------------------------------------------------------
 analytics_schema | employees  | trg_check_salary | CREATE TRIGGER trg_check_salary BEFORE INSERT OR UPDATE ON analytics_schema.employees FOR EACH ROW EXECUTE FUNCTION analytics_schema.fn_check_salary() | INSERT,UPDATE | BEFORE         | ROW           | t               | fn_check_salary       |                            | CREATE OR REPLACE FUNCTION analytics_schema.fn_check_salary()+
                  |            |                  |                                                                                                                                                        |               |                |               |                 |                       |                            |  RETURNS trigger                                             +
                  |            |                  |                                                                                                                                                        |               |                |               |                 |                       |                            |  LANGUAGE plpgsql                                            +
                  |            |                  |                                                                                                                                                        |               |                |               |                 |                       |                            | AS $function$                                                +
                  |            |                  |                                                                                                                                                        |               |                |               |                 |                       |                            | BEGIN                                                        +
                  |            |                  |                                                                                                                                                        |               |                |               |                 |                       |                            |     IF NEW.salary < 0 THEN                                   +
                  |            |                  |                                                                                                                                                        |               |                |               |                 |                       |                            |         RAISE EXCEPTION 'Salary cannot be negative';         +
                  |            |                  |                                                                                                                                                        |               |                |               |                 |                       |                            |     END IF;                                                  +
                  |            |                  |                                                                                                                                                        |               |                |               |                 |                       |                            |     RETURN NEW;                                              +
                  |            |                  |                                                                                                                                                        |               |                |               |                 |                       |                            | END;                                                         +
                  |            |                  |                                                                                                                                                        |               |                |               |                 |                       |                            | $function$                                                   +
                  |            |                  |                                                                                                                                                        |               |                |               |                 |                       |                            |
(1 row)

test_db=# SELECT * FROM pdcd_schema.get_table_triggers_md5(ARRAY['analytics_schema']);

   schema_name    | object_type | object_type_name | object_subtype | object_subtype_name |                                                                                                                                                                                                     object_subtype_details                                                                                                                                                                                                     |            object_md5
------------------+-------------+------------------+----------------+---------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------
 analytics_schema | Table       | employees        | Trigger        | trg_check_salary    | trigger_event:INSERT,UPDATE,trigger_timing:BEFORE,trigger_level:ROW,trigger_enabled:true,trigger_definition:CREATE TRIGGER trg_check_salary BEFORE INSERT OR UPDATE ON analytics_schema.employees FOR EACH ROW EXECUTE FUNCTION analytics_schema.fn_check_salary(),trigger_function_name:fn_check_salary,trigger_function_arguments:,trigger_function_definition:CREATE OR REPLACE FUNCTION analytics_schema.fn_check_salary()+| fcd3aeb09a37d99fc06e65abeb64c115
                  |             |                  |                |                     |  RETURNS trigger                                                                                                                                                                                                                                                                                                                                                                                                              +|
                  |             |                  |                |                     |  LANGUAGE plpgsql                                                                                                                                                                                                                                                                                                                                                                                                             +|
                  |             |                  |                |                     | AS $function$                                                                                                                                                                                                                                                                                                                                                                                                                 +|
                  |             |                  |                |                     | BEGIN                                                                                                                                                                                                                                                                                                                                                                                                                         +|
                  |             |                  |                |                     |     IF NEW.salary < 0 THEN                                                                                                                                                                                                                                                                                                                                                                                                    +|
                  |             |                  |                |                     |         RAISE EXCEPTION 'Salary cannot be negative';                                                                                                                                                                                                                                                                                                                                                                          +|
                  |             |                  |                |                     |     END IF;                                                                                                                                                                                                                                                                                                                                                                                                                   +|
                  |             |                  |                |                     |     RETURN NEW;                                                                                                                                                                                                                                                                                                                                                                                                               +|
                  |             |                  |                |                     | END;                                                                                                                                                                                                                                                                                                                                                                                                                          +|
                  |             |                  |                |                     | $function$                                                                                                                                                                                                                                                                                                                                                                                                                    +|
                  |             |                  |                |                     |                                                                                                                                                                                                                                                                                                                                                                                                                                |
(1 row)