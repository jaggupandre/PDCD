-- 1️⃣ Create the parent table
CREATE TABLE analytics_schema.departments (
    department_id SERIAL PRIMARY KEY,
    department_name VARCHAR(100) NOT NULL UNIQUE
);


ALTER TABLE analytics_schema.departments
    ADD COLUMN primary_location VARCHAR(100);

ALTER TABLE analytics_schema.departments
    RENAME COLUMN department_name TO dept_name;


-- 2️⃣ Create the child table
CREATE TABLE analytics_schema.employees (
    employee_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50),
    department_id INT REFERENCES analytics_schema.departments(department_id) ON DELETE SET NULL,
    salary NUMERIC(10,2),
    hire_date DATE DEFAULT CURRENT_DATE
);

test_db=# \dt analytics_schema.*
                    List of relations
      Schema      |    Name     | Type  |     Owner
------------------+-------------+-------+----------------
 analytics_schema | departments | table | jagdish_pandre
 analytics_schema | employees   | table | jagdish_pandre