DROP TABLE IF EXISTS analytics_schema.employees;
DROP TABLE IF EXISTS analytics_schema.departments;

CREATE TABLE analytics_schema.departments (
    department_id SERIAL PRIMARY KEY,                            -- PK
    department_name VARCHAR(100) NOT NULL UNIQUE,                 -- Unique + Not Null
    main_location VARCHAR(100) DEFAULT 'Headquarters',            -- Default constraint
    ternary_location VARCHAR(100),
    manager_id INT CHECK (manager_id > 0),                        -- Check constraint
    budget_code VARCHAR(50) UNIQUE CHECK (budget_code <> '')      -- Unique + Check
);


CREATE TABLE analytics_schema.employees (
    employee_id SERIAL PRIMARY KEY,                             -- PK
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100),
    email VARCHAR(150) UNIQUE NOT NULL,                         -- Unique + NN
    phone_number VARCHAR(20) CHECK (phone_number ~ '^[0-9+-]+$'),
    hire_date DATE NOT NULL DEFAULT CURRENT_DATE,               -- NN + Default
    salary NUMERIC(10,2) CHECK (salary >= 0),                   -- Check
    department_id INT NOT NULL,                                 -- FK reference
    CONSTRAINT fk_department
        FOREIGN KEY (department_id)
        REFERENCES analytics_schema.departments(department_id)
        ON UPDATE CASCADE ON DELETE SET NULL
);

-- | Constraint | Column(s)                                   | Type                 |
-- | ---------- | ------------------------------------------- | -------------------- |
-- | PK         | employee_id                                 | PRIMARY KEY          |
-- | NN         | first_name, email, hire_date, department_id | NOT NULL             |
-- | UNIQUE     | email                                       | UNIQUE               |
-- | CHECK      | phone_number, salary                        | CHECK                |
-- | DEFAULT    | hire_date                                   | DEFAULT CURRENT_DATE |
-- | FK         | department_id → departments(department_id)  | FOREIGN KEY          |

-- \i '/Users/jagdish_pandre/meta_data_report/PDCD/PDCD/sql_dev/temp_folder_testing/Constraints/new_test_0.sql'