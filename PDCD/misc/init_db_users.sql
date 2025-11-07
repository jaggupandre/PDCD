-- 1️⃣ Create the user
CREATE USER pdcd_user WITH PASSWORD 'pdcd_pass';

-- 2️⃣ Create the database owned by this user
CREATE DATABASE pdcd_db OWNER pdcd_user;

-- 3️⃣ Connect to the new database
\c pdcd_db

-- 4️⃣ Grant full privileges inside the database
GRANT ALL PRIVILEGES ON DATABASE pdcd_db TO pdcd_user;

-- 5️⃣ Allow schema creation and usage
GRANT CREATE, CONNECT, TEMP ON DATABASE pdcd_db TO pdcd_user;

-- 6️⃣ (Optional) If you already have a schema like 'public', make the user its owner
ALTER SCHEMA public OWNER TO pdcd_user;

-- 7️⃣ Ensure the user can create and modify all types of objects in future schemas
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT ALL ON TABLES TO pdcd_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT ALL ON SEQUENCES TO pdcd_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT ALL ON FUNCTIONS TO pdcd_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT ALL ON TYPES TO pdcd_user;
