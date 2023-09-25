CREATE IF NOT EXISTS DATABASE airflow_db;

DO $$
BEGIN
CREATE ROLE airflow superuser WITH PASSWORD 'airflow';
EXCEPTION WHEN duplicate_object THEN RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
END
$$;

ALTER USER airflow SET search_path = public;
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow;
USE airflow_db;
GRANT ALL ON SCHEMA public TO airflow;