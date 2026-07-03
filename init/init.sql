-- 1. Create the Debezium service role with SUPERUSER rights right from the start
DO $$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'debezium') THEN
      CREATE ROLE debezium WITH LOGIN REPLICATION SUPERUSER PASSWORD 'dbz_password';
   END IF;
END
$$;

-- 2. Build the tracking table
CREATE TABLE IF NOT EXISTS public.orders (
    id SERIAL PRIMARY KEY,
    customer_name VARCHAR(255),
    amount DECIMAL(10,2),
    status VARCHAR(50),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Set structural snapshot states to record both before and after images
ALTER TABLE public.orders REPLICA IDENTITY FULL;

-- 4. Set explicit ownership
ALTER TABLE public.orders OWNER TO debezium;

-- 5. Insert initial seed baseline data rows
INSERT INTO public.orders (customer_name, amount, status)
VALUES
('Alice', 120.00, 'NEW'),
('Bob', 85.75, 'NEW'),
('Charlie', 99.90, 'NEW');
