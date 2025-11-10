-- Enable replication role for Debezium later
CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'dbz_password';

-- Create demo table
CREATE TABLE public.orders (
    id SERIAL PRIMARY KEY,
    customer_name VARCHAR(255),
    amount DECIMAL(10, 2),
    status VARCHAR(50),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data
INSERT INTO public.orders (customer_name, amount, status)
VALUES
('Alice', 120.00, 'NEW'),
('Bob', 85.75, 'NEW'),
('Charlie', 99.90, 'NEW');
