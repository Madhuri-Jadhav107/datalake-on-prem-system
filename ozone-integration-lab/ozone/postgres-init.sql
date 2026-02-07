-- Create a sample table for CDC demonstration
CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255),
    city VARCHAR(255),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert some sample data
INSERT INTO customers (name, email, city) VALUES 
('Madhuri Jadhav', 'madhuri@example.com', 'Mumbai'),
('John Doe', 'john.doe@example.com', 'New York'),
('Alice Smith', 'alice.s@example.com', 'London');
