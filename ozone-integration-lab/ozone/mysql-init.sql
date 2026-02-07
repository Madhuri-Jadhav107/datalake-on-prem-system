-- Create database
CREATE DATABASE IF NOT EXISTS inventory;
USE inventory;

-- Create customers table
CREATE TABLE IF NOT EXISTS customers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    city VARCHAR(255) NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Insert sample data
INSERT INTO customers (name, email, city) VALUES ('Virat Kohli', 'virat@gmail.com', 'Delhi');
INSERT INTO customers (name, email, city) VALUES ('Rohit Sharma', 'rohit@gmail.com', 'Mumbai');
INSERT INTO customers (name, email, city) VALUES ('Sachin Tendulkar', 'sachin@gmail.com', 'Mumbai');
