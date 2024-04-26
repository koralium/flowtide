CREATE TABLE dbserver1.inventory.customers (
  id,
  first_name,
  last_name,
  email
);

INSERT INTO outputtable
SELECT 
  first_name,
  last_name, 
  email
FROM dbserver1.inventory.customers;