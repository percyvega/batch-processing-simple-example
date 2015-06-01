DROP TABLE people IF EXISTS;

CREATE TABLE people  (
    person_id BIGINT IDENTITY NOT NULL PRIMARY KEY,
    full_name VARCHAR(30),
    address VARCHAR(55)
);
