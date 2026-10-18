-- A user ID uniquely identifies a row; repeat pipeline runs ignore conflicts.
CREATE TABLE IF NOT EXISTS users (
    id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100)
);
