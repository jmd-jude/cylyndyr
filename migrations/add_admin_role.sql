-- Add is_admin column to users table
ALTER TABLE users ADD COLUMN is_admin BOOLEAN NOT NULL DEFAULT FALSE;

-- Set test@test.com as admin
UPDATE users SET is_admin = TRUE WHERE email = 'test@test.com';
