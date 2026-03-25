CREATE EXTENSION IF NOT EXISTS pgcrypto;

INSERT INTO PHONES (phoneUUID, phoneName)
VALUES (gen_random_uuid(), 'iPhone 16 Pro');

INSERT INTO USERS (userUUID, userName)
SELECT gen_random_uuid(), 'User ' || i
FROM generate_series(1, 100000) AS s(i);