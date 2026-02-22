CREATE EXTENSION IF NOT EXISTS pgcrypto;

INSERT INTO PHONES (phoneUUID, phoneName)
SELECT gen_random_uuid(), 'Phone ' || i
FROM generate_series(1, 10) AS s(i);

INSERT INTO USERS (userUUID, userName)
SELECT gen_random_uuid(), 'User ' || i
FROM generate_series(1, 100) AS s(i);