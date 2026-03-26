CREATE TABLE CONFIG (
    key VARCHAR PRIMARY KEY,
    value INT DEFAULT 0
);

INSERT INTO CONFIG (key, value) VALUES ('total_paid', 0);