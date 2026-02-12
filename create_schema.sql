CREATE SCHEMA IF NOT EXISTS sdumont2nd;

CREATE TABLE sdumont2nd.job (
    jobid       INTEGER PRIMARY KEY,
    jobname     VARCHAR(255),
    account     VARCHAR(50),
    partition   VARCHAR(50),
    nnodes      INTEGER,
    alloccpus   INTEGER,
    jobstart    TIMESTAMP,
    jobend      TIMESTAMP,
    elapsed     VARCHAR(20),
    status      VARCHAR(20),
    nodelist    VARCHAR(1000),
    task        VARCHAR(255),
    username    VARCHAR(50),
    reqtres     VARCHAR(80)
);

-- O comando abaixo foi utilizado para criar a tabela job do schema sdumont2nd
-- Se baseando no schema sdumont.
-- CREATE TABLE sdumont2nd.job (LIKE sdumont.job INCLUDING ALL);

