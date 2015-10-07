

-- Creates tablespace for tests with ASSM enabled
DROP TABLESPACE tbs_txjjson
  INCLUDING CONTENTS AND DATAFILES
  CASCADE CONSTRAINTS;

-- testLargeConent() in test_OracleDocument2.java tested 100M doc 
-- so big datafile size is needed
CREATE TABLESPACE tbs_txjjson
DATAFILE 'tbs_txjjson.dbf' SIZE 200M
EXTENT MANAGEMENT LOCAL
SEGMENT SPACE MANAGEMENT AUTO;

ALTER USER MYACCOUNT QUOTA UNLIMITED ON tbs_txjjson;
ALTER USER MYACCOUNT default tablespace tbs_txjjson;

-- Grant necessary privileges to the account used
-- by the tests
grant SODA_APP to MYACCOUNT;
grant CREATE VIEW to MYACCOUNT;
grant CREATE TABLE to MYACCOUNT;
grant CREATE SEQUENCE to MYACCOUNT;
grant CREATE PROCEDURE to MYACCOUNT;
grant CREATE TRIGGER to MYACCOUNT;

-- check db char set
SELECT value FROM nls_database_parameters WHERE parameter='NLS_CHARACTERSET';

-- check encryption wallet info
select * from V$ENCRYPTION_WALLET;


