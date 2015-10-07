Rem sodatestsetup.sql
Rem
Rem Copyright (c) 2014, Oracle and/or its affiliates. All rights reserved.
Rem
Rem    NAME
Rem      sodatestsetup.sql - sql script to setup SODA tests env
Rem
Rem    DESCRIPTION
Rem      Create table/view for SODA tests
Rem
Rem    MODIFIED         (MM/DD/YY)
Rem    Vincent Liu     08/31/14 - Created
Rem

SET ECHO ON
SET FEEDBACK 1
SET NUMWIDTH 10
SET LINESIZE 80
SET TRIMSPOOL ON
SET TAB OFF
SET PAGESIZE 100

drop view soda_view;

drop table soda_table;

create table soda_table (ID VARCHAR2(255) NOT NULL PRIMARY KEY, CONTENT_TYPE VARCHAR2(255),
    CREATED_ON TIMESTAMP(6), LAST_MODIFIED TIMESTAMP(6), VERSION VARCHAR2(255), JSON_DOCUMENT BLOB);

create view soda_view as select * from soda_table;

create FUNCTION c2b( c IN CLOB ) RETURN BLOB
as
  pos PLS_INTEGER := 1;
  buffer RAW( 32767 );
  res BLOB;
  lob_len PLS_INTEGER := DBMS_LOB.getLength( c );
BEGIN
  DBMS_LOB.createTemporary( res, TRUE );
  DBMS_LOB.OPEN( res, DBMS_LOB.LOB_ReadWrite );
  LOOP
    buffer := UTL_RAW.cast_to_raw( DBMS_LOB.SUBSTR( c, 16000, pos ) );
    IF UTL_RAW.LENGTH( buffer ) > 0 THEN
      DBMS_LOB.writeAppend( res, UTL_RAW.LENGTH( buffer ), buffer );
    END IF;
    pos := pos + 16000;
    EXIT WHEN pos > lob_len;
  END LOOP;
RETURN res; -- res is OPEN here
END c2b;
/

-- create sodatbl and sequence sodatbl_version_seq for testGetVersion()
drop table sodatbl;
drop sequence sodatbl_version_seq;

create table sodatbl (ID VARCHAR2(255) NOT NULL PRIMARY KEY, CONTENT_TYPE VARCHAR2(255),
    CREATED_ON TIMESTAMP(6), LAST_MODIFIED TIMESTAMP(6), VERSION NUMBER(6) NOT NULL, JSON_DOCUMENT BLOB);

create sequence sodatbl_version_seq increment by 1 start with 1 minvalue 1 maxvalue 9999999999999 nocache order;

create or replace trigger sodatbl_trigger
before insert or update on sodatbl
for each row
begin
    select sodatbl_version_seq.nextval into:new.version from sys.dual ;
end;
/

commit;

