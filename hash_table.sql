-- Drop table

-- DROP TABLE dbo.hash_table;

CREATE TABLE aaMarcos_LW_Test.dbo.hash_table (
	database_name varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	table_schema nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	table_name sysname COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	ID varchar(18) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	row_hash varchar(61) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
);