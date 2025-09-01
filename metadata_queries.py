"""
Metadata extraction queries for Snowflake.
You can edit or add queries here. The extractor will import this module.
"""
from __future__ import annotations

from typing import Dict


def quote_ident(name: str) -> str:
	return f'"{name}"' if name and not name.startswith('"') else name


def fq_table(database: str | None, schema: str | None, table: str) -> str:
	if database and schema:
		return f'{quote_ident(database)}.{quote_ident(schema)}.{quote_ident(table)}'
	if schema:
		return f'{quote_ident(schema)}.{quote_ident(table)}'
	return quote_ident(table)


QUERIES: Dict[str, str] = {
	"tables": """
		SELECT t.table_catalog AS database_name,
			   t.table_schema AS schema_name,
			   t.table_name,
			   t.table_type,
			   t.row_count,
			   t.created,
			   t.last_altered,
			   d.comment AS table_description,
			   CASE WHEN LOWER(t.table_name) LIKE '%fact%' THEN 'fact'
					WHEN LOWER(t.table_name) LIKE '%dim%' THEN 'dimension'
					ELSE NULL END AS table_category
		FROM information_schema.tables t
		LEFT JOIN information_schema.tables d
		  ON d.table_catalog = t.table_catalog AND d.table_schema = t.table_schema AND d.table_name = t.table_name
		WHERE t.table_type IN ('BASE TABLE','VIEW')
		ORDER BY t.table_schema, t.table_name;
	""",
	"views": """
		SELECT table_catalog AS database_name,
			   table_schema AS schema_name,
			   table_name,
			   created,
			   last_altered
		FROM information_schema.views
		ORDER BY table_schema, table_name;
	""",
	"columns": """
		SELECT c.table_catalog AS database_name,
			   c.table_schema AS schema_name,
			   c.table_name,
			   c.column_name,
			   c.ordinal_position,
			   c.data_type,
			   c.character_maximum_length,
			   c.numeric_precision,
			   c.numeric_scale,
			   c.is_nullable,
			   col.comment AS column_description
		FROM information_schema.columns c
		LEFT JOIN information_schema.columns col
		  ON col.table_catalog=c.table_catalog AND col.table_schema=c.table_schema AND col.table_name=c.table_name AND col.column_name=c.column_name
		ORDER BY c.table_schema, c.table_name, c.ordinal_position;
	""",
	"foreign_keys": """
		SELECT kcu.table_catalog AS database_name,
			   kcu.table_schema AS schema_name,
			   kcu.table_name,
			   kcu.column_name,
			   rc.unique_constraint_catalog AS referenced_database,
			   rc.unique_constraint_schema AS referenced_schema,
			   ccu.table_name AS referenced_table,
			   ccu.column_name AS referenced_column
		FROM information_schema.referential_constraints rc
		JOIN information_schema.key_column_usage kcu
		  ON rc.constraint_catalog=kcu.constraint_catalog AND rc.constraint_schema=kcu.constraint_schema AND rc.constraint_name=kcu.constraint_name
		JOIN information_schema.constraint_column_usage ccu
		  ON rc.unique_constraint_catalog=ccu.constraint_catalog AND rc.unique_constraint_schema=ccu.constraint_schema AND rc.unique_constraint_name=ccu.constraint_name
	""",
	"indexes": """
		SELECT database_name, schema_name, table_name, index_name, column_name
		FROM snowflake.account_usage.table_indexes
		WHERE deleted IS NULL
	""",
	"query_history": """
		SELECT query_text,
			   database_name,
			   schema_name,
			   execution_status,
			   error_code,
			   start_time,
			   end_time,
			   total_elapsed_time
		FROM table(information_schema.query_history())
		ORDER BY start_time DESC
		LIMIT 500;
	""",
}

# Account-level fallback (no database context)
QUERIES_ACCOUNT: Dict[str, str] = {
	"tables": """
		SELECT table_catalog AS database_name,
			   table_schema AS schema_name,
			   table_name,
			   table_type,
			   row_count,
			   created,
			   last_altered,
			   NULL AS table_description,
			   CASE WHEN LOWER(table_name) LIKE '%fact%' THEN 'fact'
					WHEN LOWER(table_name) LIKE '%dim%' THEN 'dimension'
					ELSE NULL END AS table_category
		FROM snowflake.account_usage.tables
		WHERE deleted IS NULL
		ORDER BY database_name, schema_name, table_name;
	""",
	"views": """
		SELECT table_catalog AS database_name,
			   table_schema AS schema_name,
			   table_name,
			   created,
			   last_altered
		FROM snowflake.account_usage.views
		WHERE deleted IS NULL
		ORDER BY database_name, schema_name, table_name;
	""",
	"columns": """
		SELECT table_catalog AS database_name,
			   table_schema AS schema_name,
			   table_name,
			   column_name,
			   ordinal_position,
			   data_type,
			   character_maximum_length,
			   numeric_precision,
			   numeric_scale,
			   is_nullable,
			   NULL AS column_description
		FROM snowflake.account_usage.columns
		WHERE deleted IS NULL
		ORDER BY schema_name, table_name, ordinal_position;
	""",
	"foreign_keys": """
		SELECT database_name AS database_name,
			   schema_name AS schema_name,
			   table_name AS table_name,
			   column_name AS column_name,
			   referenced_database AS referenced_database,
			   referenced_schema AS referenced_schema,
			   referenced_table_name AS referenced_table,
			   referenced_column_name AS referenced_column
		FROM snowflake.account_usage.referential_constraints
		WHERE deleted IS NULL
	""",
	"indexes": """
		SELECT database_name, schema_name, table_name, index_name, column_name
		FROM snowflake.account_usage.table_indexes
		WHERE deleted IS NULL
	""",
	"query_history": """
		SELECT query_text,
			   database_name,
			   schema_name,
			   execution_status,
			   error_code,
			   start_time,
			   end_time,
			   total_elapsed_time
		FROM snowflake.account_usage.query_history
		ORDER BY start_time DESC
		LIMIT 500;
	""",
}


def min_max_distinct_for_column(database: str | None, schema: str | None, table: str, column: str, data_type: str) -> str:
	numeric_types = {"NUMBER", "DECIMAL", "INT", "INTEGER", "BIGINT", "FLOAT", "DOUBLE"}
	date_types = {"DATE", "TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ"}
	text_types = {"TEXT", "VARCHAR", "STRING"}
	fully_qualified = fq_table(database, schema, table)
	col = quote_ident(column)

	if data_type.upper() in numeric_types:
		return f"SELECT MIN({col}) AS min_value, MAX({col}) AS max_value, COUNT(DISTINCT {col}) AS distinct_count FROM {fully_qualified};"
	if data_type.upper() in date_types:
		return f"SELECT MIN({col}) AS min_date, MAX({col}) AS max_date, COUNT(DISTINCT {col}) AS distinct_count FROM {fully_qualified};"
	if data_type.upper() in text_types:
		return f"SELECT COUNT(DISTINCT {col}) AS distinct_count FROM {fully_qualified};"
	return f"SELECT COUNT(DISTINCT {col}) AS distinct_count FROM {fully_qualified};"
