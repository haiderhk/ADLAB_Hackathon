from __future__ import annotations

import logging
import json
import os
from typing import Any, Dict, List
from decimal import Decimal
from datetime import datetime, date

import pandas as pd
import snowflake.connector
from tenacity import retry, stop_after_attempt, wait_exponential

from config import app_config, snowflake_config
from graphdb_utils import GraphDB
from metadata_queries import QUERIES, QUERIES_ACCOUNT, min_max_distinct_for_column

logger = logging.getLogger(__name__)


def _json_safe(value: Any) -> Any:
	if isinstance(value, Decimal):
		return float(value)
	if isinstance(value, (datetime, date)):
		return value.isoformat()
	return value


def _json_safe_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
	return [{k: _json_safe(v) for k, v in r.items()} for r in records]


def get_sf_connection() -> snowflake.connector.connection.SnowflakeConnection:
	return snowflake.connector.connect(
		account=snowflake_config.account,
		user=snowflake_config.user,
		password=snowflake_config.password,
		warehouse=snowflake_config.warehouse,
		role=snowflake_config.role,
		database=snowflake_config.database or None,
		schema=snowflake_config.schema or None,
	)


def apply_context(conn: snowflake.connector.connection.SnowflakeConnection) -> None:
	cur = conn.cursor()
	try:
		if snowflake_config.role:
			cur.execute(f"USE ROLE {snowflake_config.role}")
		if snowflake_config.warehouse:
			cur.execute(f"USE WAREHOUSE {snowflake_config.warehouse}")
		if snowflake_config.database:
			cur.execute(f"USE DATABASE {snowflake_config.database}")
		if snowflake_config.schema:
			cur.execute(f"USE SCHEMA {snowflake_config.schema}")
	finally:
		cur.close()


@retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(3))
def run_query(conn: snowflake.connector.connection.SnowflakeConnection, sql: str) -> pd.DataFrame:
	cur = conn.cursor()
	try:
		cur.execute(sql)
		try:
			df = cur.fetch_pandas_all()
		except Exception:
			df = pd.DataFrame()
		return df
	finally:
		cur.close()


def _get(row: Dict[str, Any], key: str) -> Any:
	return row.get(key, row.get(key.upper()))


def connection_test() -> Dict[str, Any]:
	try:
		conn = get_sf_connection()
		try:
			apply_context(conn)
			df = run_query(conn, "SELECT CURRENT_ACCOUNT() AS acct, CURRENT_ROLE() AS role, CURRENT_WAREHOUSE() AS wh, CURRENT_DATABASE() AS db, CURRENT_SCHEMA() AS sch")
			return {"ok": True, "info": df.to_dict(orient="records")}
		finally:
			conn.close()
	except Exception as e:
		logger.exception("Connection test failed")
		return {"ok": False, "error": str(e)}


def extract_metadata() -> Dict[str, List[Dict[str, Any]]]:
	conn = get_sf_connection()
	results: Dict[str, List[Dict[str, Any]]] = {"tables": [], "views": [], "columns": [], "query_history": []}

	try:
		apply_context(conn)
		queries = QUERIES if snowflake_config.database else QUERIES_ACCOUNT
		for key, sql in queries.items():
			logger.info("Running metadata query: %s", key)
			try:
				df = run_query(conn, sql)
			except Exception as e:
				logger.exception("Metadata query failed for %s", key)
				df = pd.DataFrame()
			results[key] = _json_safe_records(df.to_dict(orient="records"))

		# Write early snapshot even if stats/graph fail
		os.makedirs("./data", exist_ok=True)
		with open("./data/metadata_latest.json", "w", encoding="utf-8") as f:
			json.dump(results, f, ensure_ascii=False, indent=2)

		# Derive stats (best-effort)
		try:
			columns = results.get("columns", [])
			per_table: Dict[tuple, List[Dict[str, Any]]] = {}
			for c in columns:
				db = c.get("database_name") or c.get("DATABASE_NAME")
				schema = c.get("schema_name") or c.get("SCHEMA_NAME")
				table = c.get("table_name") or c.get("TABLE_NAME")
				if not table:
					continue
				tkey = (db, schema, table)
				per_table.setdefault(tkey, []).append(c)
			stats: List[Dict[str, Any]] = []
			for tkey, cols in per_table.items():
				for c in cols[:5]:
					col = c.get("column_name") or c.get("COLUMN_NAME")
					dtype = c.get("data_type") or c.get("DATA_TYPE")
					if not col or not dtype:
						continue
					sql = min_max_distinct_for_column(tkey[0], tkey[1], tkey[2], col, dtype)
					try:
						df = run_query(conn, sql)
					except Exception as e:
						logger.warning("Stat query failed for %s.%s.%s.%s: %s", tkey[0], tkey[1], tkey[2], col, e)
						df = pd.DataFrame()
					row = df.to_dict(orient="records")[0] if not df.empty else {}
					row = {k: _json_safe(v) for k, v in row.items()}
					stats.append({"database_name": tkey[0], "schema_name": tkey[1], "table_name": tkey[2], "column_name": col, "data_type": dtype, **row})
			results["column_stats"] = stats
		except Exception:
			logger.exception("Failed to compute column stats")

		# Persist updated snapshot
		with open("./data/metadata_latest.json", "w", encoding="utf-8") as f:
			json.dump(results, f, ensure_ascii=False, indent=2)

		# Build graph (best-effort)
		try:
			graph = GraphDB()
			graph.load_local()
			graph.build_from_metadata(results.get("tables", []), results.get("columns", []))
			graph.save_local()
			graph.upsert_to_neo4j()
		except Exception:
			logger.exception("Failed to build or upsert graph")

		return results
	except Exception:
		logger.exception("Metadata extraction failed")
		raise
	finally:
		conn.close()


def save_metadata_docs(metadata: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
	docs: List[Dict[str, Any]] = []

	for t in metadata.get("tables", []):
		db = t.get("database_name") or t.get("DATABASE_NAME")
		schema = t.get("schema_name") or t.get("SCHEMA_NAME")
		table = t.get("table_name") or t.get("TABLE_NAME")
		text = f"Table {db}.{schema}.{table} has row_count={t.get('row_count') or t.get('ROW_COUNT')} created={t.get('created') or t.get('CREATED')} last_altered={t.get('last_altered') or t.get('LAST_ALTERED')}"
		docs.append({"id": f"table::{db}.{schema}.{table}", "text": text, "metadata": t})

	for c in metadata.get("columns", []):
		db = c.get("database_name") or c.get("DATABASE_NAME")
		schema = c.get("schema_name") or c.get("SCHEMA_NAME")
		table = c.get("table_name") or c.get("TABLE_NAME")
		col = c.get("column_name") or c.get("COLUMN_NAME")
		dtype = c.get("data_type") or c.get("DATA_TYPE")
		nullable = c.get("is_nullable") or c.get("IS_NULLABLE")
		text = f"Column {db}.{schema}.{table}.{col} type={dtype} nullable={nullable}"
		docs.append({"id": f"column::{db}.{schema}.{table}.{col}", "text": text, "metadata": c})

	for s in metadata.get("column_stats", []):
		db = s.get("database_name")
		schema = s.get("schema_name")
		table = s.get("table_name")
		col = s.get("column_name")
		text = (f"Stats for {db}.{schema}.{table}.{col}: " + ", ".join(f"{k}={_json_safe(v)}" for k, v in s.items() if k.lower() not in {"database_name", "schema_name", "table_name", "column_name"}))
		docs.append({"id": f"colstats::{db}.{schema}.{table}.{col}", "text": text, "metadata": s})

	for q in metadata.get("query_history", []):
		db = q.get("database_name") or q.get("DATABASE_NAME")
		schema = q.get("schema_name") or q.get("SCHEMA_NAME")
		text = f"Query on {db}.{schema} status={q.get('execution_status') or q.get('EXECUTION_STATUS')} elapsed_ms={_json_safe(q.get('total_elapsed_time') or q.get('TOTAL_ELAPSED_TIME'))} text={str(q.get('query_text') or q.get('QUERY_TEXT'))[:500]}"
		docs.append({"id": f"query::{hash(text)}", "text": text, "metadata": q})

	# Persist docs JSON for UI (safe encoding)
	os.makedirs("./data", exist_ok=True)
	with open("./data/metadata_docs.json", "w", encoding="utf-8") as f:
		json.dump(docs, f, ensure_ascii=False, indent=2)

	return docs
