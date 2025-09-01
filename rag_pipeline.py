from __future__ import annotations

import logging
from typing import Any, Dict, List

import chromadb
import pandas as pd
import plotly.express as px
import snowflake.connector
from chromadb.utils import embedding_functions
from openai import OpenAI
from tenacity import retry, stop_after_attempt, wait_exponential

from cache_utils import cached_call
from config import app_config, snowflake_config
from graphdb_utils import GraphDB

logger = logging.getLogger(__name__)


# ---------- Embedding and Chroma setup ----------

def get_embedding_fn():
	if app_config.openai_api_key:
		return embedding_functions.OpenAIEmbeddingFunction(
			api_key=app_config.openai_api_key,
			model_name=app_config.openai_embedding_model,
		)
	return None


def get_chroma_collection():
	client = chromadb.PersistentClient(path=app_config.chroma_persist_dir)
	collection = client.get_or_create_collection(
		name="metadata_docs",
		embedding_function=get_embedding_fn(),
		metadata={"hnsw:space": "cosine"},
	)
	return collection


def upsert_docs_to_chroma(docs: List[Dict[str, Any]]) -> int:
	collection = get_chroma_collection()
	ids = [d["id"] for d in docs]
	metadatas = [d.get("metadata", {}) for d in docs]
	contents = [d["text"] for d in docs]
	batch_size = 100
	count = 0
	for i in range(0, len(ids), batch_size):
		collection.upsert(ids=ids[i:i+batch_size], metadatas=metadatas[i:i+batch_size], documents=contents[i:i+batch_size])
		count += len(ids[i:i+batch_size])
	return count


def retrieve_context_from_vector(question: str, top_k: int = 8) -> List[str]:
	collection = get_chroma_collection()
	res = collection.query(query_texts=[question], n_results=top_k)
	documents = res.get("documents", [[]])[0]
	return documents


def retrieve_from_graph(keyword: str, max_results: int = 8) -> List[str]:
	g = GraphDB()
	g.load_local()
	matches = g.search(keyword, max_results=max_results)
	return [f"Graph match: {node_id} props={props}" for node_id, props in matches]


# ---------- LLM helpers ----------

_client: OpenAI | None = None


def get_llm_client() -> OpenAI:
	global _client
	if _client is None:
		_client = OpenAI()
	return _client


@retry(wait=wait_exponential(min=1, max=10), stop=stop_after_attempt(3))
def llm_complete(system_prompt: str, user_prompt: str) -> str:
	client = get_llm_client()
	resp = client.chat.completions.create(
		model=app_config.openai_model,
		messages=[
			{"role": "system", "content": system_prompt},
			{"role": "user", "content": user_prompt},
		],
		temperature=0.1,
	)
	return resp.choices[0].message.content or ""


# ---------- SQL generation and execution ----------


def build_system_prompt() -> str:
	return (
		"You are an expert data analyst working with a Snowflake data warehouse. "
		"Given database metadata context and a business question, produce: "
		"1) a concise business insight in simple English, 2) one Snowflake SQL query to answer it, "
		"3) suggested chart type. Only generate valid Snowflake SQL; NEVER drop or modify data."
	)


def build_user_prompt(question: str, context_snippets: List[str]) -> str:
	context_text = "\n\n".join(context_snippets[:12])
	return (
		f"METADATA CONTEXT:\n{context_text}\n\n"
		f"QUESTION:\n{question}\n\n"
		"Respond in JSON with keys: insight, sql, chart_type (one of line, bar, area, scatter, table)."
	)


def pick_context(question: str) -> List[str]:
	vctx = retrieve_context_from_vector(question, top_k=app_config.default_top_k)
	if len(vctx) >= 3:
		return vctx
	gctx = retrieve_from_graph(question, max_results=app_config.default_top_k)
	return gctx if gctx else vctx


def generate_sql_and_insight(question: str) -> Dict[str, Any]:
	def _compute() -> Dict[str, Any]:
		context_snippets = pick_context(question)
		response = llm_complete(build_system_prompt(), build_user_prompt(question, context_snippets))
		try:
			import json
			data = json.loads(response)
		except Exception:
			data = {"insight": response, "sql": None, "chart_type": "table"}
		return {**data, "context": context_snippets}

	return cached_call(("qa", question), _compute, expire=1800)


def _apply_context(conn):
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


def run_sql(sql: str) -> pd.DataFrame:
	conn = snowflake.connector.connect(
		account=snowflake_config.account,
		user=snowflake_config.user,
		password=snowflake_config.password,
		warehouse=snowflake_config.warehouse,
		role=snowflake_config.role,
		database=snowflake_config.database or None,
		schema=snowflake_config.schema or None,
	)
	try:
		_apply_context(conn)
		cur = conn.cursor()
		cur.execute(sql)
		try:
			df = cur.fetch_pandas_all()
		except Exception:
			df = pd.DataFrame()
		return df
	finally:
		conn.close()


def summarize_dataframe(df: pd.DataFrame) -> List[str]:
	notes: List[str] = []
	if df.empty:
		return ["No rows returned."]
	notes.append(f"Rows: {len(df):,}")
	for col in df.columns:
		series = df[col]
		if pd.api.types.is_datetime64_any_dtype(series) or pd.api.types.is_object_dtype(series):
			try:
				s2 = pd.to_datetime(series, errors="ignore")
				if pd.api.types.is_datetime64_any_dtype(s2):
					notes.append(f"{col}: min={s2.min()}, max={s2.max()}")
					continue
			except Exception:
				pass
		if pd.api.types.is_numeric_dtype(series):
			notes.append(f"{col}: min={series.min()}, max={series.max()}")
		else:
			distinct = series.dropna().unique()
			notes.append(f"{col}: distinct≈{min(len(distinct), 100)}")
	return notes


def render_chart(df: pd.DataFrame, chart_type: str):
	if df.empty:
		return None
	time_cols = [c for c in df.columns if "date" in c.lower() or "time" in c.lower()]
	num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
	cat_cols = [c for c in df.columns if not pd.api.types.is_numeric_dtype(df[c])]

	if chart_type in {"line", "area"} and time_cols and num_cols:
		x_col, y_col = time_cols[0], num_cols[0]
		df_sorted = df.sort_values(x_col)
		fig = px.line(df_sorted, x=x_col, y=y_col) if chart_type == "line" else px.area(df_sorted, x=x_col, y=y_col)
		return fig
	if chart_type == "bar" and cat_cols and num_cols:
		x_col, y_col = cat_cols[0], num_cols[0]
		fig = px.bar(df, x=x_col, y=y_col)
		return fig
	if chart_type == "scatter" and len(num_cols) >= 2:
		fig = px.scatter(df, x=num_cols[0], y=num_cols[1])
		return fig
	if len(df.columns) >= 2 and num_cols:
		fig = px.bar(df, x=df.columns[0], y=num_cols[0])
		return fig
	return None
